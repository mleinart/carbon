from twisted.application.service import Service
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList
from twisted.internet.interfaces import IConsumer, IPushProducer
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.task import LoopingCall
from twisted.protocols.basic import Int32StringReceiver
from twisted.protocols.pcp import BasicProducerConsumerProxy
from zope.interface import implements

#from decorators import debugCall
from carbon.conf import settings
from carbon.util import pickle
from carbon import instrumentation, log, pipeline, state

from collections import deque


SEND_QUEUE_LOW_WATERMARK = settings.MAX_QUEUE_SIZE * 0.8
EPSILON = 0.00000001


class CarbonClientProtocol(Int32StringReceiver):
  """Handles serialization of metric data and the lifecycle of the connection,
  propagating up pauses in the child transport to other producers"""
  implements(IConsumer, IPushProducer)

  consumer = None  # Transport consumes from us
  producer = None  # CarbonClientBuffer produces for us
  paused = True

  def connectionMade(self):
    log.clients("%s::connectionMade" % self)
    self.connected = 1
    self.factory.resetDelay()
    self.transport.registerProducer(self, streaming=True)
    self.consumer = self.transport
    # Define internal metric names
    self.destinationName = self.factory.destinationName
    self.queuedUntilReady = 'destinations.%s.queued_until_destination_ready' % self.destinationName
    self.datapoints_sent = 'destinations.%s.datapoints_sent' % self.destinationName
    self.messages_sent = 'destinations.%s.messages_sent' % self.destinationName

    self.factory.connectionMade.callback(self)
    self.factory.connectionMade = Deferred()
    self.resumeProducing()

  def connectionLost(self, reason):
    log.clients("%s::connectionLost %s" % (self, reason.getErrorMessage()))
    self.pauseProducing()
    self.connected = 0

  def sendDatapoints(self, datapoints):
    self.sendString(pickle.dumps(datapoints, protocol=-1))

  #IPushProducer methods

  def pauseProducing(self):
    self.paused = True
    if self.producer is not None:
      self.producer.pauseProducing()

  def resumeProducing(self):
    self.paused = False
    if self.producer is not None:
      self.producer.resumeProducing()

  def stopProducing(self):
    if self.producer is not None:
      self.producer.stopProducing()

  #IConsumer methods

  def registerProducer(self, producer, streaming):
    self.producer = producer
    producer.consumer = self

  def unregisterProducer(self):
    if self.producer is not None:
      self.producer = None
    if self.consumer is not None:
      self.consumer.unregisterProducer()

  # Satisfy IConsumer
  write = sendDatapoints

  def __str__(self):
    return 'CarbonClientProtocol(%s:%d:%s)' % (self.factory.destination)
  __repr__ = __str__

  def __nonzero__(self):
    return bool(self.connected)


class CarbonClientBuffer:
  implements(IConsumer, IPushProducer)

  consumer = None  # CarbonClientProtocol consumes from us
  producer = None  # CarbonClientFactory produces for us
  paused = True

  def __init__(self, max_size):
    self.max_size = max_size
    self._buffer = deque(maxlen=max_size)
    self._bufferEmpty = None  # Deferred
    self._bufferFlushLoop = LoopingCall(self._doBufferFlush)
    self._pendingFlush = None

  @property
  def size(self):
    return len(self._buffer)

  @property
  def isEmpty(self):
    return not bool(self._buffer)

  @property
  def isFull(self):
    return self.size >= self.max_size

  def put(self, metric, datapoint):
    """Put a datapoint into the buffer"""
    self._buffer.append((metric, datapoint))
    #XXX What about when we have a circular buffer?
    if self.isFull:
      self.producer.pauseProducing()

  def take(self, count):
    """Take datapoints from the buffer. If the number of datapoints requested
    cannot be satisfied, return all datapoints or an empty list

    :param count: The requested number of datapoints

    :returns: A list of (metric, (timestamp, value)) tuples"""

    datapoints = []
    for _i in xrange(count):
      try:
        datapoints.append(self._buffer.popleft())
      except IndexError:
        pass

    if self.producer is not None:
      if self.producer.paused:
        if self.size <= SEND_QUEUE_LOW_WATERMARK:
          self.producer.resumeProducing()

    return datapoints

  def sendDatapoint(self, metricDatapoint):
    metric, datapoint = metricDatapoint
    self.put(metric, datapoint)

    if not self.paused:
      # Wait until we have at least 1 full message worth of points
      if self.size >= settings.MAX_DATAPOINTS_PER_MESSAGE:
        self._sendOneMessage()
        # Got one out, cancel any pending or ongoing flush
        self._cancelScheduledFlush()
        self.stopFlush()
      # If there are still points in the buffer we either
      # a) Were in the middle of a big buffer flush and had a point come in
      # b) We didn't have enough points to send
      # In both cases we'd rather be driven by incoming points but we schedule a
      # flush at MAX_SEND_PAUSE seconds in the future in case one doesn't come
      if self._buffer:
        self._scheduleFlush()

  def _doBufferFlush(self):
    self._bufferEmpty = Deferred()
    self._pendingFlush = None
    if self.paused:
      self.stopFlush()
    if self._buffer:
      self._sendOneMessage()
    # Whether or not we sent one, stop the flush and notify we're empty
    if self.isEmpty:
      self.stopFlush()
      self._bufferEmpty.callback(0)

  def _cancelScheduledFlush(self):
    if self._pendingFlush is not None:
      if self._pendingFlush.active():
        self._pendingFlush.cancel()
    self._pendingFlush = None

  def _scheduleFlush(self):
    if self._pendingFlush is None:
      self._pendingFlush = reactor.callLater(settings.MAX_SEND_PAUSE, self.flushBuffer)

  def _sendOneMessage(self):
    datapoints = self.take(settings.MAX_DATAPOINTS_PER_MESSAGE)
    self.consumer.sendDatapoints(datapoints)

  def stopFlush(self):
    if self._bufferFlushLoop.running:
      self._bufferFlushLoop.stop()

  def flushBuffer(self):
    self._cancelScheduledFlush()

    if not self._bufferFlushLoop.running:
      self._bufferFlushLoop.start(1 / settings.MAX_QUEUE_FLUSH_RATE, now=True)
    return self._bufferEmpty

  #IPushProducer methods

  def pauseProducing(self):
    self.paused = True

  def resumeProducing(self):
    self.paused = False
    self.flushBuffer()

  def stopProducing(self):
    if self.producer is not None:
      self.producer.stopProducing()

  #IConsumer methods

  def registerProducer(self, producer, streaming):
    self.producer = producer

  def unregisterProducer(self):
    if self.producer is not None:
      self.producer = None
    if self.consumer is not None:
      self.consumer.unregisterProducer()

  # Satisfy IConsumer
  write = sendDatapoint

  def __nonzero__(self):
    return bool(self._buffer)


class CarbonClientFactory(ReconnectingClientFactory):
  implements(IPushProducer)

  maxDelay = 5

  def __init__(self, destination):
    self.destination = destination
    self.destinationName = ('%s:%d:%s' % destination).replace('.', '_')
    self.host, self.port, self.carbon_instance = destination
    self.addr = (self.host, self.port)
    self.started = False
    self.clientBuffer = CarbonClientBuffer(settings.MAX_QUEUE_SIZE)
    self.clientBuffer.registerProducer(self, streaming=True)

    # Notifications for CarbonClientManager
    self.connectFailed = Deferred()
    self.connectionMade = Deferred()
    self.connectionLost = Deferred()
    self.paused = Deferred()
    self.resumed = Deferred()

  def buildProtocol(self, addr):
    protocol = CarbonClientProtocol()
    protocol.factory = self
    protocol.registerProducer(self.clientBuffer, streaming=True)
    return protocol

  def startConnecting(self):  # calling this in startFactory yields recursion problems
    self.started = True
    self.connector = reactor.connectTCP(self.host, self.port, self)

  def stopConnecting(self):
    self.started = False
    self.stopTrying()
    if self.connector:
      return self.connector.disconnect()

  #IPushProducer

  def pauseProducing(self):
    if not self.paused.called:
      self.paused.callback(self)

  def resumeProducing(self):
    if not self.resumed.called:
      self.resumed.callback(self)

  def stopProducing(self):
    self.disconnect()

  def sendDatapoint(self, metricDatapoint):
    self.clientBuffer.sendDatapoint(metricDatapoint)

  def startedConnecting(self, connector):
    log.clients("%s::startedConnecting (%s:%d)" % (self, connector.host, connector.port))

  def clientConnectionLost(self, connector, reason):
    ReconnectingClientFactory.clientConnectionLost(self, connector, reason)
    log.clients("%s::clientConnectionLost (%s:%d) %s" % (self, connector.host, connector.port, reason.getErrorMessage()))
    self.connectionLost.callback(dict(connector=connector, reason=reason))
    self.connectionLost = Deferred()

  def clientConnectionFailed(self, connector, reason):
    ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)
    log.clients("%s::clientConnectionFailed (%s:%d) %s" % (self, connector.host, connector.port, reason.getErrorMessage()))
    self.connectFailed.callback(dict(connector=connector, reason=reason))
    self.connectFailed = Deferred()

  def disconnect(self):
    if self.clientBuffer:
      queueEmpty = self.clientBuffer.flushBuffer()
      queueEmpty.addCallback(lambda result: self.stopConnecting())
    readyToStop = DeferredList(
      [self.connectionLost, self.connectFailed],
      fireOnOneCallback=True,
      fireOnOneErrback=True)

    # This can happen if the client is stopped before a connection is ever made
    if (not readyToStop.called) and (not self.started):
      readyToStop.callback(None)

    return readyToStop

  def __str__(self):
    return 'CarbonClientFactory(%s:%d:%s)' % self.destination
  __repr__ = __str__


class CarbonClientManager(Service):
  """Manages initialization and shutdown of individual destinations. Maintains
  destination state and manages failover behavior"""
  def __init__(self, router):
    self.router = router
    self.clientFactories = {}  # { destination : CarbonClientFactory() }

  def startService(self):
    Service.startService(self)
    for factory in self.clientFactories.values():
      if not factory.started:
        factory.startConnecting()

  def stopService(self):
    Service.stopService(self)
    self.stopAllClients()

  def startClient(self, destination):
    if destination in self.clientFactories:
      return

    log.clients("connecting to carbon daemon at %s:%d:%s" % destination)
    self.router.addDestination(destination)
    factory = self.clientFactories[destination] = CarbonClientFactory(destination)
    connectAttempted = DeferredList(
        [factory.connectionMade, factory.connectFailed],
        fireOnOneCallback=True,
        fireOnOneErrback=True)
    if self.running:
      factory.startConnecting()  # this can trigger & replace connectFailed

    return connectAttempted

  def _registerPause(self, factory):
    if factory.paused.called:
      factory.paused = Deferred()
    factory.paused.addCallback(self.pauseClient)

  def _registerResume(self, factory):
    if factory.resume.called:
      factory.resume = Deferred()
    factory.resume.addCallback(self.resumeClient)

  def pauseClient(self, factory):
    #XXX fail the factory
    self._registerResume(factory)

  def resumeClient(self, factory):
    #XXX un-fail the factory
    self._registerPause(factory)

  def stopClient(self, destination):
    factory = self.clientFactories.get(destination)
    if factory is None:
      return

    self.router.removeDestination(destination)
    stopCompleted = factory.disconnect()
    stopCompleted.addCallback(lambda result: self.disconnectClient(destination))
    return stopCompleted

  def disconnectClient(self, destination):
    factory = self.clientFactories.pop(destination)
    c = factory.connector
    if c and c.state == 'connecting' and not factory.clientBuffer:
      c.stopConnecting()

  def stopAllClients(self):
    deferreds = []
    for destination in list(self.clientFactories):
      deferreds.append(self.stopClient(destination))
    return DeferredList(deferreds)

  def sendDatapoint(self, metric, datapoint):
    for destination in self.router.getDestinations(metric):
      self.clientFactories[destination].sendDatapoint((metric, datapoint))

  def __str__(self):
    return "<%s[%x]>" % (self.__class__.__name__, id(self))


class RelayProcessor(pipeline.Processor):
  plugin_name = 'relay'

  def process(self, metric, datapoint):
    state.client_manager.sendDatapoint(metric, datapoint)
    return pipeline.Processor.NO_OUTPUT
