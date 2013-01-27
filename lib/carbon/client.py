from twisted.application.service import Service
from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList
from twisted.internet.interfaces import IPushProducer
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.task import LoopingCall
from twisted.protocols.basic import Int32StringReceiver
from zope.interface import implements

from decorators import debugCall
from carbon.conf import settings
from carbon.util import pickle
from carbon import instrumentation, log, pipeline, state

from collections import deque


SEND_QUEUE_LOW_WATERMARK = settings.MAX_QUEUE_SIZE * 0.8
EPSILON = 0.00000001


class CarbonClientBuffer(object):
  def __init__(self, max_size):
    self.max_size = max_size

    self._queue = deque(maxlen=max_size)
    self.queueFull = Deferred()
    self.queueHasSpace = Deferred()
    self.queueEmpty = Deferred()

  @property
  def size(self):
    return len(self._queue)

  @property
  def isEmpty(self):
    return not bool(self._queue)

  @property
  def isFull(self):
    return self.size >= self.max_size

  def _check_space(self):
    if self.queueFull.called:
      if self.size <= SEND_QUEUE_LOW_WATERMARK:
        self.queueHasSpace(self.size)
    elif self.isFull:
      self.queueFull.callback(self.size)
    elif self.isEmpty:
      self.queueEmpty.callback(self.size)
      self.queueEmpty = Deferred()

  #@debugCall
  def put(self, metric, datapoint):
    """Put a datapoint into the buffer"""
    self._queue.append((metric, datapoint))
    self._check_space()

  #@debugCall
  def take(self, count):
    """Take datapoints from the buffer. If the number of datapoints requested
    cannot be satisfied, return all datapoints or an empty list

    :param count: The requested number of datapoints

    :returns: A list of (metric, (timestamp, value)) tuples"""

    datapoints = []
    for _i in xrange(count):
      try:
        datapoints.append(self._queue.popleft())
      except IndexError:
        pass

    self._check_space()
    return datapoints

  def __nonzero__(self):
    return bool(self._queue)


class CarbonClientProtocol(Int32StringReceiver):
  def connectionMade(self):
    log.clients("%s::connectionMade" % self)
    self.factory.resetDelay()
    self.connected = 1
    self.transport.registerProducer(self.factory, streaming=True)
    # Define internal metric names
    self.destinationName = self.factory.destinationName
    self.queuedUntilReady = 'destinations.%s.queued_until_destination_ready' % self.destinationName
    self.datapoints_sent = 'destinations.%s.datapoints_sent' % self.destinationName
    self.messages_sent = 'destinations.%s.messages_sent' % self.destinationName

    self.factory.connectionMade.callback(self)
    self.factory.connectionMade = Deferred()
    self.factory.resumeProducing()

  def connectionLost(self, reason):
    log.clients("%s::connectionLost %s" % (self, reason.getErrorMessage()))
    self.factory.pauseProducing()
    self.connected = 0

  def disconnect(self):
    if self.connected:
      self.transport.unregisterProducer()
      self.transport.loseConnection()
      self.connected = 0

  #@debugCall
  def sendDatapoints(self, datapoints):
    self.sendString(pickle.dumps(datapoints, protocol=-1))

  def __str__(self):
    return 'CarbonClientProtocol(%s:%d:%s)' % (self.factory.destination)
  __repr__ = __str__

  def __nonzero__(self):
    return bool(self.connected)


class CarbonClientFactory(ReconnectingClientFactory):
  implements(IPushProducer)

  maxDelay = 5

  def __init__(self, destination):
    self.destination = destination
    self.destinationName = ('%s:%d:%s' % destination).replace('.', '_')
    self.host, self.port, self.carbon_instance = destination
    self.addr = (self.host, self.port)
    self.started = False
    self.outputBuffer = CarbonClientBuffer(settings.MAX_QUEUE_SIZE)
    self.bufferFlushLoop = LoopingCall(self._doFlushBuffered)
    self.pendingFlush = None
    self.paused = True  # Paused until connected
    # This factory maintains protocol state across reconnects
    self.connectedProtocol = None
    self.connectFailed = Deferred()
    self.connectionMade = Deferred()
    self.connectionLost = Deferred()
    # Define internal metric names
    self.attemptedRelays = 'destinations.%s.attempted_relays' % self.destinationName
    self.queuedUntilConnected = 'destinations.%s.queued_until_connected' % self.destinationName

  def buildProtocol(self, addr):
    self.connectedProtocol = CarbonClientProtocol()
    self.connectedProtocol.factory = self
    return self.connectedProtocol

  def startConnecting(self):  # calling this in startFactory yields recursion problems
    self.started = True
    self.connector = reactor.connectTCP(self.host, self.port, self)

  def stopConnecting(self):
    self.started = False
    self.stopTrying()
    if self.connectedProtocol:
      return self.connectedProtocol.disconnect()

  #@debugCall
  def pauseProducing(self):
    self._cancelFlush()
    self.paused = True

  #@debugCall
  def resumeProducing(self):
    self.paused = False
    self._scheduleFlush()

  #@debugCall
  def stopProducing(self):
    self.disconnect()

  #@debugCall
  def sendDatapoint(self, metric, datapoint):
    instrumentation.increment(self.attemptedRelays)
    self.outputBuffer.put(metric, datapoint)

    if not self.paused:
      if self.outputBuffer.size >= settings.MAX_DATAPOINTS_PER_MESSAGE:
        self._sendBuffered()
        if self.pendingFlush:
          self.pendingFlush.cancel()
          self.pendingFlush = None
      if self.outputBuffer:
        # Set a timeout at which we send anyway
        self._scheduleFlush()

  #@debugCall
  def _sendBuffered(self):
    datapoints = self.outputBuffer.take(settings.MAX_DATAPOINTS_PER_MESSAGE)
    self.connectedProtocol.sendDatapoints(datapoints)

  #@debugCall
  def _scheduleFlush(self):
    if self.pendingFlush is None:
      self.pendingFlush = reactor.callLater(settings.MAX_SEND_PAUSE, self.flushBuffered)

  #@debugCall
  def _cancelFlush(self):
    if self.bufferFlushLoop.running:
      self.bufferFlushLoop.stop()

  #@debugCall
  def _doFlushBuffered(self):
    self.pendingFlush = None
    if self.outputBuffer and not self.paused:
      self._sendBuffered()
    else:
      # Done for now, kill the loop
      self.bufferFlushLoop.stop()

  #@debugCall
  def flushBuffered(self):
    if not self.bufferFlushLoop.running:
      self.bufferFlushLoop.start(1 / settings.MAX_QUEUE_FLUSH_RATE, now=True)

  def startedConnecting(self, connector):
    log.clients("%s::startedConnecting (%s:%d)" % (self, connector.host, connector.port))

  def clientConnectionLost(self, connector, reason):
    ReconnectingClientFactory.clientConnectionLost(self, connector, reason)
    log.clients("%s::clientConnectionLost (%s:%d) %s" % (self, connector.host, connector.port, reason.getErrorMessage()))
    self.connectedProtocol = None
    self.connectionLost.callback(0)
    self.connectionLost = Deferred()

  def clientConnectionFailed(self, connector, reason):
    ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)
    log.clients("%s::clientConnectionFailed (%s:%d) %s" % (self, connector.host, connector.port, reason.getErrorMessage()))
    self.connectFailed.callback(dict(connector=connector, reason=reason))
    self.connectFailed = Deferred()

  def disconnect(self):
    if self.outputBuffer:
      self.outputBuffer.queueEmpty.addCallback(lambda result: self.stopConnecting())
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
    self.client_factories = {}  # { destination : CarbonClientFactory() }

  def startService(self):
    Service.startService(self)
    for factory in self.client_factories.values():
      if not factory.started:
        factory.startConnecting()

  def stopService(self):
    Service.stopService(self)
    self.stopAllClients()

  def startClient(self, destination):
    if destination in self.client_factories:
      return

    log.clients("connecting to carbon daemon at %s:%d:%s" % destination)
    self.router.addDestination(destination)
    factory = self.client_factories[destination] = CarbonClientFactory(destination)
    connectAttempted = DeferredList(
        [factory.connectionMade, factory.connectFailed],
        fireOnOneCallback=True,
        fireOnOneErrback=True)
    if self.running:
      factory.startConnecting()  # this can trigger & replace connectFailed
    factory.outputBuffer.queueFull.addCallback(self.handleQueueFull, factory)

    return connectAttempted

  def stopClient(self, destination):
    factory = self.client_factories.get(destination)
    if factory is None:
      return

    self.router.removeDestination(destination)
    stopCompleted = factory.disconnect()
    stopCompleted.addCallback(lambda result: self.disconnectClient(destination))
    return stopCompleted

  def disconnectClient(self, destination):
    factory = self.client_factories.pop(destination)
    c = factory.connector
    if c and c.state == 'connecting' and not factory.hasQueuedDatapoints():
      c.stopConnecting()

  def stopAllClients(self):
    deferreds = []
    for destination in list(self.client_factories):
      deferreds.append(self.stopClient(destination))
    return DeferredList(deferreds)

  def handleQueueFull(self, size, factory):
    #XXX Failure handling

    factory.queueHasSpace.addCallback(self.handleQueueHasSpace, factory)

  def handleQueueHasSpace(self, size, factory):

    # Reset the deferreds that fired
    factory.queueFull = Deferred()
    factory.queueHasSpace = Deferred()
    factory.outputBuffer.queueFull.addCallback(self.handleQueueFull, factory)

  #@debugCall
  def sendDatapoint(self, metric, datapoint):
    for destination in self.router.getDestinations(metric):
      self.client_factories[destination].sendDatapoint(metric, datapoint)

  def __str__(self):
    return "<%s[%x]>" % (self.__class__.__name__, id(self))


class RelayProcessor(pipeline.Processor):
  plugin_name = 'relay'

  def process(self, metric, datapoint):
    state.client_manager.sendDatapoint(metric, datapoint)
    return pipeline.Processor.NO_OUTPUT
