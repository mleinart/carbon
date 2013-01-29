from nose.twistedtools import deferred, reactor

from twisted.internet.task import deferLater
from twisted.protocols.basic import _PauseableMixin
from twisted.test.proto_helpers import StringTransport

import carbon.client as carbon_client
from carbon.client import CarbonClientBuffer, CarbonClientFactory, \
  CarbonClientProtocol, SEND_QUEUE_LOW_WATERMARK
from carbon.conf import CarbonConfiguration
from carbon.routers import DatapointRouter
from carbon import instrumentation

from pickle import loads as pickle_loads
from struct import unpack, calcsize
from unittest import TestCase

from mock import Mock, patch

INT32_FORMAT = '!I'
INT32_SIZE = calcsize(INT32_FORMAT)


def decode_sent(data):
  pickle_size = unpack(INT32_FORMAT, data[:INT32_SIZE])[0]
  return pickle_loads(data[INT32_SIZE:INT32_SIZE + pickle_size])


class BroadcastRouter(DatapointRouter):
  def __init__(self, destinations=[]):
    self.destinations = set(destinations)

  def addDestination(self, destination):
    self.destinations.append(destination)

  def removeDestination(self, destination):
    self.destinations.discard(destination)

  def getDestinations(self, key):
    for destination in self.destinations:
      yield destination


class ConnectingCarbonClientProtocolTest(TestCase):
  def setUp(self):
    self.factory_mock = Mock(spec=CarbonClientFactory)()
    self.factory_mock.destination = ('127.0.0.1', 2004, 'a')
    self.transport_mock = Mock(spec=StringTransport)()
    self.protocol = CarbonClientProtocol()
    self.protocol.factory = self.factory_mock
    self.protocol.transport = self.transport_mock

  def test_connectionMade_resets_factory(self):
    self.protocol.connectionMade()
    self.factory_mock.resetDelay.assert_called_once()

  def test_connectionMade_sets_connected(self):
    self.protocol.connectionMade()
    self.assertTrue(self.protocol.connected)

  def test_connectionMade_registers_as_producer(self):
    self.protocol.connectionMade()
    self.transport_mock.registerProducer.assert_called_once_with(
      self.protocol, streaming=True)


class ConnectedCarbonClientProtocolTest(TestCase):
  def setUp(self):
    self.factory_mock = Mock(spec=CarbonClientFactory)
    self.factory_mock.return_value.destination = ('127.0.0.1', 2004, 'a')
    self.transport = StringTransport()
    self.protocol = CarbonClientProtocol()
    self.protocol.factory = self.factory_mock()
    self.protocol.transport = self.transport
    self.protocol.connected = 1
    self.protocol.paused = False
    self.transport.registerProducer(self.protocol, streaming=True)

  def test_connectionLost(self):
    self.protocol.connectionLost(Mock())
    self.assertFalse(self.protocol.connected)
    self.assertTrue(self.protocol.paused)

  def test_pauseProducing(self):
    self.protocol.producer = Mock(spec=_PauseableMixin)
    self.protocol.pauseProducing()
    self.assertTrue(self.protocol.paused)
    self.protocol.producer.return_value.pauseProducing.assert_called_once()

  def test_resumeProducing(self):
    self.protocol.paused = True
    self.protocol.producer = Mock(spec=_PauseableMixin)
    self.protocol.resumeProducing()
    self.assertFalse(self.protocol.paused)
    self.protocol.producer.return_value.resumeProducing.assert_called_once()

  def test_sendDatapoints(self):
    datapoint = ('foo.bar', (1000000000, 1.0))
    self.protocol.sendDatapoints([datapoint])
    sent_data = self.transport.value()
    sent_datapoints = decode_sent(sent_data)
    self.assertEquals([datapoint], sent_datapoints)


class CarbonClientBufferTest(TestCase):
  def setUp(self):
    self._settings_patch = patch.dict('carbon.client.settings', {
      'MAX_QUEUE_FLUSH_RATE': 5,
      'MAX_SEND_PAUSE': 0.1,
      'MAX_DATAPOINTS_PER_MESSAGE': 2
    })
    self._settings_patch.start()
    self.max_size = 5
    self.client_buffer = CarbonClientBuffer(self.max_size)
    self.producer_mock = Mock(spec=_PauseableMixin)
    self.producer_mock.return_value.paused = False
    self.consumer_mock = Mock(spec=CarbonClientProtocol)
    self.client_buffer.producer = self.producer_mock
    self.client_buffer.consumer = self.consumer_mock
    self.client_buffer.paused = False

  def _assert_stopped(self):
    self.assertFalse(self.client_buffer._bufferFlushLoop.running)

  def tearDown(self):
    self._settings_patch.stop()

  def test_size(self):
    self.client_buffer.put('foo.bar', (1000000000, 1.0))
    self.assertEquals(1, self.client_buffer.size)

  def test_isEmpty(self):
    self.assertTrue(self.client_buffer.isEmpty)
    self.client_buffer.put('foo.bar', (1000000000, 1.0))
    self.assertFalse(self.client_buffer.isEmpty)

  def test_isFull(self):
    self.assertFalse(self.client_buffer.isFull)
    for _i in range(self.max_size):
      self.client_buffer.put('foo.bar', (1000000000, 1.0))
    self.assertTrue(self.client_buffer.isFull)

  def test_put_full_pauses_producer(self):
    self.client_buffer.paused = True
    for _i in range(self.max_size):
      self.client_buffer.put('foo.bar', (1000000000, 1.0))
    self.producer_mock.return_value.pauseProducing.assert_called_once()

  def test_take_returns_number_asked(self):
    for _i in range(self.max_size):
      self.client_buffer.put('foo.bar', (1000000000, 1.0))

    datapoints = self.client_buffer.take(self.max_size)
    self.assertEquals(self.max_size, len(datapoints))

  def test_take_returns_all_when_too_small(self):
    for _i in range(self.max_size):
      self.client_buffer.put('foo.bar', (1000000000, 1.0))

    datapoints = self.client_buffer.take(self.max_size * 2)
    self.assertEquals(self.max_size, len(datapoints))

  def test_take_resumes_producer_at_threshold(self):
    self.client_buffer.paused = True
    for _i in range(self.max_size):
      self.client_buffer.put('foo.bar', (1000000000, 1.0))
    self.client_buffer.take(self.max_size - int(SEND_QUEUE_LOW_WATERMARK))
    self.producer_mock.return_value.resumeProducing.assert_called_once()

  @deferred(timeout=1)  # we should return immediately
  def test_flushBuffer_empty(self):
    return self.client_buffer.flushBuffer()

  @deferred(timeout=1)
  def test_flushBuffer_stops_flush_when_paused(self):
    # Make a non-empty buffer
    self.client_buffer.put('foo.bar', (1000000000, 1.0))
    self.client_buffer.paused = True

    reactor.callLater(0, self.client_buffer.flushBuffer)
    return deferLater(reactor, 0.1, self._assert_stopped)

  @deferred(timeout=1)
  def test_flushBuffer_stops_when_empty(self):
    for _i in range(2):
      self.client_buffer.put('foo.bar', (1000000000, 1.0))
    reactor.callLater(0, self.client_buffer.flushBuffer)
    return deferLater(reactor, 0.1, self._assert_stopped)

  def test_sendDatapoint_adds_to_queue_when_unpaused(self):
    self.client_buffer.sendDatapoint(('foo.bar', (1000000000, 1.0)))
    self.assertEquals(1, self.client_buffer.size)

  def test_sendDatapoint_adds_to_queue_when_paused(self):
    self.client_buffer.paused = True
    self.client_buffer.sendDatapoint(('foo.bar', (1000000000, 1.0)))
    self.assertEquals(1, self.client_buffer.size)

  @deferred(timeout=1)
  def test_sendDatapoint_doesnt_sent_immediately_if_under_max(self):
    self.client_buffer.sendDatapoint(('foo.bar', (1000000000, 1.0)))

    def _assert_size_one():
      self.assertEquals(1, self.client_buffer.size)
    return deferLater(reactor, 0, _assert_size_one)

  @deferred(timeout=1)
  def test_sendDatapoint_sends_later_if_under_max(self):
    self.client_buffer.sendDatapoint(('foo.bar', (1000000000, 1.0)))

    def _assert_empty():
      self.assertTrue(self.client_buffer.isEmpty)
    return deferLater(reactor, 0.2, _assert_empty)

  def test_sendDatapoint_sends_one_message_if_over_max(self):
    for _i in range(2):
      self.client_buffer.sendDatapoint(('foo.bar', (1000000000, 1.0)))
    self.assertTrue(self.client_buffer.isEmpty)



# @patch('carbon.state.instrumentation', Mock(spec=instrumentation))
# class ConnectedCarbonClientProtocol(TestCase):
#   def setUp(self):
#     carbon_client.settings = CarbonConfiguration()  # reset to defaults
#     factory = CarbonClientFactory(('127.0.0.1', 0, 'a'))
#     self.proto = factory.buildProtocol(('127.0.0.1', 0))
#     self.transport = proto_helpers.StringTransport()
#     self.proto.makeConnection(self.transport)
#
#   def _decode_sent(self):
#     sent_data = self.transport.value()
#     pickle_size = unpack(INT32_FORMAT, sent_data[:INT32_SIZE])[0]
#     return pickle_loads(sent_data[INT32_SIZE:INT32_SIZE + pickle_size])
#
#   def test_send_datapoint(self):
#     datapoint = (1000000000, 1.0)
#     self.proto.sendDatapoint('foo.bar.baz', datapoint)
#     self.assertEquals([('foo.bar.baz', datapoint)],
#               self._decode_sent())
#
#   def test_sendsome_queued_empty_queue(self):
#     self.proto.sendSomeQueued()
#     self.assertEqual('', self.transport.value())
#
#   def test_send_some_queued_nonempty_queue(self):
#     carbon_client.settings.MAX_DATAPOINTS_PER_MESSAGE = 25
#     for i in xrange(100):
#       self.proto.factory.enqueue('foo.bar.baz', (1000000000, float(i)))
#     self.proto.sendSomeQueued()
#     sent = self._decode_sent()
#     self.assertEqual(25, len(sent))
#     self.assertEqual(0.0, sent[0][1][1])
#     self.assertEqual(24.0, sent[24][1][1])
#
#   def test_queue_has_space(self):
#     carbon_client.settings.MAX_QUEUE_SIZE = 100
#     carbon_client.settings.MAX_DATAPOINTS_PER_MESSAGE = 20
#     for i in xrange(101):
#       self.proto.factory.enqueue('foo.bar.baz', (1000000000, float(i)))
#     self.assertTrue(self.proto.factory.queueFull.called)
#     self.proto.sendSomeQueued()
#     self.assertFalse(self.proto.factory.queueHasSpace.called)
