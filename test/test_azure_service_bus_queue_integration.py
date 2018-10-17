import pytest
from azure.servicebus import Message

import time
import uuid

from src.azureservicebusqueue import AzureServiceBusQueue
from test.unittestbase import TestCase


class TestAzureServiceBusQueue(TestCase):
    """
    these integration tests are calling azure service bus.
    Hence you need service_bus_namespace, service_bus_sas_key_name and service_bus_sas_key_value
     to run the integration tests
    """

    @classmethod
    def setUp(cls):
        """
         before running each test, setup(cls) method will set up the test
         pre-conditions
         NOTE: before each test a queue is created (with a random name)
        """

        cls.message = Message(b'Test Message')
        cls.service_bus = AzureServiceBusQueue(str(uuid.uuid4()))
        cls.service_bus._get_client()

    @classmethod
    def tearDown(cls):
        """
         after running each test, tearDown() method will do the post-conditions
         and cleaning up tasks(e.g. delete the queue)
         NOTE: after each test the created queue will be deleted
        """
        cls.service_bus._get_client().delete_queue(cls.service_bus._queue_name)

    @pytest.mark.skip(reason="Integration test")
    def test_push_and_peek_message_from_queue_integration(self):
        # given
        self.service_bus.push(self.message)
        # when
        message = self.service_bus.peek()
        # then
        self.assertEqual(message.body, self.message.body)

    @pytest.mark.skip(reason="Integration test")
    def test_peek_will_not_delete_message_from_queue_integration(self):
        # given
        self.service_bus.push(self.message)
        # when
        peeked_message = self.service_bus.peek()
        popped_message = self.service_bus.pop()
        # then
        self.assertEqual(peeked_message.body, self.message.body)
        self.assertEqual(popped_message.body, self.message.body)

    @pytest.mark.skip(reason="Integration test")
    def test_queue_preserve_the_fifo_order_integration(self):
        # given
        self.service_bus.push(Message(b'Message 1'))
        time.sleep(1)
        self.service_bus.push(Message(b'Message 2'))
        # when
        message_one = self.service_bus.pop()
        message_two = self.service_bus.pop()
        # then
        self.assertEqual(message_one.body, b'Message 1')
        self.assertEqual(message_two.body, b'Message 2')

    @pytest.mark.skip(reason="Integration test")
    def test_pop_from_empty_queue_will_not_fail_integration(self):
        # when
        message = self.service_bus.pop()
        # then
        self.assertEqual(message.body, None)
