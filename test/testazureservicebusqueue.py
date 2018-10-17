from azure.servicebus import Message

from unittest.mock import patch
import uuid

from src.azureservicebusqueue import AzureServiceBusQueue
from test.unittestbase import TestCase


class TestAzureServiceBusQueue(TestCase):

    @classmethod
    def setUp(cls):

        cls.message = Message(b'Test Message')
        cls.service_bus = AzureServiceBusQueue(str(uuid.uuid4()))

    def test_message_will_be_pushed_to_queue(self):
        with patch.object(AzureServiceBusQueue, 'push', return_value=None) as mock_method:
            self.service_bus.push(self.message)
            mock_method.assert_called_once_with(self.message)

    def test_message_will_be_popped_from_queue(self):
        with patch.object(AzureServiceBusQueue, 'pop', return_value=self.message):
            message = self.service_bus.pop()
            self.assertEqual(message, self.message)

    def test_message_will_be_peeked_from_queue(self):
        with patch.object(AzureServiceBusQueue, 'peek', return_value=self.message):
            message = self.service_bus.peek()
            self.assertEqual(message, self.message)
