from azure.servicebus import ServiceBusService
from azure.storage.queue import QueueMessageFormat


class AzureServiceBusQueue:
    """
    Interface for interacting with an Azure Service BUS Queue (through the Queue contract)
    """

    def __init__(self, queue_name):
        """
        Initializes the service_bus queue.
        :param queue_name: The name of the queue to access. If a queue with this name doesn't
        already exist on the storage account, the queue will be created on the first operation.
        :param config: AzureServiceBusConfig with a valid service_namespace, shared_access_key_name
        and shared_access_key_value
        """
        self._queue_name = queue_name
        self._service_bus_service = None

    def _get_client(self, service_namespace, shared_access_key_name, shared_access_key_value):
        """
        this method creates a ServiceBusService.
        create_queue does not have a side effectIf the queue already exists
        :return: an client of type ServiceBusService
        """
        if self._service_bus_service is None:
            self._service_bus_service = ServiceBusService(
                service_namespace=service_namespace,
                shared_access_key_name=shared_access_key_name,
                shared_access_key_value=shared_access_key_value)
            self._service_bus_service.encode_function = QueueMessageFormat.text_base64encode
            self._service_bus_service.decode_function = QueueMessageFormat.text_base64decode
            self._service_bus_service.create_queue(self._queue_name)
        return self._service_bus_service

    def push(self, message):
        """
        Pushes a new message onto the queue.
        """
        self._service_bus_service.send_queue_message(self._queue_name, message)

    def pop(self):
        """
        Pops the first message from the queue and returns it.
        """
        return self._service_bus_service.receive_queue_message(self._queue_name, peek_lock=False)

    def peek(self):
        """
        Peeks the fist message from the queue and returns it.
        """
        response = self._service_bus_service.receive_queue_message(self._queue_name, peek_lock=True)
        response.unlock()
        return response
