import os
from typing import Any, Callable
from concurrent.futures import ThreadPoolExecutor
from logging import debug

import pika
from pika import SelectConnection
from pika.connection import ConnectionParameters
from pika.adapters.asyncio_connection import AsyncioConnection
from pika.spec import Basic, BasicProperties
from pika.channel import Channel
from pika.credentials import ExternalCredentials, PlainCredentials

from constants.rabbit_constants import EnvKeys

# from dotenv import load_dotenv
# load_dotenv() # Take environment variables from .env.

"""
    RabbitDriver -
    This module contains driver functions to create and consume from RabbitMQ queues
"""
class RabbitQueue:
    '''
        Callback can be none, because it can be used just for publishing data
    '''
    def __init__(
        self, 
        callback: Callable[[Channel, Basic.Deliver, BasicProperties, Any], None] = None,
        auto_ack: bool = False, exclusive: bool = False,
        consumer_tag: str = None, arguments: dict[str, dict] = {}, 
        exchange_name: str = '', is_new_channel: bool = False
    ) -> None:        
        self.callback = callback
        self.auto_ack = auto_ack
        self.exclusive = exclusive
        self.consumer_tag = consumer_tag
        self.arguments = arguments
        self.exchange_name = exchange_name
        self.is_new_channel = is_new_channel

class RabbitDriver:
    connection: SelectConnection | AsyncioConnection = None
    default_channel: Channel = None
    
    _use_asyncio_connection: bool
    _thread_pool_executor: ThreadPoolExecutor = None

    ''' Format of this dictionary like this: { queue_name: RabbitQueue (class) } '''
    queues_configurations: dict[str, RabbitQueue] = {}
    ''' Format of this dictionary like this: { queue_name: parent_channel } '''
    active_channels: dict[str, Channel] = {}

    @staticmethod
    def initialize_rabbitmq(
        queues_configurations: dict[str, RabbitQueue],
        host: str = None,
        port: int = None,
        credentials: PlainCredentials | ExternalCredentials = None,
        use_asyncio_connection: bool = False,
        concurrent_messages_number: int = 5
    ) -> None:
        """
            This function initializes the RabbitMQ connection in easy way.
            
            args:
                queues_configurations: dict[str, RabbitQueue] - Dictionary of queue names along the configuration object
                host: str - The host to connect to
                credentials: PlainCredentials | ExternalCredentials = None - In case the RabbitMQ connection have a different credentials, 
                you can set them by this property
                use_asyncio_connection: bool = false - Whether to use asyncio as the connection method
                concurrent_messages_number: int = 5 - The number of concurrent threads of message handlers will process
        """
        RabbitDriver._use_asyncio_connection = use_asyncio_connection
        RabbitDriver._thread_pool_executor = ThreadPoolExecutor(concurrent_messages_number)
        
        RabbitDriver.queues_configurations = queues_configurations
        RabbitDriver.__initialize_connection(host, port, credentials)

    @staticmethod
    def __initialize_connection(
        host: str = None, port: int = None, 
        credentials: PlainCredentials | ExternalCredentials = None
    ) -> None:
        """
            This function initializes the connection to the RabbitMQ 
            args:
                host: str - The host to connect to
                port: int - The port to connect to
                credentials: PlainCredentials | ExternalCredentials - The credentials to use for the connection (in case not default credentials)
        """
        print('__initialize_connection() executing')

        if host is None:
            host = str(os.getenv(EnvKeys.RABBIT_HOST))
        assert host is not None, 'RabbitMQ host must be provided'

        parameters: ConnectionParameters = pika.ConnectionParameters(host, credentials=credentials)

        # Priority of port: manually set (arg), ENV, _DEFAULT
        if port is None:
            assert os.getenv(EnvKeys.RABBIT_PORT) is not None, 'Port for RabbitMQ server must be provided'
            port = int(os.getenv(EnvKeys.RABBIT_PORT))
            parameters.port = port

        if RabbitDriver._use_asyncio_connection:
            RabbitDriver.connection = AsyncioConnection(
                parameters=parameters,
                on_open_callback = lambda connection: RabbitDriver.__setup_channels(connection),
                on_close_callback = lambda event: print(f'Connection closed (by {event} event)'),
                on_open_error_callback=lambda event: print(f'Connection error (by {event} event)')
            )
        else:
            RabbitDriver.connection = pika.SelectConnection(
                parameters = parameters,
                on_open_callback = lambda connection: RabbitDriver.__setup_channels(connection),
                on_close_callback = lambda event: print(f'Connection closed (by {event} event)'),
                on_open_error_callback=lambda event: print(f'Connection error (by {event} event)')
            )

        
    @staticmethod
    def __setup_channels(connection: SelectConnection) -> None:
        """
            This function setup the needed channels for communication (receiving cahnnel and sending channel)
            
            args:
                connection: SelectConnection - Pika's connection object
        """
        print('__setup_channels() executing')
        assert connection is not None, 'Cannot set up channels without active connection'
        
        # A channel for sending messages
        RabbitDriver.default_channel = RabbitDriver.connection.channel()

        # Queues callback channel (channel that receives all the queues callbacks)
        RabbitDriver.connection.channel(on_open_callback = RabbitDriver.__assign_channel)

    @staticmethod
    def __assign_channel(channel: Channel) -> None:
        """
            This function creating the configured queues inside the given channel.
            args:
                channel: Channel - The channel the queues will be created inside
        """
        print('__assign_channel() executing')

        # Open new thread on each queue and saving
        for queue_name in RabbitDriver.queues_configurations:
            RabbitDriver.__setup_queue(queue_name, RabbitDriver.queues_configurations[queue_name], channel)
        
        RabbitDriver.active_channels[queue_name] = channel

    @staticmethod
    def __setup_queue(queue_name: str, queue_declaration: RabbitQueue, channel: Channel) -> None:
        """
            This function setup a queue consumers
            args:
                queue_name: str - Queue name to declare
                queue_declaration: RabbitQueue - Queue settings for declaration
                channel: Channel - The channel to declare the queue in
        """
        print('__setup_queue() executing')

        channel.queue_declare(queue=queue_name)
        
        # In case the queue_configuration has a callback function, it means the user want to set a consumer
        if queue_declaration.callback is not None:
            channel.basic_consume(
                queue_name,
                lambda *_args: RabbitDriver._thread_pool_executor.submit(queue_declaration.callback, *_args),
                auto_ack = queue_declaration.auto_ack,
                exclusive = queue_declaration.exclusive,
                consumer_tag = queue_declaration.consumer_tag,
                arguments = queue_declaration.arguments
            )
        
    @staticmethod
    def get_channel() -> Channel:
        """
            Get the "sending" channel
            returns: Channel - The "sending" channel
        """
        print('get_channel() executing')
        _channel: Channel = RabbitDriver.default_channel

        if _channel is not None:
            return _channel
        
        raise Exception('Did not find an active channel, RabbitDriver initialized?')

    @staticmethod
    def listen() -> None:
        """
            This function start the listening loop, 
            The code will not continue after this function until you stop the listening loop
        """
        try:
            print('listen() executing')

            err_message: str = "There is no declared connection, don't forget to call initialize_rabbitmq() method before"
            assert RabbitDriver.connection is not None, err_message

            print('Starting to listen by io loop')
            if RabbitDriver._use_asyncio_connection:
                RabbitDriver.connection.ioloop.run_forever()
            else:
                RabbitDriver.connection.ioloop.start()
        except Exception as ex:
            print('Listen for RabbitMQ failed:', ex)

    @staticmethod
    def close_connection() -> None:
        """
            This function closing the active connection
        """
        print('close_connection() executing')

        for queue_name in RabbitDriver.active_channels:
            try:
                RabbitDriver.active_channels[queue_name].close()
            except Exception as ex:
                debug(f'Can not close {queue_name}. ex: ', ex)

        RabbitDriver.connection.close()