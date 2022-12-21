from pika.credentials import PlainCredentials
import pika

from models.configs_model import CredsConfig

def initialize_connection(configs: CredsConfig) -> pika.BlockingConnection:
    """
        Initialize a new connection to RabbitMQ
        args:
            configs: CredsConfig - configs object that describes the connection
    """
    creds: PlainCredentials | None = None
    if configs.rabbit.user:
        creds = PlainCredentials(configs.rabbit.user, configs.rabbit.password)
    
    params = pika.ConnectionParameters(configs.rabbit.host, configs.rabbit.port, credentials=creds)
    rabbit_connection = pika.BlockingConnection(params)
    return rabbit_connection