from dataclasses import dataclass

@dataclass
class Arguments:
    config_file: str
    bucket: str
    publish_queue: str
    results_queue: str
    # timeout: int

    # AWS configurations
    aws_uri: str
    aws_access_key: str
    aws_secret_key: str
    
    # RabbitMQ configurations
    rabbit_host: str
    rabbit_port: int
    rabbit_user: str
    rabbit_password: str