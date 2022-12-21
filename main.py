import argparse
import json
import os
import sys

from pika import BlockingConnection
from pika.channel import Channel
from boto3_type_annotations.s3 import ObjectSummary

from drivers.rabbit_driver import initialize_connection
from drivers.s3_driver import S3Driver
from models.arguments_model import Arguments
from models.configs_model import AWSConfig, CredsConfig, RabbitConfig

TO_PUSH: list = []
TOTAL_OBJECTS_PUSHED: int = 0
TOTAL_OBJECTS_READ: int = 0
STARTING_TIME: int = 0
rabbit_connection: BlockingConnection = None

def load_env_variables() -> None:
    """
        Loading env variables
    """
    from dotenv import load_dotenv
    load_dotenv()

def read_config_file(filename: str) -> CredsConfig:
    """
        Reads a json config file
        args:
            filename: str - The path to the config file
        returns:
            CredsConfig - The config object
    """
    import json

    assert os.path.exists(filename), "Given config file does not exist"
    with open(filename) as f:
        config_json: dict = json.load(f)
        generated_config: CredsConfig = CredsConfig.from_json(config_json)
        
        return generated_config

def load_arguments() -> Arguments:
    """
        Load the arguments from the args
        returns:
            Arguments - The arguments object
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config-file',
        type=str,
        required=False,
        help='Path to credentials file contains credentials for the RabbitMQ and AWS S3 servers'
    )
    parser.add_argument(
        '-b', '--bucket', 
        type=str,
        required=True,
        help='Bucket to run of'
    )
    parser.add_argument(
        '-pq', '--publish-queue',
        type=str,
        required=True,
        help='Publish queue to publish messages to'
    )
    parser.add_argument(
        '-rq', '--results-queue',
        type=str,
        # required=True,
        help='Results queue to listen to'
    )
    parser.add_argument(
        '-au', '--aws-uri',
        type=str,
        # required=True,
        help='AWS uri to s3 server'
    )
    parser.add_argument(
        '-aak', '--aws-access-key',
        type=str,
        required=False,
        help='AWS access key'
    )
    parser.add_argument(
        '-ask', '--aws-secret-key',
        type=str,
        required=False,
        help='AWS secret key'
    )
    parser.add_argument(
        '-rh', '--rabbit-host',
        type=str,
        # required=True,
        help='Rabbit host'
    )
    parser.add_argument(
        '-rp', '--rabbit-port',
        type=int,
        # required=True,
        help='Rabbit port'
    )

    return parser.parse_args()

def load_all_objects(bucket_name: str) -> list[ObjectSummary]:
    """
        Load all objects
        args:
            bucket_name: str - The name of the bucket
        returns:
            list - List of all objects
    """
    try:
        all_objects: list[ObjectSummary] = []
        bucket = S3Driver.S3.Bucket(bucket_name)
        for obj in bucket.objects.all():
            all_objects.append(obj)

        return all_objects
    except Exception as e:
        print(e)

def initialization(configs: CredsConfig) -> None:
    """
        Initialize all connections
    """
    global rabbit_connection

    if configs.aws.uri:
        S3Driver.initialize_s3(configs.aws.uri, configs.aws.access_key, configs.aws.secret_key)
        print('S3 initialized')
    
    if configs.rabbit.host:
        rabbit_connection = initialize_connection(configs)

def publish_to_queue(data, queue_name: str, exchange: str='') -> None:
    """
        Publish data to rabbitmq queue
    """
    global rabbit_connection

    channel: Channel = rabbit_connection.channel()
    channel.queue_declare(queue_name) # Creates the queue in case it not already 
    channel.basic_publish(exchange, routing_key=queue_name, body=json.dumps(data))

def gen_url_from_ref(storage_host_url: str, ref: ObjectSummary) -> str:
    return f"{storage_host_url}/{ref.bucket_name}/{ref.key}"

def args_to_config(args: Arguments) -> CredsConfig:
    aws = AWSConfig(args.aws_uri, args.aws_access_key, args.aws_secret_key)
    rabbit = RabbitConfig(args.rabbit_host, args.rabbit_port, args.rabbit_user, args.rabbit_password)

    return CredsConfig(aws, rabbit)


def main(args: Arguments) -> None:
    """
        Main logic of the script
    """
    if args.config_file != None:
        config = read_config_file(args.config_file)
    else:
        config = args_to_config()

    initialization(config)
    all_objs: list = load_all_objects()

    # Push all the objects into `publish_queue`
    for obj in all_objs:
        publish_to_queue({
            'imageUrl': gen_url_from_ref(config.aws.uri, obj)
        }, args.publish_queue)

    # Read the messages from `results_queue`
    # read_from_queue()

if __name__ == '__main__':
    try:
        args: Arguments = load_arguments()
        main(args)

    except KeyboardInterrupt:
        print('Interrupted')
        try:
            # TODO: Close rabbitmq connection and stop all threads
            sys.exit(0)
        except SystemExit:
            os._exit()