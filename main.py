import argparse
import os

from drivers.s3_driver import S3Driver
from models.arguments_model import Arguments
from models.configs_model import CredsConfig

TOTAL_OBJECTS_PUSHED: int = 0
TOTAL_MESSAGES_READ: int = 0
STARTING_TIME: int = 0

def load_env_variables() -> None:
    """
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
        # required=True,
        help='Bucket to run of'
    )
    parser.add_argument(
        '-pq', '--publish-queue',
        type=str,
        # required=True,
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

# TODO: Delete default type before deploy
def load_all_objects(bucket_name: str = 'in-progress') -> list:
    """
        Load all objects
        args:
            bucket_name: str - The name of the bucket
        returns:
            list - List of all objects
    """
    try:
        bucket = S3Driver.S3.Bucket(bucket_name)
        counter: int = 0
        for obj in bucket.objects.all():
            counter += 1
            print('current obj', counter)
        print('all_objects are:', counter)
    except Exception as e:
        print(e)

def initialization(configs: CredsConfig) -> None:
    """
        Initialize all connections
    """
    S3Driver.initialize_s3(configs.aws.uri, configs.aws.access_key, configs.aws.secret_key)
    print('S3 initialized')
    # pass


def main(args: Arguments) -> None:
    """
        Main logic of the script
    """
    if args.config_file != None:
        config = read_config_file(args.config_file)
        initialization(config)

    # Load all the objects from the bucket
    load_all_objects()

    # Push all the objects into `publish_queue`
    # publish_to_queue()

    # Read the messages from `results_queue`
    # read_from_queue()

if __name__ == '__main__':
    args: Arguments = load_arguments()
    
    # TODO: Delete before deployment
    args.config_file = 'test_config.json'

    main(args)