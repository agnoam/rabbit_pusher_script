import argparse

from dotenv import load_dotenv
load_dotenv()

def load_arguments() -> argparse.ArgumentParser:
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
        required=True,
        help='Results queue to listen to'
    )
    parser.add_argument(
        '-ah', '--aws-host',
        type=str,
        required=True,
        help='AWS host'
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
        required=True,
        help='Rabbit host'
    )
    parser.add_argument(
        '-rp', '--rabbit-port',
        type=int,
        required=True,
        help='Rabbit port'
    )

    return parser

if __name__ == '__main__':
    parser = load_arguments()