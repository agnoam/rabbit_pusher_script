from __future__ import annotations
from dataclasses import dataclass

@dataclass
class AWSConfig:
    uri: str
    access_key: str
    secret_key: str

    def to_json(self) -> dict:
        return {
            "uri": self.uri,
            "access_key": self.access_key,
            "secret_key": self.secret_key,
        }

@dataclass
class RabbitConfig:
    host: str
    port: int
    user: str
    password: str

    def to_json(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password
        }

@dataclass
class CredsConfig:
    aws: AWSConfig
    rabbit: RabbitConfig

    def to_json(self) -> dict:
        return {
            "aws": self.aws.to_json(),
            "rabbit": self.rabbit.to_json(),
        }

    def from_json(json: dict) -> CredsConfig:
        _aws: AWSConfig = None
        _rabbit: RabbitConfig = None

        if 'aws' in json:
            assert json['aws']['uri'] != None, 'URI for AWS S3 server must be provided'
            assert json['aws']['secret_key'] != None, 'Secret key must be provided'
            assert json['aws']['access_key'] != None, 'Access key must be provided'
            
            _aws = AWSConfig(
                uri=json['aws']['uri'],
                access_key=json['aws']['access_key'],
                secret_key=json['aws']['secret_key']
            )

        if 'rabbit' in json:
            assert json['rabbit']['host'] != None, 'Host must be provided'
            assert json['rabbit']['port'] != None, 'Port must be provided'
            
            if 'user' in json['rabbit'] or 'password' in json['rabbit']:
                assert json['rabbit']['user'] != None, 'User must be provided'
                assert json['rabbit']['password'] != None, 'Password must be provided'

                _rabbit = RabbitConfig(
                    host=json['rabbit']['host'],
                    port=json['rabbit']['port'],
                    user=json['rabbit']['user'],
                    password=json['rabbit']['password']
                )
            else:
                _rabbit = RabbitConfig(
                    host=json['rabbit']['host'],
                    port=json['rabbit']['port']
                )

        return CredsConfig(_aws, _rabbit)