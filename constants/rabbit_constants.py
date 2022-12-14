from typing import Final

class EnvKeys:
    RABBIT_HOST: Final[str] = 'RABBIT_HOST'
    RABBIT_PORT: Final[str] = 'RABBIT_PORT'
    RABBIT_USERNAME: Final[str] = 'RABBIT_USERNAME'
    RABBIT_PASSWORD: Final[str] = 'RABBIT_PASSWORD'

    RABBIT_FACES_FINDER_QUEUE_NAME: Final[str] = 'RABBIT_FACES_FINDER_QUEUE_NAME'
    RABBIT_FACES_CALC_EMBEDDING_QUEUE_NAME: Final[str] = 'RABBIT_FACES_CALC_EMBEDDING_QUEUE_NAME'
    RABBIT_OUTPUT_GENERATOR_QUEUE_NAME: Final[str] = 'RABBIT_OUTPUT_GENERATOR_QUEUE_NAME'