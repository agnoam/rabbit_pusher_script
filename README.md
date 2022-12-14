# RabbitMQ pusher
This script run over an aws s3 bucket and send all the files as RabbitMQ messages to a publish queue.


## Config file
For the constants configurations e.g. credentials. 
This file have to be in `.json` format

## Constraints
The script will work just in case all the "files" (s3 objects) will be at the root of the given bucket.