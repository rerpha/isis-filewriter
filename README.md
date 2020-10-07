# Filewriter for ISIS files

Used for writing ISIS NeXus files with the filewriter. 

## Requirements 
- Docker
- Docker-compose
 
## Steps to run: 
1. Replace `command-uri` in `config-files/file_writer_config.ini` to the correct broker/topic
1. Replace `status-uri` in `config-files/file_writer_config.ini` to the correct broker/topic
1. Replace `KAFKA_BROKER` in `docker-compose.yml` to the correct broker
1. Run `docker-compose up` in the root directory

*Note: to run headless, use* `docker-compose up -d`

## To stop:
1. Either run `docker-compose down` or use `docker ps` and find the container name and run `docker stop [container]`

To remove the container, use `docker rm [container]`

To remove the container's image, use `docker rmi  [imagename]`
