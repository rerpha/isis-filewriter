# Filewriter for ISIS files

Used for writing ISIS NeXus files with the filewriter. 

## Requirements 
- Docker
- Docker-compose
 
## Steps to run: 
1. Replace `command-uri` in `config-files.file_writer_config.ini` to the correct broker/topic
1. Replace `status-uri` in `config-files.file_writer_config.ini` to the correct broker/topic
1. Run `docker-compose up` in the root directory
 