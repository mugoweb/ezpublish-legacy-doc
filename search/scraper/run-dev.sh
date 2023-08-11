#!/bin/bash
docker compose -f docker-compose.dev.yml up --build
#docker build -t my-scrapy-app .
#docker run -it --rm --name my-running-scrypy-app my-scrapy-app
