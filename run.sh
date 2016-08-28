#!/bin/bash

docker run -p 8787:8787 -e TZ=America/Edmonton --link db:postgres -i -t tamer
