#!/bin/bash

docker run -d -p 8787:8787 -e TZ=America/Edmonton --link db:postgres tamer
