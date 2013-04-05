#!/bin/bash

redis-server --port 7777 &
redis-server --port 8888 &
redis-server --port 6379 &
redis-server --port 9999 &

sudo mongod
