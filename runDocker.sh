#!/bin/bash

sudo docker stop ctr_streaming-processor
sudo docker rm ctr_streaming-processor
sudo docker rmi img_streaming-processor


sudo docker build --rm -t img_streaming-processor:latest .

sudo docker run  --network="host" -p 7070:7070  --name  ctr_streaming-processor  -d img_streaming-processor
