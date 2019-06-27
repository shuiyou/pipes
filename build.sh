#!/bin/bash
docker rmi $(docker images --filter "dangling=true" -q --no-trunc)
docker build --no-cache -t registry.cn-shanghai.aliyuncs.com/transformer/pipes ./src