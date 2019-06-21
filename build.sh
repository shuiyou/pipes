#!/bin/bash
docker build ./src -t registry.cn-shanghai.aliyuncs.com/transformer/pipes
docker push registry.cn-shanghai.aliyuncs.com/transformer/pipes