#!/bin/bash
docker build ./src -t registry.cn-shanghai.aliyuncs.com/transformer/defensor
docker push registry.cn-shanghai.aliyuncs.com/transformer/defensor