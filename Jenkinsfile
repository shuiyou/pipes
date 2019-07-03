#!/usr/bin/env groovy

node {

    imageVersion = 'registry.cn-shanghai.aliyuncs.com/transformer/pipes:1.0.0'
    stage('checkout') {
         def scmVars = checkout scm
         env.GIT_COMMIT = scmVars.GIT_COMMIT
         env.GIT_URL = scmVars.GIT_URL
    }

    stage('login docker repository') {
        sh "chmod +x build.sh"
        sh "docker login -u admin@magfin.cn -p qwertyuiop1 registry.cn-shanghai.aliyuncs.com"
    }

    stage('build image') {
        sh "sh build.sh"
        sh "docker build --no-cache -t " + imageVersion + " ./src"
    }

    stage('push image') {
        sh "docker push " + imageVersion
    }
}
