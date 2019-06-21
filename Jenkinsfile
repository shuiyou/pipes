#!/usr/bin/env groovy

node {
    stage('checkout') {
         def scmVars = checkout scm
         env.GIT_COMMIT = scmVars.GIT_COMMIT
         env.GIT_URL = scmVars.GIT_URL
    }

    stage('clean') {
        sh "chmod +x build.sh"
        sh "docker login -u admin@magfin.cn -p qwertyuiop1 registry.cn-shanghai.aliyuncs.com"
    }

    stage('packaging') {
        sh "sh build.sh"
    }
}
