#!/usr/bin/env groovy
common {
  slackChannel = '#connect-warn'
  upstreamProjects = 'confluentinc/common'
  pintMerge = true
  downStreamValidate = false
  nodeLabel = 'docker-debian-jdk8'
}

def call(body) {
    build job: "confluentinc-pr/connect-comprehensive-test-framework/PR-3/", parameters: [
                        string(name: 'PLUGIN_NAME', value: 'kafka-connect-elasticsearch'),
                        string(name: 'BRANCH_TO_TEST', value: env.CHANGE_BRANCH)
                    ], wait: true
}

call{}