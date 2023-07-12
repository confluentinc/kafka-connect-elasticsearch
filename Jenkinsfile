#!/usr/bin/env groovy
common {
  slackChannel = '#connect-warn'
  upstreamProjects = 'confluentinc/common'
  pintMerge = true
  downStreamValidate = false
}

def upgradeConnectorTest(body) {
  build job: "confluentinc/connect-comprehensive-test-framework/master/", parameters: [
          string(name: 'PLUGIN_NAME', value: 'kafka-connect-elasticsearch'),
          string(name: 'BRANCH_TO_TEST', value: env.CHANGE_BRANCH)
  ], wait: true
}
upgradeConnectorTest{}
