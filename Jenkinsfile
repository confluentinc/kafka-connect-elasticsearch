#!/usr/bin/env groovy
// common {
//   slackChannel = '#connect-warn'
//   upstreamProjects = 'confluentinc/common'
//   pintMerge = true
//   downStreamValidate = false
// }

def call(body) {
    build job: "${projectName}", parameters: [
                        string(name: 'PLUGIN_NAME', value: 'kafka-connect-elasticsearch'),
                        string(name: 'BRANCH_TO_TEST', value: env.CHANGE_BRANCH)
                    ], wait: true
}