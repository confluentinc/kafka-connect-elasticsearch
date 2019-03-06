#!/usr/bin/env groovy

def defaultConfig = [
        nodeLabel       : defaultNodeLabel(),
        cron            : '@weekly',
        testResultSpecs : ['junit': '**/*-reports/TEST-*.xml'],
        slackChannel    : '#connect-eng',
        upstreamProjects:'confluentinc/common'
]

def config = jobConfig({}, defaultConfig)

def job = {
  stage('Build') {
    archiveArtifacts artifacts: 'pom.xml'
    withMaven(
            globalMavenSettingsConfig: 'jenkins-maven-global-settings',
            // findbugs publishing is skipped in both steps because multi-module projects cause
            // extra copies to be reported. Instead, use commonPost to collect once at the end
            // of the build.
            options: [findbugsPublisher(disabled: true)]
    ) {
      // Append validate after site to fix site checkstyle issue: https://github.com/confluentinc/common/pull/78
      withDockerServer([uri: dockerHost()]) {
        withEnv(['MAVEN_OPTS=-XX:MaxPermSize=128M']) {
          sh "mvn --batch-mode -Pjenkins clean install dependency:analyze site validate -U"
        }
      }
    }

    step([$class: 'hudson.plugins.findbugs.FindBugsPublisher', pattern: '**/*bugsXml.xml'])
    step([$class: 'DependencyCheckPublisher'])
  }

  if (config.publish && config.isDevJob) {
    stage('Deploy') {
      withMaven(
              globalMavenSettingsConfig: 'jenkins-maven-global-settings',
              // skip publishing results again to avoid double-counting
              options: [openTasksPublisher(disabled: true), junitPublisher(disabled: true), findbugsPublisher(disabled: true)]
      ) {
        sh "mvn --batch-mode -Pjenkins -D${env.deployOptions} deploy -DskipTests"
      }
    }
  }
}

runJob config, job, { commonPost(config) }
