#! /bin/bash

set -e

declare -a APPS=(
    taskcluster-aws-provisioner2
    cloud-mirror
    ec2-manager
    mozilla-taskcluster
    queue-taskcluster-net
    taskcluster-auth
    taskcluster-events
    taskcluster-github
    taskcluster-hooks
    taskcluster-index
    taskcluster-login
    taskcluster-notify
    taskcluster-pulse
    taskcluster-purge-cache
    taskcluster-secrets
    taskcluster-stats-collector
    taskcluster-task-analysis
    taskcluster-tools
)

main() {
    for app in ${APPS[@]}; do
        echo == $app >&2
        heroku config -a $app
        echo
    done
}

main

