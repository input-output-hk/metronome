#!/usr/bin/env bash

set -e

PROJECT_DIR=$(dirname $0)/../..
SCALA_VER=2.13.4
ASSEMBLY_JAR=${PROJECT_DIR}/out/metronome/${SCALA_VER}/examples/assembly/dest/out.jar

cd $PROJECT_DIR
mill metronome[${SCALA_VER}].examples.assembly

exec java -cp ${ASSEMBLY_JAR} io.iohk.metronome.examples.robot.app.RobotApp --node-index $1
