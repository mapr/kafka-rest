#!/bin/bash
set -ex

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
. "${SCRIPT_DIR}/_initialize_package_variables.sh"
. "${SCRIPT_DIR}/_utils.sh"

build_kafka_rest() {
  mvn ${KAFKA_MAVEN_ARGS}
  tgz_name="./kafka-rest/target/kafka-rest-*-package.tar.gz"
  mkdir -p "${BUILD_ROOT}/build"
  tar xvf ${tgz_name} -C "${BUILD_ROOT}/build"
}

main() {
  echo "Cleaning '${BUILD_ROOT}' dir..."
  rm -rf "$BUILD_ROOT"

  echo "Building project..."
  build_kafka_rest

  echo "Preparing directory structure..."
  setup_role "mapr-kafka-rest"

  setup_package "mapr-kafka-rest"

  echo "Building packages..."
  build_package "mapr-kafka-rest"

  echo "Resulting packages:"
  find "$DIST_DIR" -exec readlink -f {} \;
}

main
