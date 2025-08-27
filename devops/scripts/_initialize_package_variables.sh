#!/bin/bash

GIT_COMMIT=$(git log -1 --pretty=format:"%H")
INSTALLATION_PREFIX=${INSTALLATION_PREFIX:-"/opt/mapr"}
PKG_NAME=${PKG_NAME:-"kafka-rest"}
PKG_VERSION=${PKG_VERSION:-"7.6.0.300"}
PKG_3DIGIT_VERSION=$(echo "$PKG_VERSION" | cut -d '.' -f 1-3)
TIMESTAMP=${TIMESTAMP:-$(sh -c 'date "+%Y%m%d%H%M"')}
PKG_INSTALL_ROOT=${PKG_INSTALL_ROOT:-"${INSTALLATION_PREFIX}/${PKG_NAME}/${PKG_NAME}-${PKG_3DIGIT_VERSION}"}

DIST_DIR=${DIST_DIR:-"devops/dist"}
# rpmbuild does not work properly when relatve path specified here
BUILD_ROOT=${BUILD_ROOT:-"$(pwd)/devops/buildroot"}
