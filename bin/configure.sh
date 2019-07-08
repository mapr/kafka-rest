#!/bin/bash
#######################################################################
# Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
#######################################################################
#
# Configure script for Kafka REST. Allows to configure Kafka in secure
# and unsecure modes. Secure mode provides SSL support and generates
# appropriate certificate, client key with default password.
#
#######################################################################

#######################################################################
# Import functions and variables from 'common-ecosystem.sh'
#######################################################################

MAPR_HOME=${MAPR_HOME:-/opt/mapr}

. ${MAPR_HOME}/server/common-ecosystem.sh 2> /dev/null # prevent verbose output, set by 'set -x'
if [ $? -ne 0 ]; then
  echo 'Error: Seems that MAPR_HOME is not correctly set or mapr-core is not installed.'
  exit 1
fi 2> /dev/null
{ set +x; } 2>/dev/null

initCfgEnv


#######################################################################
# Globals definition
#######################################################################

# general
VERSION=5.1.2
KAFKA_REST_PORT=8082
NOW=`date "+%Y%m%d_%H%M%S"`
KAFKA_REST_NAME=${KAFKA_REST_NAME:-kafka-rest}
KAFKA_REST_HOME=${NEW_MYPKG_CONF_FILE:-${MAPR_HOME}/${KAFKA_REST_NAME}}
KAFKA_REST_PACKAGE_DIR=${KAFKA_REST_PACKAGE_DIR:-${KAFKA_REST_HOME}/${KAFKA_REST_NAME}-${VERSION}}
KAFKA_REST_CONF_DIR=${KAFKA_REST_CONF_DIR:-${KAFKA_REST_PACKAGE_DIR}/config}
KAFKA_REST_CONF_TEMPLATE_DIR=${KAFKA_REST_CONF_TEMPLATE_DIR:-${KAFKA_REST_PACKAGE_DIR}/conf.new}
KAFKA_REST_VERSION_FILE=${KAFKA_REST_VERSION_FILE:-${KAFKA_REST_HOME}/kafka-restversion}
KAFKA_REST_PROPERTIES=${KAFKA_REST_PROPERTIES:-${KAFKA_REST_CONF_DIR}/${KAFKA_REST_NAME}.properties}

# directory contains saved user's configs
KAFKA_REST_SAVED_PROPS_DIR=${KAFKA_REST_SAVED_PROPS_DIR:-${KAFKA_REST_PACKAGE_DIR}/config/saved}

# indicates whether this is initial run of script
KAFKA_REST_INITIAL_RUN=true
if [ -d $KAFKA_REST_SAVED_PROPS_DIR ]; then # directory exists, script was executed
    KAFKA_REST_INITIAL_RUN=false;
fi

# MapR ecosystems' restart directory and Kafka REST service restart script
MAPR_RESTART_SCRIPTS_DIR=${MAPR_RESTART_SCRIPTS_DIR:-${MAPR_HOME}/conf/restart}
KAFKA_REST_RESTART_SRC=${KAFKA_REST_RESTART_SRC:-${MAPR_RESTART_SCRIPTS_DIR}/${KAFKA_REST_NAME}-${VERSION}.restart}

# Warden-specific
KAFKA_REST_WARDEN_CONF=${KAFKA_REST_WARDEN_CONF:-${KAFKA_REST_CONF_DIR}/warden.kafka-rest.conf}
KAFKA_REST_WARDEN_DEST_CONF=${KAFKA_REST_WARDEN_DEST_CONF:-${MAPR_HOME}/conf/conf.d/warden.kafka-rest.conf}

#######################################################################
# Functions definition
#######################################################################

function print_usage() {
    cat <<EOM
Usage: $(basename $0) [-s|--secure || -u|--unsecure] [-R] [--EC] [-h|--help]
Options:
    --secure    Configure Kafka REST for secure cluster. Enables SSL for Kafka REST.

    --unsecure  Configure Kafka REST for unsecure cluster. SSL for Kafka REST won't be enabled.

    -R          Indicates that cluster is up. If this option is not specified only activities that do not require a
                cluster to be up will be performed.

    --EC         Common/shared ecosystem parameters. Example:
                --EC “-OT <openTsdbServerList>”

    --help      Provides information about usage of the script.
EOM
}

function check_for_options_conflict() {
    if $1; then
        echo "Can not be both '--secure' and '--unsecure' options. Please, specify one of them."
        return 1
    fi

    return 0
}

function write_version_file() {
    if [ -f $KAFKA_REST_VERSION_FILE ]; then
        rm -f $KAFKA_REST_VERSION_FILE
    fi
    echo $VERSION > $KAFKA_REST_VERSION_FILE
}

function setup_warden_config() {

    logInfo 'Setup Warden config'

    if [ -f $WARDEN_KAFKA_REST_DEST_CONF ]; then
        rm -f $WARDEN_KAFKA_REST_DEST_CONF
    fi
    cp $KAFKA_REST_WARDEN_CONF $KAFKA_REST_WARDEN_DEST_CONF

    chown ${MAPR_USER} ${KAFKA_REST_WARDEN_DEST_CONF}
    chgrp ${MAPR_GROUP} ${KAFKA_REST_WARDEN_DEST_CONF}
}

function change_permissions() {
    chown -R ${MAPR_USER} ${KAFKA_REST_PACKAGE_DIR}
    chgrp -R ${MAPR_GROUP} ${KAFKA_REST_PACKAGE_DIR}
    chmod u+x ${KAFKA_REST_PACKAGE_DIR}/bin/*
    chmod 640 ${KAFKA_REST_PROPERTIES}
}

function save_current_properties() {
    cp -p $KAFKA_REST_PROPERTIES ${KAFKA_REST_SAVED_PROPS_DIR}/${KAFKA_REST_NAME}.properties.${NOW}
}

function create_saved_properties_directory() {
    if [ ! -d $KAFKA_REST_SAVED_PROPS_DIR ]; then
        mkdir $KAFKA_REST_SAVED_PROPS_DIR
    fi
}

function write_kafka_rest_restart(){
    if [ ! -d $MAPR_RESTART_SCRIPTS_DIR ]; then
        mkdir $MAPR_RESTART_SCRIPTS_DIR
        chown -R ${MAPR_USER} ${MAPR_RESTART_SCRIPTS_DIR}
        chgrp -R ${MAPR_GROUP} ${MAPR_RESTART_SCRIPTS_DIR}
    fi
    
    echo -e "#!/bin/bash\nsudo -u $MAPR_USER maprcli node services -action restart -name kafka-rest -nodes `hostname`" > ${KAFKA_REST_RESTART_SRC}

    chown ${MAPR_USER} ${KAFKA_REST_RESTART_SRC}
    chgrp ${MAPR_GROUP} ${KAFKA_REST_RESTART_SRC}
    chmod u+x ${KAFKA_REST_RESTART_SRC}
}

function restart_kafka_rest_service() {
	su ${MAPR_USER} <<-EOF
	maprcli node services -name kafka-rest -action restart -nodes `hostname`
	EOF
	write_kafka_rest_restart
}

function register_port_if_available() {

    # TODO shlock issue
    #if checkNetworkPortAvailability $KAFKA_REST_PORT; then
    #    registerNetworkPort kafka-rest $KAFKA_REST_PORT
    #  else
    #    return 1
    #fi
    return 0
}

function configure() {
    mode=$1

    logInfo 'This is initial run of Kafka REST configure.sh';
    create_saved_properties_directory
    write_version_file
    if ! register_port_if_available $KAFKA_REST_PORT; then
        logErr "Can not register port '$KAFKA_REST_PORT' for Kafka REST since it is not available."
        return 1
    fi
    save_current_properties
    cp ${KAFKA_REST_CONF_TEMPLATE_DIR}/kafka-rest-${mode}.properties ${KAFKA_REST_PROPERTIES}
    cp ${KAFKA_REST_CONF_TEMPLATE_DIR}/log4j.properties ${KAFKA_REST_CONF_DIR}/log4j.properties
    cp ${KAFKA_REST_CONF_TEMPLATE_DIR}/warden.kafka-rest.conf ${KAFKA_REST_CONF_DIR}/warden.kafka-rest.conf
    if ! ${KAFKA_REST_INITIAL_RUN}; then
        write_kafka_rest_restart
    fi
    return 0
}

#######################################################################
# Parse options
#######################################################################


{ OPTS=`getopt -n "$0" -a -o suhR --long secure,unsecure,customSecure,help,EC -- "$@"`; } 2>/dev/null
eval set -- "$OPTS"

SECURITY=disabled
HELP=false
while true; do
  case "$1" in
    -s | --secure )
    SECURITY=default
    shift ;;

    -u | --unsecure )
    SECURITY=disabled
    shift ;;
    
    -cs | --customSecure)
    SECURITY=custom
    shift ;;
    
    -h | --help ) HELP=true; shift ;;

    -R)
    # sets isOnlyRoles
    shift ;;

    --EC)
     # ignoring
     shift ;;

    -- ) shift; break ;;

    * ) break ;;
  esac
done


if $HELP; then
    print_usage
    exit 0
fi

change_permissions
setup_warden_config

if [[ -z ${isOnlyRoles} ]] ; then
    case ${SECURITY} in
        disabled )
            if configure unsecure; then
                logInfo 'Kafka REST successfully configured to run in unsecure mode.'
            else
                logErr 'Error: Errors occurred while configuring Kafka REST to run in unsecure mode.'
                exit 1
            fi
        ;;

        default )
            if configure secure; then
                logInfo 'Kafka REST successfully configured to run in secure mode.'
            else
                logErr 'Error: Errors occurred while configuring Kafka REST to run in secure mode.'
                exit 1
            fi
        ;;

        custom )
        # do nothing
        ;;
    esac
fi

if [[ -f "$KAFKA_REST_PACKAGE_DIR/conf/.not_configured_yet" ]] ; then
    rm -f "$KAFKA_REST_PACKAGE_DIR/conf/.not_configured_yet"
fi

exit 0