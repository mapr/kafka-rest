#!/bin/bash
#######################################################################
# Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
#######################################################################
#
# Configure script for Kafka REST. Allows to configure Kafka in secure
# and insecure modes. Secure mode provides SSL support and generates
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
VERSION=4.1.0
KAFKA_REST_PORT=8082
NOW=`date "+%Y%m%d_%H%M%S"`
KAFKA_REST_NAME=${KAFKA_REST_NAME:-kafka-rest}
KAFKA_REST_HOME=${NEW_MYPKG_CONF_FILE:-${MAPR_HOME}/${KAFKA_REST_NAME}}
KAFKA_REST_PACKAGE_DIR=${KAFKA_REST_PACKAGE_DIR:-${KAFKA_REST_HOME}/${KAFKA_REST_NAME}-${VERSION}}
KAFKA_REST_VERSION_FILE=${KAFKA_REST_VERSION_FILE:-${KAFKA_REST_HOME}/kafkarestversion}
KAFKA_REST_PROPERTIES=${KAFKA_REST_PROPERTIES:-${KAFKA_REST_PACKAGE_DIR}/config/${KAFKA_REST_NAME}.properties}

# directory contains saved user's configs
KAFKA_REST_SAVED_PROPS_DIR=${KAFKA_REST_SAVED_PROPS_DIR:-${KAFKA_REST_PACKAGE_DIR}/config/saved}

# indicates whether this is initial run of script
KAFKA_REST_INITIAL_RUN=true
if [ -d $KAFKA_REST_SAVED_PROPS_DIR ]; then # directory exists, script was executed
    KAFKA_REST_INITIAL_RUN=false;
fi

# indicates whether cluster is up or not
KAFKA_REST_CORE_IS_RUNNING=false
if [ ! -z ${isOnlyRoles+x} ]; then # isOnlyRoles exists
    if [ $isOnlyRoles -eq 1 ] ; then
        KAFKA_REST_CORE_IS_RUNNING=true;
    fi
fi

# MapR ecosystems' restart directory and Kafka REST service restart script
MAPR_RESTART_SCRIPTS_DIR=${MAPR_RESTART_SCRIPTS_DIR:-${MAPR_HOME}/conf/restart}
KAFKA_REST_RESTART_SRC=${KAFKA_REST_RESTART_SRC:-${MAPR_RESTART_SCRIPTS_DIR}/${KAFKA_REST_NAME}-${VERSION}.restart}

# SSL-specific
KAFKA_REST_CERTIFICATES_DIR=${KAFKA_REST_CERTIFICATES_DIR:-${KAFKA_REST_PACKAGE_DIR}/certs}
KAFKA_REST_CERT=${KAFKA_CERT:-${KAFKA_REST_CERTIFICATES_DIR}/cert.pem}
KAFKA_REST_DEST_STORE=${KAFKA_REST_DEST_STORE:-${KAFKA_REST_CERTIFICATES_DIR}/keystore.p12}
KAFKA_REST_DEST_STORE_PASSWD=${KAFKA_REST_DEST_STORE_PASSWD:-'mapr123'}
KAFKA_REST_OPENSSL_KEY=${KAFKA_REST_OPENSSL_KEY:-${KAFKA_REST_CERTIFICATES_DIR}/key.pem}

MAPR_CLDB_SSL_KEYSTORE=${MAPR_CLDB_SSL_KEYSTORE:-${MAPR_HOME}/conf/ssl_keystore}
MAPR_CLDB_SSL_TRUSTSTORE=${MAPR_CLDB_SSL_TRUSTSTORE:-${MAPR_HOME}/conf/ssl_truststore}
KAFKA_REST_MAPR_CLDB_SSL_KEYSTORE_PASSWD=${KAFKA_REST_MAPR_CLDB_SSL_KEYSTORE_PASSWD:-'mapr123'}
KAFKA_REST_MAPR_CLDB_SSL_TRUSTSTORE_PASSWD=${KAFKA_REST_MAPR_CLDB_SSL_TRUSTSTORE_PASSWD:-'mapr123'}

# Warden-specific
KAFKA_REST_WARDEN_CONF=${KAFKA_REST_WARDEN_CONF:-${KAFKA_REST_PACKAGE_DIR}/config/warden.kafka-rest.conf}
KAFKA_REST_WARDEN_DEST_CONF=${KAFKA_REST_WARDEN_DEST_CONF:-${MAPR_HOME}/conf/conf.d/warden.kafka-rest.conf}



#######################################################################
# Functions definition
#######################################################################

function print_usage() {
    cat <<EOM
Usage: $(basename $0) [-s|--secure || -u|--unsecure] [-R] [--EC] [-h|--help]
Options:
    --secure    Configure Kafka REST for secure cluster. Enables SSL for Kafka REST and generates certificate, client
                key at '${KAFKA_REST_CERTIFICATES_DIR}'.

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
}

function check_mapr_cldb_keystore() {
    if [ ! -f $MAPR_CLDB_SSL_KEYSTORE ]; then
        logErr "Error: Can not enable Kafka REST SSL since MapR keystore file '$MAPR_HOME/conf/ssl_keystore' does not" \
        "exist. It seems that cluster is configured in non-secure way."
        return 1
    fi

    return 0
}

function check_mapr_cldb_truststore() {
    if [ ! -f $MAPR_CLDB_SSL_TRUSTSTORE ]; then
        logErr "Error: Can not enable Kafka REST SSL since MapR truststore file '$MAPR_CLDB_SSL_TRUSTSTORE' does not" \
        "exist. It seems that cluster is configured in non-secure way."
        return 1
    fi

    return 0
}

function save_current_properties() {
    cp -p $KAFKA_REST_PROPERTIES ${KAFKA_REST_SAVED_PROPS_DIR}/${KAFKA_REST_NAME}.properties.${NOW}
}

function create_saved_properties_directory() {
    if [ ! -d $KAFKA_REST_SAVED_PROPS_DIR ]; then
        mkdir $KAFKA_REST_SAVED_PROPS_DIR
    fi
}

function delete_certs_dir() {
    if [ -d $KAFKA_REST_CERTIFICATES_DIR ]; then
        rm -Rf ${KAFKA_REST_CERTIFICATES_DIR}
    fi
}

function create_properties_file_with_ssl_config() {
        cat >>${KAFKA_REST_PROPERTIES} <<-EOL
		listeners=https://0.0.0.0:${KAFKA_REST_PORT}
		ssl.keystore.location=${MAPR_CLDB_SSL_KEYSTORE}
		ssl.keystore.password=${KAFKA_REST_MAPR_CLDB_SSL_KEYSTORE_PASSWD}
		ssl.key.password=${KAFKA_REST_MAPR_CLDB_SSL_KEYSTORE_PASSWD}
		EOL
}

function create_standard_properties_file() {
        TMP_CONFIG=$(sed '/^listeners/d' ${KAFKA_REST_PROPERTIES} | sed '/^ssl.key/d')
        echo "$TMP_CONFIG" > ${KAFKA_REST_PROPERTIES}
}

function generate_cert_and_key() {

    { ALIAS="$(getClusterName)"; } 2>/dev/null
    mkdir -p "${KAFKA_REST_CERTIFICATES_DIR}"
    keytool -v -exportcert -alias "${ALIAS}" -keystore "${MAPR_CLDB_SSL_TRUSTSTORE}" -rfc -file "${KAFKA_REST_CERT}" \
        -storepass "${KAFKA_REST_MAPR_CLDB_SSL_TRUSTSTORE_PASSWD}" 2>/dev/null
    if [ $? -ne 0 ] ; then
        logWarn 'Warning: No certificate has been generated due to keytool error.'
        return 1
    fi

    keytool -importkeystore -noprompt \
        -srckeystore "${MAPR_CLDB_SSL_KEYSTORE}" -destkeystore "${KAFKA_REST_DEST_STORE}" \
        -srcstoretype JKS -deststoretype PKCS12 \
        -srcstorepass "${KAFKA_REST_MAPR_CLDB_SSL_KEYSTORE_PASSWD}" \
        -deststorepass "${KAFKA_REST_DEST_STORE_PASSWD}" 2>/dev/null

    if [ $? -ne 0 ] ; then
        logWarn 'Warning: No keystore has been imported due to keytool error.'
        return 1
      else
        logInfo "Keystore has been imported from JKS to PKCS12 at '${KAFKA_REST_DEST_STORE}'"
    fi

    openssl pkcs12 -in "${KAFKA_REST_DEST_STORE}" -nodes -nocerts -out "${KAFKA_REST_OPENSSL_KEY}" \
        -passin "pass:${KAFKA_REST_DEST_STORE_PASSWD}" 2>/dev/null
    if [ $? -ne 0 ] ; then
        logWarn 'Warning: No PKCS12 has been converted due to openssl error.'
        return 1
      else
        logInfo "PKCS12 has been converted to pem using OpenSSL at '${KAFKA_REST_OPENSSL_KEY}'"
    fi

    return 0
}

function enable_ssl() {
    if ! check_mapr_cldb_keystore; then
        return 1
    fi

    if ! check_mapr_cldb_truststore; then
        return 1
    fi

    save_current_properties
    create_properties_file_with_ssl_config

    if ! generate_cert_and_key; then
        return 1
    fi

    chown -R ${MAPR_USER} ${KAFKA_REST_CERTIFICATES_DIR}
    chgrp -R ${MAPR_GROUP} ${KAFKA_REST_CERTIFICATES_DIR}

    return 0
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

function configure_insecure_mode() {
    logInfo 'This is initial run of Kafka REST configure.sh';
    create_saved_properties_directory
    delete_certs_dir
    write_version_file
    if ! register_port_if_available $KAFKA_REST_PORT; then
        logErr "Can not register port '$KAFKA_REST_PORT' for Kafka REST since it is not available."
        return 1
    fi
    save_current_properties
    create_standard_properties_file
    change_permissions
    setup_warden_config
    write_kafka_rest_restart    
    return 0
}

function configure_secure_mode() {
    logInfo 'This is initial run of Kafka REST configure.sh';
    create_saved_properties_directory
    delete_certs_dir
    write_version_file

    if ! register_port_if_available $KAFKA_REST_PORT; then
       logErr "Can not register port '$KAFKA_REST_PORT' for Kafka REST since it is not available."
       return 1
    fi

    if ! enable_ssl; then
        return 1
    fi
    change_permissions
    setup_warden_config
    write_kafka_rest_restart       
    return 0
}


#######################################################################
# Parse options
#######################################################################


{ OPTS=`getopt -n "$0" -a -o suhR --long secure,unsecure,customSecure,help,EC -- "$@"`; } 2>/dev/null
eval set -- "$OPTS"

SECURE=false
CUSTOM=false
HELP=false
while true; do
  case "$1" in
    -s | --secure )
    SECURE=true;
    shift ;;

    -u | --unsecure )
    SECURE=false;
    shift ;;
    
    -cs | --customSecure)  
      if [ -f "$KAFKA_REST_PACKAGE_DIR/conf/.not_configured_yet" ]; then
        SECURE=true;
      else
        SECURE=false;
        CUSTOM=true;
      fi
    shift ;;
    
    -h | --help ) HELP=true; shift ;;

    -R) KAFKA_REST_CORE_IS_RUNNING=true; shift ;;

    --EC)
     # ignoring
     shift ;;

    -- ) shift; break ;;

    * ) break ;;
  esac
done


if $HELP; then
    print_usage
fi  

if [ -f "$KAFKA_REST_PACKAGE_DIR/conf/.not_configured_yet" ]  ; then
    rm -f "$KAFKA_REST_PACKAGE_DIR/conf/.not_configured_yet"
fi
 
if $SECURE; then
    num=3
    IS_SECURE_CONFIG=$(grep -e ssl.key -e listeners  ${KAFKA_REST_PROPERTIES} | wc -l)
    if [ $IS_SECURE_CONFIG -lt $num ]; then
        if configure_secure_mode; then
            logInfo 'Kafka REST successfully configured to run in secure mode.'
        else
            logErr 'Error: Errors occurred while configuring Kafka REST to run in secure mode.'
            exit 1
        fi
    else
        change_permissions
        setup_warden_config
        write_kafka_rest_restart    
	    logInfo ''Kafka REST has been already configured to run in secure mode.''
    fi
else
    setup_warden_config
    change_permissions
    write_kafka_rest_restart    
    if $CUSTOM; then
        exit 0
    fi
    if grep -q ssl "$KAFKA_REST_PROPERTIES"; then
       configure_insecure_mode
       logInfo 'Kafka REST successfully configured to run in unsecure mode.'
    fi
fi

exit 0