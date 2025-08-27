%undefine __check_files

summary:     HPE DataFabric Ecosystem Pack: Confluent Kafka REST
license:     Hewlett Packard Enterprise, CopyRight
Vendor:      Hewlett Packard Enterprise
name:        mapr-kafka-rest
version:     __RELEASE_VERSION__
release:     1
prefix:      /
group:       HPE
buildarch:   noarch
requires:    mapr-client >= 7.2.0, mapr-kafka >= 3.6.1
conflicts:   mapr-core < 7.2.0, mapr-kafka < 3.6.1
AutoReqProv: no


%description
Confluent Kafka REST distribution included in HPE DataFabric Software Ecosystem Pack
Tag: __RELEASE_BRANCH__
Commit: __GIT_COMMIT__


%clean
echo "NOOP"


%files
__PREFIX__/kafka-rest
__PREFIX__/roles

%pre
# $1 -eq 1 install
# $1 -eq 2 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "pre install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :




if [ "$1" = "2" ]; then
    if [ -f __PREFIX__/kafka-rest/kafkarestversion ] || [ -f __PREFIX__/kafka-rest/kafka-restversion ]; then
        OLD_TIMESTAMP=$(rpm --queryformat='%%{VERSION}' -q mapr-kafka-rest)
        OLD_VERSION=$(echo "$OLD_TIMESTAMP" | grep -o '^[0-9]*\.[0-9]*\.[0-9]*')

        OLD_TIMESTAMP_FILE="%{_localstatedir}/lib/rpm-state/mapr-kafka-rest-old-timestamp"
        OLD_VERSION_FILE="%{_localstatedir}/lib/rpm-state/mapr-kafka-rest-old-version"

        STATE_DIR="$(dirname $OLD_TIMESTAMP_FILE)"
        if [ ! -d "$STATE_DIR" ]; then
            mkdir -p "$STATE_DIR"
        fi

        echo "$OLD_TIMESTAMP" > "$OLD_TIMESTAMP_FILE"
        echo "$OLD_VERSION" > "$OLD_VERSION_FILE"
        mkdir -p __PREFIX__/kafka-rest/kafka-rest-$OLD_TIMESTAMP/config
        cp -r __PREFIX__/kafka-rest/kafka-rest-$OLD_VERSION/config/* __PREFIX__/kafka-rest/kafka-rest-$OLD_TIMESTAMP/config
        DAEMON_CONF=__PREFIX__/conf/daemon.conf

        if [ -f "$DAEMON_CONF" ]; then
            MAPR_USER=$( awk -F = '$1 == "mapr.daemon.user" { print $2 }' $DAEMON_CONF)
            MAPR_GROUP=$( awk -F = '$1 == "mapr.daemon.group" { print $2 }' $DAEMON_CONF)
            if [ ! -z "$MAPR_USER" ]; then
                chown -R ${MAPR_USER}:${MAPR_GROUP} __PREFIX__/kafka-rest/kafka-rest-$OLD_TIMESTAMP
            fi
        fi
    fi
fi





%post
# $1 -eq 1 install
# $1 -eq 2 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "post install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :


echo Post Configuration
if [ "$1" -eq "2" ]; then
    if [ -f __PREFIX__/kafka-rest/kafkarestversion ]; then
        rm -f __PREFIX__/kafka-rest/kafkarestversion
    fi
fi
echo "__VERSION_3DIGIT__" > __PREFIX__/kafka-rest/kafka-restversion

mkdir -p "__INSTALL_3DIGIT__"/config
touch "__INSTALL_3DIGIT__/config/.not_configured_yet"

if [ "$1" -eq "2" ]; then
    if [ -f __PREFIX__/conf/conf.d/warden.kafka-rest.conf ]; then
        rm -Rf __PREFIX__/conf/conf.d/warden.kafka-rest.conf
    fi
	OLD_CONFIG=$(ls  -r  __PREFIX__/kafka-rest/  | grep  "kafka-rest-__VERSION_3DIGIT__." |  head -1)
	if [ ! -z $OLD_CONFIG ]; then
		cp __PREFIX__/kafka-rest/${OLD_CONFIG}/config/*  __INSTALL_3DIGIT__/config/
	fi
fi



%preun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "preun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

# stop service if it is running
bash __INSTALL_3DIGIT__/bin/kafka-rest-stop

if [ -d  __INSTALL_3DIGIT__/conf/ ]; then
    rm -Rf __INSTALL_3DIGIT__/conf/
fi
if [ -f __PREFIX__/conf/conf.d/warden.kafka-rest.conf ]; then
    rm -Rf __PREFIX__/conf/conf.d/warden.kafka-rest.conf
fi
if [ "$1" -eq "0" ]; then
    rm -rf __INSTALL_3DIGIT__/logs
    rm -Rf __INSTALL_3DIGIT__/certs/
    rm -Rf  __INSTALL_3DIGIT__/config/saved/
    rm -rf  __PREFIX__/conf/restart/kafka-rest-7.6.0.restart

fi

%postun
# N/A     install
# $1 -eq 1 upgrade
# $1 -eq 0 uninstall
[ -n "$VERBOSE" ] && echo "postun install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :
if [ "$1" -eq "0" ]; then
    rm -rf __PREFIX__/kafka-rest
fi


%posttrans
# $1 -eq 0 install
# $1 -eq 0 upgrade
# N/A     uninstall
[ -n "$VERBOSE" ] && echo "posttrans install called with argument \`$1'" >&2
[ -n "$VERBOSE" ] && set -x ; :

if [ ! -d "__INSTALL_3DIGIT__"/config ]; then
    mkdir -p "__INSTALL_3DIGIT__"/config
    touch "__INSTALL_3DIGIT__/config/.not_configured_yet"
fi

OLD_TIMESTAMP_FILE="%{_localstatedir}/lib/rpm-state/mapr-kafka-rest-old-timestamp"
OLD_VERSION_FILE="%{_localstatedir}/lib/rpm-state/mapr-kafka-rest-old-version"

# This files will exist only on upgrade
if [ -e "$OLD_TIMESTAMP_FILE" ] && [ -e "$OLD_VERSION_FILE" ]; then
    OLD_TIMESTAMP=$(cat "$OLD_TIMESTAMP_FILE")
    OLD_VERSION=$(cat "$OLD_VERSION_FILE")

    rm "$OLD_TIMESTAMP_FILE" "$OLD_VERSION_FILE"

    # Remove directory with old version
    NEW_VERSION=$(cat __PREFIX__/kafka-rest/kafka-restversion)

    if [ "$OLD_VERSION" != "$NEW_VERSION" ]; then
        rm -rf "__PREFIX__/kafka-rest/kafka-rest-${OLD_VERSION}"
    fi
fi
