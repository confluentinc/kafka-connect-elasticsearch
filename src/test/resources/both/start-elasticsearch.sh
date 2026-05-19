#!/bin/bash

# Generate the certificates used in the HTTPS tests.
# Copy these into the docker image to make available to client (Java class in this repo) and
# elastic server (docker container started in test setup). Volume mounting is unsolved issue.

ES_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}"/ )" >/dev/null 2>&1 && pwd )"
cd $ES_DIR/../..
ES_DIR=$(pwd)
SAVE_PATH="$PATH"

if [[ -z "${IP_ADDRESS}" ]]; then
    IP_ADDRESS=$(hostname -I)
fi

echo
echo "Replacing the ip address in the ${ES_DIR}/config/ssl/instances.yml file with ${IP_ADDRESS}"
sed -i "s/ipAddress/${IP_ADDRESS}/g" ${ES_DIR}/config/ssl/instances.yml


echo "Setting up Elasticsearch and generating certificates in ${ES_DIR}"

if [[ -n "$ELASTIC_PASSWORD" ]]; then
    # Add ES bundled JDK to PATH as fallback if system Java (keytool) is not present.
    command -v keytool &>/dev/null || export PATH="/usr/share/elasticsearch/jdk/bin:$PATH"

    echo "=== CREATE Keystore ==="
    echo "Elastic password is: $ELASTIC_PASSWORD"
    if [ -f ${ES_DIR}/config/elasticsearch.keystore ]; then
        echo "Removing old ${ES_DIR}/config/elasticsearch.keystore"
        rm ${ES_DIR}/config/elasticsearch.keystore
    fi
    [[ -f ${ES_DIR}/config/elasticsearch.keystore ]] || (${ES_DIR}/bin/elasticsearch-keystore create)
    echo "Setting bootstrap.password..."
    (echo "$ELASTIC_PASSWORD" | ${ES_DIR}/bin/elasticsearch-keystore add -x 'bootstrap.password')

    # Create SSL Certs
    echo "=== CREATE SSL CERTS ==="

    # check if old cluster-ca.zip exists, if it does remove and create a new one.
    if [ -f ${ES_DIR}/config/ssl/cluster-ca.zip ]; then
        echo "Removing old ca zip..."
        rm ${ES_DIR}/config/ssl/cluster-ca.zip
    fi
    echo "Creating cluster-ca.zip... (warnings are benign)"
    ${ES_DIR}/bin/elasticsearch-certutil ca --pem --silent --out ${ES_DIR}/config/ssl/cluster-ca.zip

    # check if ca directory exists, if does, remove then unzip new files
    if [ -d ${ES_DIR}/config/ssl/ca ]; then
        echo "CA directory exists, removing..."
        rm -rf ${ES_DIR}/config/ssl/ca
    fi

    echo "Unzip ca files..."
    unzip ${ES_DIR}/config/ssl/cluster-ca.zip -d ${ES_DIR}/config/ssl
    rm -f ${ES_DIR}/config/ssl/cluster-ca.zip

    # check if certs zip exist. If it does remove and create a new one.
    if [ -f ${ES_DIR}/config/ssl/cluster.zip ]; then
        echo "Remove old cluster.zip zip..."
        rm ${ES_DIR}/config/ssl/cluster.zip
    fi
    echo "Create cluster certs zipfile... (warnings are benign)"
    ${ES_DIR}/bin/elasticsearch-certutil cert --silent --pem --in ${ES_DIR}/config/ssl/instances.yml --out ${ES_DIR}/config/ssl/cluster.zip --ca-cert ${ES_DIR}/config/ssl/ca/ca.crt --ca-key ${ES_DIR}/config/ssl/ca/ca.key

    if [ -d ${ES_DIR}/config/ssl/docker-cluster ]; then
        rm -rf ${ES_DIR}/config/ssl/cluster
    fi
    echo "Unzipping cluster certs zipfile..."
    unzip ${ES_DIR}/config/ssl/cluster.zip -d ${ES_DIR}/config/ssl/cluster
    rm -f ${ES_DIR}/config/ssl/cluster.zip

    echo "Move elasticsearch certs to SSL config dir..."
    mv ${ES_DIR}/config/ssl/cluster/elasticsearch/* ${ES_DIR}/config/ssl/

    echo "Generating truststore at ${ES_DIR}/config/ssl/truststore.jks"
    # -storetype JKS: Java 9+ defaults to PKCS12; Kafka's SslFactory defaults to JKS, so force it.
    keytool -keystore ${ES_DIR}/config/ssl/truststore.jks -storetype JKS -import -file ${ES_DIR}/config/ssl/ca/ca.crt -alias cacert -storepass $STORE_PASSWORD -noprompt

    echo "Generating keystore for client at ${ES_DIR}/config/ssl/keystore.jks"
    # Use elasticsearch-certutil to create a PKCS12 client cert (avoids needing openssl).
    ${ES_DIR}/bin/elasticsearch-certutil cert --silent --pass $STORE_PASSWORD \
        --ca-cert ${ES_DIR}/config/ssl/ca/ca.crt --ca-key ${ES_DIR}/config/ssl/ca/ca.key \
        --out ${ES_DIR}/config/ssl/client.p12

    # Convert the PKCS12 keystore to JKS keystore.
    # -deststoretype JKS: Java 9+ defaults to PKCS12; Kafka's SslFactory defaults to JKS, so force it.
    keytool -importkeystore -destkeystore ${ES_DIR}/config/ssl/keystore.jks -deststoretype JKS -deststorepass $STORE_PASSWORD -srckeystore ${ES_DIR}/config/ssl/client.p12 -srcstoretype PKCS12 -srcstorepass $STORE_PASSWORD -noprompt
    rm -f ${ES_DIR}/config/ssl/client.p12
fi

# Set path to find ElasticSearch's java, while reverting any initial injection
# of the system-installed java
export PATH=/usr/share/elasticsearch/jdk/bin/:$SAVE_PATH

echo "Elasticsearch Configuration"
cat /usr/share/elasticsearch/config/elasticsearch.yml

echo
echo "Starting Elasticsearch with SSL and Kerberos enabled ..."
# eswrapper is the ES 8.x+ process supervisor that starts Elasticsearch as the elasticsearch user
# and handles signal forwarding. exec replaces this shell so signals reach ES directly.
# Replaces the older "su - elasticsearch" heredoc pattern used in earlier ES Docker images.
exec /usr/local/bin/docker-entrypoint.sh eswrapper

