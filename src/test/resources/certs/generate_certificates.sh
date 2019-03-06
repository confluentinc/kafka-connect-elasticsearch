#!/usr/bin/env bash

# Generate the certificates used in the HTTPS tests.
# Copy these into the docker image to make available to client (Java class in this repo) and
# elastic server (docker container started in test setup). Volume mounting is unsolved issue.

DOMAIN=172.17.0.1

openssl genrsa -out key.pem 2048
openssl req -new -x509 -keyout cacert.key -out ca.pem -days 3000 \
    -passin pass:asdfasdf \
    -passout pass:asdfasdf \
    -subj "/C=US/ST=CA/L=Palo Alto/O=Confluent/CN=$DOMAIN"
openssl req -new -key key.pem -out cert.csr \
    -subj "/C=US/ST=CA/L=Palo Alto/O=Confluent/CN=$DOMAIN"
openssl x509 -req -in cert.csr -CA ca.pem -CAkey cacert.key -CAcreateserial \
    -passin pass:asdfasdf \
    -out cert.pem -days 10000 -sha256
openssl pkcs12 -export -out bundle.p12 -in cert.pem -inkey key.pem \
    -passin pass:asdfasdf \
    -passout pass:asdfasdf
keytool -keystore truststore.jks -import -file ca.pem -alias cacert \
    -storepass asdfasdf -noprompt
keytool -destkeystore keystore.jks -deststorepass asdfasdf \
    -importkeystore -srckeystore bundle.p12  -srcstorepass asdfasdf -srcstoretype PKCS12 \
    -noprompt

chmod 777 ./*
