# ElasticsearchContainer does not use this Dockerfile, but dynamically builds its own
# Dockerfile that is patterned very closely after this Dockerfile.
#
# This image can be used for local testing and exploration, but strictly speaking
# is not used in the integration tests.

FROM docker.elastic.co/elasticsearch/elasticsearch:7.0.0

# We require these to generate certs
RUN yum -y install openssl

# Install our script to generate certs
ENV STORE_PASSWORD=asdfasdf
ENV ELASTIC_PASSWORD=elastic

RUN mkdir -p /usr/share/elasticsearch/config/certs
COPY ./instances.yml /usr/share/elasticsearch/config/ssl/instances.yml
COPY ./setup-elasticsearch.sh /usr/share/elasticsearch/config/ssl/setup-elasticsearch.sh
RUN /usr/share/elasticsearch/config/ssl/setup-elasticsearch.sh

