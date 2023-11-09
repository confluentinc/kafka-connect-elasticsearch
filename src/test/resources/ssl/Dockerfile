# ElasticsearchContainer does not use this Dockerfile, but dynamically builds its own
# Dockerfile that is patterned very closely after this Dockerfile.
#
# This image can be used for local testing and exploration, but strictly speaking
# is not used in the integration tests.

FROM docker.elastic.co/elasticsearch/elasticsearch:8.2.2

# Install our script to generate certs
ENV STORE_PASSWORD=asdfasdf
ENV ELASTIC_PASSWORD=elastic

USER root

RUN mkdir -p /usr/share/elasticsearch/config/certs
COPY ./elasticsearch.yml /usr/share/elasticsearch/config/elasticsearch.yml
COPY ./instances.yml /usr/share/elasticsearch/config/ssl/instances.yml
COPY ./start-elasticsearch.sh /usr/share/elasticsearch/config/ssl/start-elasticsearch.sh
RUN chmod +x /usr/share/elasticsearch/config/ssl/start-elasticsearch.sh
RUN /usr/share/elasticsearch/config/ssl/start-elasticsearch.sh

ENTRYPOINT /usr/share/elasticsearch/config/ssl/start-elasticsearch.sh

