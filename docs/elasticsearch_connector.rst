Elasticsearch  Connector
========================

The Elasticsearch connector allows to move data out of Kafka to Elasticsearch.

Quickstart
----------
In this Quickstart, we use the Elasticsearch connector to export data produced by the Avro console
producer to Elasticsearch.

Start Zookeeper, Kafka and SchemaRegistry if you haven't done so. The instructions on how to start
these services are available at the Confluent Platform QuickStart. You also need to have
Elasticsearch running locally or remotely and make sure that you know the address to connect to
Elasticsearch.

This Quickstart assumes that you started the required services with the default configurations and
you should make necessary changes according to the actual configurations used.

First, start the Avro console producer::

  $ ./bin/kafka-avro-console-producer --broker-list localhost:9092 --topic test-elasticsearch \
  --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'

Then in the console producer, type in::

  {"f1": "value1"}
  {"f1": "value2"}
  {"f1": "value3"}

The three records entered are published to the Kafka topic ``test-elasticsearch`` in Avro format.

Before starting the connector, please make sure that the configurations in
``etc/kafka-connect-elasticsearch/quickstart-elasticsearch.properties`` are properly set to your
configurations of Elasticsearch, e.g. ``http.address`` points to the correct http address.
Then run the following command to start Kafka Connect with the Elasticsearch connector::

  $ ./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties \
  etc/kafka-connect-elasticsearch/quickstart-elasticsearch.properties

You should see that the process starts up and logs some messages, and then exports data from Kafka
to Elasticsearch. Once the connector finishes ingesting data to Elasticsearch, check that the data
is available in Elasticsearch::

  $ search

Features
--------
The Elasticsearch connector offers a bunch of features:

* **Exactly Once Delivery**: The connector relies on Elasticsearch's write semantics to ensure
  exactly once delivery to Elasticsearch. In case of keys are ignored, the Elasticsearch connector
  uses ``topic+partition+offset`` as document id to ensure that the same record in Kafka only creates
  one document in Elasticsearch even in case that it is written to Elasticsearch multiple times.
  In case of keys are kept , the keys are used as the document ids and the connector ensures that
  only one document for the same key exists in Elasticsearch.

* **Batching and Pipelining**: The connector supports batching and pipelined writing to Elasticsearch.
  It accumulates messages in batches and allows concurrent processing of multiple batches.

* **Delivery Ordering**: When pipelining is turned off, the connector supports ordering of delivery
  on a per batch basis. This is useful for cases that ordering of messages is important.

* **Mapping Inference**: The connector can infer mappings from the Kafka Connect schemas.
  When this is enabled, the connector creates a mapping based on the schema from the message. However
  , the inference is limited to field types and default values when a field is missing. If more
  customizations are needed (e.g. user defined analyzers) for indices, we highly recommend to
  manually create mappings.

* **Schema Evolution**: The connector supports schema evolution can handle backward, forward and
  full compatible changes of schemas in Kafka Connect. It can also handle some incompatible schema
  changes such as changing a field from integer to string.

Schema Evolution
----------------
The Elasticsearch connector writes data in different topics in Kafka to different indices. All
data for a topic will have the same type in Elasticseearch. This allows independent evolution of
schemas for data in different topics. This simplifies the schema evolution as the enforcement for
mappings in Elasticsearch is the following: all fields with the same name in the same index must
have the same mapping.

The Elasticsearch connector supports schema evolution as mappings in Elasticsearch are more
flexible than the Schema evolution allowed in Kafka Connect. New fields can be added as
Elasticsearch can infer the type for each newly added fields. Missing fields will be treated
as the null value defined for those fields in the mapping. Moreover, type changes for fields are
allowed in case that the types can be merged. For example, if a field defined as a string type.
If we change schema for that field to integer type later, the connector can still write the records
with the new schema to Elasticsearch. As the mappings are more flexible, schema compatibility
should be enforced when writing data to Kafka.

However, some incompatible changes of Kafka Connect can cause the connector to fail to write to
Elasticsearch. For example, the connector is not able to write the data to Elasticsearch if we
change a field in Kafka Connect schema from string to integer.

Also, there are some changes that are not allowed after a mapping is already defined. Although you
can add new types to an index, or add new fields to a type, you can’t add new analyzers or
make changes to existing fields. If you were to do so, the data that had already been indexed would
be incorrect and your searches would no longer work as expected. It is highly recommended that
to manually define mappings before writing data to Elasticsearch.



