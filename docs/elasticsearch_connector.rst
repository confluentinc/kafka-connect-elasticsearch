Elasticsearch  Connector
========================
The Elasticsearch connector allows moving data from Kafka to Elasticsearch. It writes data from
a topic in Kafka to an `index <https://www.elastic.co/guide/en/elasticsearch/reference/current/_basic_concepts.html#_index>`_
in Elasticsearch and all data for a topic have the same
`type <https://www.elastic.co/guide/en/elasticsearch/reference/current/_basic_concepts.html#_type>`_.

Elasticsearch is often used for text queries, analytics and as an key-value store
(`use cases <https://www.elastic.co/blog/found-uses-of-elasticsearch>`_). The connector covers
both the analytics and key-value store use cases. For the analytics use case,
each message is in Kafka is treated as an event and the connector uses ``topic+partition+offset``
as a unique identifier for events, which then converted to unique documents in Elasticsearch.
For the key-value store use case, it supports using keys from Kafka messages as document ids in
Elasticsearch and provides configurations ensuring that updates to a key are written to Elasticsearch
in order. For both use cases, Elasticsearch's idempotent write semantics guarantees exactly once
delivery.

`Mapping <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html>`_ is the
process of defining how a document, and the fields it contains, are stored and indexed. Users can
explicitly define mappings for types in indices. When mapping is not explicitly defined,
Elasticsearch can determine field names and types from data, however, some types such as timestamp
and decimal, may not be correctly inferred. To ensure that the types are correctly inferred, the
connector provides a feature to infer mapping from the schemas of Kafka messages.

Quickstart
----------
In this Quickstart, we use the Elasticsearch connector to export data produced by the Avro console
producer to Elasticsearch.

Start Zookeeper, Kafka and Schema Registry if you haven't done so. You also need to have
Elasticsearch running locally or remotely and make sure that you know the address to connect to
Elasticsearch.

.. ifconfig:: platform_docs

   The instructions on how to start these services are available at the
   :ref:`Confluent Platform quickstart<quickstart>`.

This Quickstart assumes that you started the required services with the default configurations.
If you are not using the default settings, you should adjust the subsequent commands to account for
different hostnames and ports.

First, start the Avro console producer::

  $ ./bin/kafka-avro-console-producer --broker-list localhost:9092 --topic test-elasticsearch-sink \
  --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'

Then in the console producer, type in::

  {"f1": "value1"}


The three records entered are published to the Kafka topic ``test-elasticsearch`` in Avro format.

Before starting the connector, please make sure that the configurations in
``etc/kafka-connect-elasticsearch/quickstart-elasticsearch.properties`` are properly set to your
configurations of Elasticsearch, e.g. ``connection.url`` points to the correct http address.
Then run the following command to start Kafka Connect with the Elasticsearch connector::

  $ ./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties \
  etc/kafka-connect-elasticsearch/quickstart-elasticsearch.properties

You should see that the process starts up and logs some messages, and then exports data from Kafka
to Elasticsearch. Once the connector finishes ingesting data to Elasticsearch, check that the data
is available in Elasticsearch::

  $ curl -XGET 'http://localhost:9200/test-elasticsearch-sink/_search?pretty'
  {
   "took" : 2,
   "timed_out" : false,
   "_shards" : {
     "total" : 5,
     "successful" : 5,
     "failed" : 0
   },
   "hits" : {
     "total" : 1,
     "max_score" : 1.0,
     "hits" : [ {
       "_index" : "test-elasticsearch-sink",
       "_type" : "kafka-connect",
       "_id" : "test-elasticsearch-sink+0+0",
       "_score" : 1.0,
       "_source" : {
         "f1" : "value1"
       }
     }]
   }
Features
--------
The Elasticsearch connector offers a bunch of features:

* **Exactly Once Delivery**: The connector relies on Elasticsearch's idempotent write semantics to
  ensure exactly once delivery to Elasticsearch. By setting ids in Elasticsearch documents, the
  connector can ensure exactly once delivery. If keys are included in Kafka messages, these keys
  are translated to Elasticsearch document ids automatically. When the keys are not included,
  or are explicitly ignored, the connector will use ``topic+partition+offset`` as the key,
  ensuring each message in Kafka has exactly one document corresponding to it in Elasticsearch.

* **Mapping Inference**: The connector can infer mappings from the Kafka Connect schemas. When
  enabled, the connector creates mappings based on schemas of Kafka messages. However, the inference
  is limited to field types and default values when a field is missing. If more customizations are
  needed (e.g. user defined analyzers), we highly recommend to manually create mappings.

* **Schema Evolution**: The connector supports schema evolution and can handle backward, forward and
  fully compatible changes of schemas in Kafka Connect. It can also handle some incompatible schema
  changes such as changing a field from integer to string.

Delivery Semantics
------------------
The connector supports batching and pipelined writes to Elasticsearch to boost throughput. It
accumulates messages in batches and allows concurrent processing of multiple batches. However,
when piplining is enabled, the ordering of batches written to Elasticsearch is not guaranteed.
A transient failure can cause a batch to be retried and because of this, a batch sent later may
be written to Elasticsearch first. Pipeplining should be turned off when order of batches is
important (set ``max.in.flight.requests=1``). Moreover, batching should be turned off when message
order is important (set ``batch.size=1``).

Mapping Management
------------------
Before using the connector, you need to think carefully on how the data should be tokenized,
analyzed and indexed, which are determined by mapping. Some changes are not allowed after a mapping
is already defined. Although you can add new types to an index, or add new fields to a type, you
can’t add new analyzers or make changes to existing fields. If you were to do so, the data that
had already been indexed would be incorrect and your searches would no longer work as expected.
It is highly recommended that to manually define mappings before writing data to Elasticsearch.

`Index templates <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html>`_
can be helpful when manually define mappings. It allows you to define templates that will
automatically be applied when new indices are created. The templates include both settings and
mappings, and a simple pattern template that controls whether the template should be applied to
the new index.

Schema Evolution
----------------
The Elasticsearch connector writes data from different topics in Kafka to different indices. All
data for a topic will have the same type in Elasticseearch. This allows independent evolution of
schemas for data from different topics. This simplifies the schema evolution as Elasticsearch has
one enforcement on mappings: all fields with the same name in the same index must have the same
mapping.

Elasticsearch supports dynamic mapping: when it encounters previously unknown field in a document,
it uses `dynamic mapping <https://www.elastic.co/guide/en/elasticsearch/guide/current/dynamic-mapping.html>`_
to determine the datatype for the field and automatically adds the new field to the type mapping.

When dynamic mapping is enabled, the Elasticsearch connector supports schema evolution as mappings
in Elasticsearch are more flexible than the schema evolution allowed in Kafka Connect when different
converters are used. For example, when the Avro converter is used, backward, forward and fully
compatible schema evolutions are allowed.

When dynamic mapping is enabled, the Elasticsearch connector allows the following schema changes:

* **Adding Fields**: Adding one or more fields to Kafka messages. Elasticsearch will add the new
  fields to the mapping when dynamic mapping is enabled.
* **Removing Fields**: Removing one or more fields to Kafka messages. Missing fields will be treated
  as the null value defined for those fields in the mapping.
* **Changing types that can be merged**: Changing a field from string type to integer type.
  For example, Elasticsearch can convert integers to strings.

The following change is not allowed:

* **Changing types that can not be merged**: Changing a field from integer type to string type.

As mappings are more flexible, schema compatibility should be enforced when writing data to Kafka.

Reindexing
----------
In some cases, the way to index a set of documents may need to be changed. For example, the analyzer,
tokenizer and which fields are indexed may need to be changed. As those cannot be changed once a
mapping is defined, we have to reindex the data.
`Index aliases <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html>`_
can be used to achieve reindexing with zero downtime. Here are the steps at needs to be performed
in Elasticsearch:

   1. Create an alias for the index with the old mapping.
   2. The applications that uses the index are pointed to the alias.
   3. Create a new index with the updated mapping.
   4. Move data from old to the new index.
   5. Atomically move the alias to the new index.
   6. Delete the old index.

For zero downtime reindexing, there are still write requests coming during the reindex period.
As aliases do not allow writing to both the old and the new index at the same time. To solve this,
the same data needs to be written both to the old and the new index.

When the Elasticsearch connector is used to write data to Elasticsearch, we can use two
connector jobs to achieve double writes:

   1. The connector job that ingest data to the old indices continue writing to the old indices.
   2. Create a new connector job that writes to new indices. This will copy both some old data and
      new data to the new indices as long as the data is in Kafka.
   3. Once the data in the old indices are moved to the new indices by the reindexing process, we
      can stop the old connector job.
