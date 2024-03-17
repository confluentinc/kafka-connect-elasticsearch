package io.confluent.connect.elasticsearch.helper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;

/**
 * Some utility functions to help mocking Elasticsearch via WireMock
 */
public class ElasticSearchMockUtil {
  public static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Add standard ElasticSearch version info to a JSON object
   * @param response The json object (usually a response) to
   *                 which to add the version info
   * @return The update JSON object node
   */
  static public ObjectNode addStandardVersionInfo(ObjectNode response) {
    // Note that "version.number" is somewhat arbitrary for our testing purposes,
    // although for some version (i.e. [7.0,7.14]) it checks for other fields,
    // so the mock might fail in that case.
    response.put("name", "KafkaESClusterNodeold_1")
        .put("cluster_name", "KafkaESCluster")
        .put("cluster_uuid", "83EJmDNrRVirBWcZDgs9ew")
        .put("tagline", "You Know, for Search")
        .putObject("version")
        .put("number", "7.16.3")
        .put("build_hash", "83EJmDNrRVirBWcZDgs9ew")
        .put("build_date", "2018-04-12T16:25:14.838Z")
        .put("build_snapshot", "false")
        .put("lucene_version", "6.6.1")
        .put("minimum_wire_compatibility_version", "1.1.1")
        .put("minimum_index_compatibility_version", "2.2.2");
    return response;
  }

  /**
   * Add the minimal response headers required by ElasticSearch client
   * @param builder The response builder for WireMock
   * @return Updated ResponseBuilder
   */
  static public ResponseDefinitionBuilder addMinimalHeaders(ResponseDefinitionBuilder builder) {
    // Now header [X-Elastic-Product]
    return builder
        .withHeader("X-Elastic-Product", "Elasticsearch")
        .withHeader(CONTENT_TYPE, "application/json");
  }

  /**
   * A standard "empty" response from ElasticSearch which includes the required version
   * information in the json body.
   * @return The minimum-allowable response from ElasticSearch for responses to calls such
   *         as "ping"
   */
  public static String minimumResponseJson() {
    try {
      return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(
          addStandardVersionInfo(MAPPER.createObjectNode())
      );
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          "Error writing default output json to string: " + e.getMessage(), e
      );
    }
  }

  /**
   * Convenience drop-in replacement for static import of WireMock.ok()
   * @return ResponseDefinitionBuilder necessary for a valid "OK" response from
   *         ElasticSearch.
   */
  public static ResponseDefinitionBuilder basicEmptyOk() {
    return addMinimalHeaders(WireMock.ok().withBody(minimumResponseJson()));
  }

}
