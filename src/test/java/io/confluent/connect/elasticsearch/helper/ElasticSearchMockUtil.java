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
    // build_flavor and build_type are required by the Java API Client: ElasticsearchVersionInfo
    // marks them non-null, so a response omitting them fails deserialization with
    // "Missing required property 'ElasticsearchVersionInfo.buildFlavor'" and the whole request is
    // reported as a failure even on a 200. The high level REST client tolerated their absence.
    // version.number is 8.x here because the client only supports 8.x servers.
    response.put("name", "KafkaESClusterNodeold_1")
        .put("cluster_name", "KafkaESCluster")
        .put("cluster_uuid", "83EJmDNrRVirBWcZDgs9ew")
        .put("tagline", "You Know, for Search")
        .putObject("version")
        .put("number", "8.19.19")
        .put("build_flavor", "default")
        .put("build_type", "docker")
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

  /**
   * A minimal valid {@code _bulk} response body: no items and no errors.
   *
   * <p>Needed because {@link #minimumResponseJson()} is shaped like the cluster-info response
   * ({@code GET /}), and a catch-all {@code any(anyUrl())} stub serving that body for a
   * {@code POST /_bulk} request now fails deserialization with "Missing required property
   * 'BulkResponse.took'" / "'BulkResponse.errors'". The Java API Client marks both non-null,
   * whereas the high level REST client tolerated their absence. Tests that do not stub
   * {@code /_bulk} explicitly should fall back to {@link #basicBulkOk()} rather than the
   * info-shaped catch-all.
   */
  public static String minimumBulkResponseJson() {
    try {
      ObjectNode response = MAPPER.createObjectNode();
      response.put("took", 30).put("errors", false).putArray("items");
      return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(response);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          "Error writing default bulk response json to string: " + e.getMessage(), e
      );
    }
  }

  /**
   * A valid, empty "OK" response for {@code POST /_bulk}.
   * @return ResponseDefinitionBuilder for a decodable bulk response
   */
  public static ResponseDefinitionBuilder basicBulkOk() {
    return addMinimalHeaders(WireMock.ok().withBody(minimumBulkResponseJson()));
  }

}
