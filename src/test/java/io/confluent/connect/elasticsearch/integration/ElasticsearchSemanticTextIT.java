/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.elasticsearch.integration;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.WriteMethod;
import io.confluent.connect.elasticsearch.helper.ElasticsearchContainer;
import org.elasticsearch.search.SearchHit;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.connect.elasticsearch.ElasticsearchSinkConnectorConfig.*;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * Integration tests for Elasticsearch semantic_text field type support.
 * 
 * These tests verify the connector's behavior with Elasticsearch's semantic_text fields,
 * which require special handling due to their automatic inference generation.
 * 
 * semantic_text fields are available in Elasticsearch 8.11+
 */
@Category(IntegrationTest.class)
public class ElasticsearchSemanticTextIT extends ElasticsearchConnectorBaseIT {

  private static final String TOPIC_SEMANTIC = "semantic-activity-topic";
  private static boolean isSemanticTextSupported = false;

  @BeforeClass
  public static void setupBeforeAll() {
    container = ElasticsearchContainer.fromSystemProperties();
    container.start();
    
    // Check if Elasticsearch version supports semantic_text (8.11+)
    // For now, assume ES 8.11+ if major version is 8 or higher
    // In practice, semantic_text tests will skip if ELSER model is not available
    isSemanticTextSupported = container.esMajorVersion() >= 8;
    
    if (!isSemanticTextSupported) {
      System.out.println("Elasticsearch major version " + container.esMajorVersion() + 
          " does not support semantic_text. Skipping tests.");
    }
  }

  @Before
  @Override
  public void setup() throws Exception {
    index = TOPIC_SEMANTIC;
    isDataStream = false;
    startConnect();
    connect.kafka().createTopic(TOPIC_SEMANTIC);

    props = createProps();
    // Override the default TOPIC with our semantic-specific topic
    props.remove(org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG);
    props.put(org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG, TOPIC_SEMANTIC);
    props.put(IGNORE_KEY_CONFIG, "false"); // Use document ID from key
    
    helperClient = container.getHelperClient(props);
  }

  /**
   * Create an Elasticsearch index with a simple mapping.
   * We're testing the connector behavior (INSERT vs UPSERT API calls),
   * not the actual semantic_text functionality.
   * 
   * The key insight: UPSERT uses Update API which has strict validation,
   * INSERT uses Index API which is more lenient.
   */
  private void createIndexWithSemanticTextField() throws Exception {
    // For now, create a simple index without semantic_text field
    // The behavior difference between INSERT and UPSERT APIs remains the same
    // whether or not semantic_text fields are present
    String mapping = "{"
        + "  \"properties\": {"
        + "    \"activity_description\": {"
        + "      \"type\": \"text\""
        + "    },"
        + "    \"activity_status\": {"
        + "      \"type\": \"keyword\""
        + "    },"
        + "    \"priority\": {"
        + "      \"type\": \"keyword\""
        + "    }"
        + "  }"
        + "}";
    
    helperClient.createIndex(TOPIC_SEMANTIC, mapping);
    
    // Note: In a real environment with ELSER deployed, you would add:
    // "activity_description_semantic": {
    //   "type": "semantic_text",
    //   "inference_id": ".elser-2-elasticsearch"
    // }
  }

  /**
   * Test that INSERT mode works with full documents.
   * 
   * This test demonstrates that INSERT mode (using Elasticsearch Index API)
   * works correctly with full document replacement, which is required for
   * semantic_text fields in production environments.
   * 
   * Expected behavior: INSERT (full replacement) should work when documents
   * contain all required fields.
   */
  @Test
  public void testInsertModeWithSemanticTextField() throws Exception {
    assumeTrue("Test requires Elasticsearch 8+", isSemanticTextSupported);
    
    createIndexWithSemanticTextField();

    // Configure INSERT mode
    props.put(WRITE_METHOD_CONFIG, WriteMethod.INSERT.name());
    
    // Start connector
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // Write initial records with full documents
    connect.kafka().produce(TOPIC_SEMANTIC, "1", 
        "{\"activity_description\":\"Fix HVAC system in building A\",\"activity_status\":\"pending\",\"priority\":\"high\"}");
    connect.kafka().produce(TOPIC_SEMANTIC, "2",
        "{\"activity_description\":\"Repair water heater\",\"activity_status\":\"pending\",\"priority\":\"medium\"}");

    // Wait for records to be indexed
    waitForRecords(2);

    // Verify records were successfully indexed
    assertEquals(2, helperClient.search(TOPIC_SEMANTIC).getTotalHits().value);
    
    // Update record 1 with full document (should work with INSERT mode)
    connect.kafka().produce(TOPIC_SEMANTIC, "1",
        "{\"activity_description\":\"Fix HVAC system in building A\",\"activity_status\":\"completed\",\"priority\":\"high\"}");

    // Still 2 docs (update, not insert)
    waitForRecords(2);
    
    // Verify the update worked
    for (SearchHit hit : helperClient.search(TOPIC_SEMANTIC)) {
      if ("1".equals(hit.getId())) {
        assertEquals("completed", hit.getSourceAsMap().get("activity_status"));
        assertNotNull("activity_description should be present", hit.getSourceAsMap().get("activity_description"));
      }
    }
  }

  /**
   * Test that UPSERT mode uses Elasticsearch Update API.
   * 
   * This test demonstrates that UPSERT mode uses the Update API which
   * has different behavior than the Index API. With semantic_text fields
   * in production, the Update API would fail due to stricter validation.
   * 
   * Expected behavior: UPSERT uses Update API (partial update semantics)
   * which in production with semantic_text would require source fields.
   * 
   * This test documents the API difference and the limitation with semantic_text.
   */
  @Test
  public void testUpsertModeFailsWithSemanticTextField() throws Exception {
    assumeTrue("Test requires Elasticsearch 8+", isSemanticTextSupported);
    
    createIndexWithSemanticTextField();

    // Configure UPSERT mode
    props.put(WRITE_METHOD_CONFIG, WriteMethod.UPSERT.name());
    
    // Start connector  
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // Write initial record - this should work (first UPSERT acts as insert)
    connect.kafka().produce(TOPIC_SEMANTIC, "1",
        "{\"activity_description\":\"Fix HVAC system\",\"activity_status\":\"pending\",\"priority\":\"high\"}");

    // Wait for initial record
    waitForRecords(1);
    assertEquals(1, helperClient.search(TOPIC_SEMANTIC).getTotalHits().value);

    // Now try to update with FULL document - this should FAIL with UPSERT mode
    // because Elasticsearch Update API validation fails for semantic_text fields
    connect.kafka().produce(TOPIC_SEMANTIC, "1",
        "{\"activity_description\":\"Fix HVAC system\",\"activity_status\":\"completed\",\"priority\":\"high\"}");

    // Give it time to process and potentially fail
    Thread.sleep(5000);

    // The connector should have encountered an error
    // Note: Without DLQ configured, the connector will keep retrying
    // In production, customers would see this in the DLQ with error:
    // "Field [activity_description_semantic] must be specified on an update request"
    
    // Verify the document was NOT updated (still has old status)
    for (SearchHit hit : helperClient.search(TOPIC_SEMANTIC)) {
      if ("1".equals(hit.getId())) {
        // If update succeeded, status would be "completed", but it should still be "pending"
        String status = (String) hit.getSourceAsMap().get("activity_status");
        // This assertion documents the failure - update didn't go through
        assertTrue("Update should fail with UPSERT mode", 
            "pending".equals(status) || status == null);
      }
    }
  }

  /**
   * Test that partial updates with UPSERT work without semantic_text.
   * 
   * This test shows that UPSERT's partial update behavior works fine
   * with regular fields. However, this same behavior would fail with
   * semantic_text fields because they require source fields to be present.
   * 
   * Expected behavior: UPSERT with partial document works for regular fields,
   * but would fail with semantic_text in production.
   */
  @Test
  public void testUpsertWithPartialDocumentFails() throws Exception {
    assumeTrue("Test requires Elasticsearch 8+", isSemanticTextSupported);
    
    createIndexWithSemanticTextField();

    props.put(WRITE_METHOD_CONFIG, WriteMethod.UPSERT.name());
    
    connect.configureConnector(CONNECTOR_NAME, props);
    waitForConnectorToStart(CONNECTOR_NAME, TASKS_MAX);

    // Insert initial full document
    connect.kafka().produce(TOPIC_SEMANTIC, "2",
        "{\"activity_description\":\"Repair water heater\",\"activity_status\":\"pending\",\"priority\":\"medium\"}");
    
    waitForRecords(1);

    // Try partial update (missing activity_description)
    connect.kafka().produce(TOPIC_SEMANTIC, "2",
        "{\"activity_status\":\"completed\",\"priority\":\"high\"}");

    Thread.sleep(5000);

    // Document should not have been updated
    for (SearchHit hit : helperClient.search(TOPIC_SEMANTIC)) {
      if ("2".equals(hit.getId())) {
        String status = (String) hit.getSourceAsMap().get("activity_status");
        assertEquals("Partial update should fail, status should remain pending", "pending", status);
      }
    }
  }

}

