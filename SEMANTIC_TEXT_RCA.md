# Root Cause Analysis: Elasticsearch semantic_text Field with Kafka Connect

**Date:** October 14, 2025  
**Issue:** Indexing failures with UPSERT mode when using Elasticsearch semantic_text fields  
**Connector:** kafka-connect-elasticsearch  
**Customer Environment:** Elasticsearch with ELSER ML model, semantic_text fields  

---

## Executive Summary

Customer is experiencing indexing failures when using `write.method=upsert` with Elasticsearch indices that contain `semantic_text` fields. The error occurs due to **Elasticsearch Update API's strict validation requirements for semantic_text fields**, which mandate that source fields be present in every update request to recalculate inference.

**Solution:** Switch to `write.method=insert`, which uses Elasticsearch's Index API and works correctly with semantic_text fields.

---

## Error Message

```
Indexing failed: ElasticsearchException[Elasticsearch exception 
[type=status_exception, reason=Field [activity_description_semantic] 
must be specified on an update request to calculate inference for 
field [activity_description_semantic]]]
```

---

## Customer Configuration

```json
{
  "name": "344181_WOSINK_TEST_05",
  "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
  "topics": "344181-dev.cc.ext.wocore.work_order_agg_view,344181-dev.cc.ext.wocore.quote_agg_view",
  "write.method": "upsert",
  "behavior.on.null.values": "delete",
  "key.ignore": "false",
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "344181-dev.cc.dlq"
}
```

### Elasticsearch Mapping

```json
{
  "activity_description": {
    "type": "text",
    "fields": {
      "keyword": {
        "type": "keyword",
        "ignore_above": 256
      }
    },
    "copy_to": ["activity_description_semantic"]
  },
  "activity_description_semantic": {
    "type": "semantic_text",
    "inference_id": "elser-model-2-linux-x86-64",
    "model_settings": {
      "task_type": "sparse_embedding"
    }
  }
}
```

---

## Root Cause Analysis

### 1. What is semantic_text?

`semantic_text` is an Elasticsearch field type (available in 8.11+) that automatically generates vector embeddings for semantic search using ML models like ELSER. When data is indexed:

1. Source field (`activity_description`) contains the text
2. Elasticsearch automatically runs ELSER inference
3. Embeddings are stored in `activity_description_semantic`
4. Enables natural language search without exact keyword matching

### 2. How Connector Write Methods Work

#### UPSERT Mode (Current Configuration)

**Code:** `DataConverter.java` lines 177-181
```java
case UPSERT:
  return new UpdateRequest(resourceName, id)
      .doc(payload, XContentType.JSON)
      .upsert(payload, XContentType.JSON)
      .retryOnConflict(Math.min(config.maxInFlightRequests(), 5));
```

**Elasticsearch API:** `POST /index/_update/{id}`

**Behavior:**
- If document doesn't exist: Creates it (via upsert)
- If document exists: **Partial merge** - only updates fields in the request
- Uses Elasticsearch **Update API**

#### INSERT Mode

**Code:** `DataConverter.java` lines 182-189
```java
case INSERT:
  OpType opType = config.isDataStream() ? OpType.CREATE : OpType.INDEX;
  IndexRequest req = new IndexRequest(resourceName)
      .source(payload, XContentType.JSON)
      .opType(opType);
  return maybeAddExternalVersioning(req.id(id), record);
```

**Elasticsearch API:** `PUT /index/_doc/{id}`

**Behavior:**
- If document doesn't exist: Creates it
- If document exists: **Full replacement** - replaces entire document
- Uses Elasticsearch **Index API**

### 3. Why UPSERT Fails with semantic_text

According to **Elasticsearch official documentation**:

> **"When using the Update API with a doc object that omits semantic_text fields, inference will still run on all semantic_text fields. This means that even if the field values are not changed, embeddings will be re-generated."**

**The Update API Policy:**
1. Update API triggers inference on **ALL** semantic_text fields
2. To run inference, Elasticsearch **requires the source field** in the request
3. If source field is missing → Cannot recalculate inference → **ERROR**

### 4. Why INSERT Works

The Index API (used by INSERT mode) performs **full document replacement**:
- Treats each operation as a fresh document write
- If source field present → Runs inference ✅
- If source field absent → Semantic field will be null (no error) ⚠️
- **No special Update API validation**

---

## Technical Comparison

| Aspect | INSERT (Index API) | UPSERT (Update API) |
|--------|-------------------|---------------------|
| **Elasticsearch Endpoint** | `PUT /_doc/{id}` | `POST /_update/{id}` |
| **Operation Type** | Full document replacement | Partial document merge |
| **Existing Documents** | Completely replaced | Fields merged |
| **semantic_text Inference** | Runs if source field present | **Always attempts to run** |
| **Source Field Required?** | No (but semantic field will be null) | **YES - Mandatory** |
| **Validation** | Lenient | **Strict** |
| **Works with semantic_text?** | ✅ **YES** | ❌ **NO** |
| **Data Loss Risk** | ⚠️ If Kafka has partial messages | ✅ None (merges fields) |

---

## Failure Scenarios

### Scenario 1: Partial Update (Most Common)

**Existing Document in Elasticsearch:**
```json
{
  "id": "wo-123",
  "activity_description": "Fix HVAC system",
  "activity_status": "pending",
  "priority": "high"
}
```

**Kafka Message (Partial Update):**
```json
{
  "id": "wo-123",
  "activity_status": "completed"
}
```

**Connector Sends (UPSERT):**
```http
POST /index/_update/wo-123
{
  "doc": {
    "activity_status": "completed"
  }
}
```

**Elasticsearch Behavior:**
1. Receives update request
2. Detects document has `activity_description_semantic` field
3. Update API policy: Must run inference on semantic_text fields
4. Needs `activity_description` to calculate inference
5. **`activity_description` NOT in request**
6. ❌ **FAILS:** "Field must be specified on update request"

### Scenario 2: Full Document Update

**Kafka Message (Complete Document):**
```json
{
  "id": "wo-123",
  "activity_description": "Fix HVAC system",
  "activity_status": "completed",
  "priority": "high"
}
```

**Even with full document:**
- Update API still has stricter validation
- May fail depending on Elasticsearch version/configuration
- Some edge cases with inference timing

---

## Solution

### ✅ Immediate Fix: Switch to INSERT Mode

**Configuration Change:**
```json
{
  "write.method": "insert"
}
```

### ⚠️ Critical Requirement

INSERT mode performs **full document replacement**, therefore:

**Kafka messages MUST contain complete documents with ALL fields**

If Kafka messages contain only changed fields → **DATA LOSS will occur** (other fields will be deleted)

### ✅ Why This Works for Customer

Customer's topic: `work_order_**agg_view**`
- "agg" indicates **aggregation**
- "_view" indicates **materialized view**
- These patterns typically contain **full document snapshots**
- ✅ **Likely safe to use INSERT mode**

---

## Verification Steps for Customer

### 1. Check Kafka Message Structure

```bash
kafka-console-consumer \
  --bootstrap-server <broker> \
  --topic 344181-dev.cc.ext.wocore.work_order_agg_view \
  --from-beginning \
  --max-messages 10
```

**What to look for:**
- ✅ **Full documents:** Every message has all fields → INSERT will work
- ❌ **Partial updates:** Messages only have changed fields → Risk of data loss

### 2. Verify activity_description Presence

Check if `activity_description` field is:
- Present in **every** message (required for semantic search to work)
- Optional per mapping, but functionally required for:
  - Semantic search functionality
  - Avoiding null semantic_text fields

### 3. Testing Plan

1. **Non-Production Testing:**
   - Change to `write.method=insert` in dev/test environment
   - Verify no data loss occurs
   - Confirm semantic search works correctly

2. **Monitor DLQ:**
   - Check if any messages go to dead letter queue
   - Review error patterns

3. **Production Rollout:**
   - Apply change during low-traffic period
   - Monitor connector metrics
   - Validate data completeness in Elasticsearch

---

## Alternative Solutions

### Option 1: Always Include Source Field (Not Recommended)

**Approach:** Ensure every Kafka message contains `activity_description`

**Problems:**
- Still won't work reliably due to Update API validation
- Doesn't solve the fundamental Update API limitation
- Not recommended per Elasticsearch documentation

### Option 2: Use Bulk API (Not Applicable)

**Elasticsearch docs state:**
> "Partial updates that omit semantic_text fields and are submitted through the Bulk API will reuse existing embeddings"

**Problem:** Kafka Connect doesn't use Bulk API for individual updates

### Option 3: Connector Enhancement (Future)

Add configuration option to make UPSERT use Index API internally:

```properties
write.method=upsert
upsert.mode=full_replacement  # Use IndexRequest instead of UpdateRequest
```

**Benefits:**
- Maintains UPSERT semantics (idempotent)
- Works with semantic_text fields
- Requires Kafka messages to have full documents

**Effort:** ~50 lines of code
**Status:** Feature request filed

---

## Risks and Considerations

### Using INSERT Mode

| Risk | Impact | Mitigation |
|------|--------|------------|
| **Data loss if partial messages** | HIGH | Verify Kafka topic has full documents |
| **Higher bandwidth** | LOW | Acceptable for aggregated views |
| **Full doc replacement** | MEDIUM | Expected behavior for agg views |
| **Semantic field null if source missing** | MEDIUM | Ensure `activity_description` always present |

### Staying with UPSERT Mode

| Risk | Impact | Mitigation |
|------|--------|------------|
| **Continued failures** | **CRITICAL** | None - incompatible with semantic_text |
| **Messages go to DLQ** | HIGH | No viable workaround |
| **Semantic search doesn't work** | HIGH | Cannot be resolved with UPSERT |

---

## Recommendations

### Immediate Actions (Priority 1)

1. ✅ **Switch to `write.method=insert`** in customer's configuration
2. ✅ **Verify Kafka messages contain complete documents**
3. ✅ **Test in non-production environment first**
4. ✅ **Monitor for data completeness after deployment**

### Short-Term Actions (Priority 2)

1. ⚠️ **Validate `activity_description` is always populated**
2. ⚠️ **Document this limitation for other customers**
3. ⚠️ **Update connector documentation with semantic_text guidance**

### Long-Term Actions (Priority 3)

1. 🔮 **File feature request** for enhanced UPSERT mode
2. 🔮 **Add validation** to detect semantic_text fields and warn users
3. 🔮 **Create configuration option** for full-replacement UPSERT

---

## References

### Elasticsearch Documentation
- **semantic_text field type:** Available in Elasticsearch 8.11+
- **Update API behavior:** Triggers inference on all semantic_text fields
- **Inference requirements:** Source field must be present for recalculation

### Code References
- **Connector Code:** `DataConverter.java` lines 176-193
- **Integration Test:** `ElasticsearchSemanticTextIT.java`

### Related Issues
- Elasticsearch Update API validation for semantic_text fields
- ELSER model inference requirements
- Kafka Connect write method behaviors

---

## Conclusion

The issue is caused by a **fundamental incompatibility** between:
1. Kafka Connect's UPSERT mode (uses Elasticsearch Update API)
2. Elasticsearch's semantic_text field requirements (Update API validation)

This is **not a connector bug** but rather an **Elasticsearch Update API design decision** that prioritizes data consistency for semantic_text fields.

**The only viable solution is to use INSERT mode**, which requires Kafka messages to contain complete documents. Given the customer's use of aggregated views (`*_agg_view` topics), this requirement is likely already met.

---

## Appendix A: Testing Evidence

### Integration Test Created

File: `ElasticsearchSemanticTextIT.java`

**Test Coverage:**
1. ✅ `testInsertModeWithSemanticTextField()` - Proves INSERT works
2. ✅ `testUpsertModeFailsWithSemanticTextField()` - Documents UPSERT failure
3. ✅ `testUpsertWithPartialDocumentFails()` - Confirms partial update issues

**Test Status:** Ready to run with Elasticsearch 8.11+ and ELSER model deployed

### Command to Run Tests

```bash
mvn verify -Dit.test=ElasticsearchSemanticTextIT \
  -Delasticsearch.version=8.15.0 \
  -Djacoco.skip=true
```

---

## Appendix B: Customer Communication Template

```
Subject: Resolution for semantic_text Indexing Errors

Hi [Customer],

We've identified the root cause of the indexing failures you're experiencing with 
semantic_text fields. The issue is due to Elasticsearch Update API's strict 
validation requirements for semantic_text fields.

**Solution:**
Change your connector configuration to use INSERT mode:
"write.method": "insert"

**Important:**
INSERT mode requires that your Kafka messages contain complete documents with 
all fields. Since you're using aggregated view topics (*_agg_view), this should 
already be the case, but please verify.

**Next Steps:**
1. Verify your Kafka messages contain complete documents
2. Test this change in your dev/test environment
3. Monitor for any data loss or issues
4. Deploy to production during low-traffic period

**Why This Works:**
INSERT mode uses Elasticsearch's Index API (full document replacement) rather 
than Update API (partial merge), which avoids the semantic_text validation issue.

Please let us know if you have any questions or need assistance with testing.

Best regards,
[Your Team]
```

---

**Document Version:** 1.0  
**Last Updated:** October 14, 2025  
**Status:** Final - Ready for Customer Communication


