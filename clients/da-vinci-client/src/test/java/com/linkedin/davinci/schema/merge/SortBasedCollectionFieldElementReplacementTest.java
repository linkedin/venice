package com.linkedin.davinci.schema.merge;

import com.linkedin.davinci.schema.SchemaUtils;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.rmd.RmdConstants;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Verifies the collection-merge (SET_UNION) element-replacement behavior gated by
 * {@code server.aa.collection.field.element.replacement.enabled}.
 *
 * The list elements are records with a field marked {@code order: ignore}. Two elements that differ only in that ignored
 * field are equal under Avro comparison (and {@link Object#equals}/{@link Object#hashCode}), so a union-add of such an
 * element collides with the stored one. The legacy behavior keeps the stored element and only advances its timestamp,
 * dropping the incoming element's content in the ignored field; the new behavior (flag on) applies the winning element.
 */
public class SortBasedCollectionFieldElementReplacementTest {
  private static final String LIST_FIELD = "RecordListField";
  private static final String ID_FIELD = "id";
  private static final String IGNORED_FIELD = "metadata";

  private static final Schema VALUE_SCHEMA = AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(
      "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"TestValueWithIgnoredField\",\n"
          + "  \"namespace\": \"com.linkedin.davinci.schema.merge\",\n" + "  \"fields\": [\n" + "    {\n"
          + "      \"name\": \"" + LIST_FIELD + "\",\n" + "      \"type\": {\n" + "        \"type\": \"array\",\n"
          + "        \"items\": {\n" + "          \"type\": \"record\",\n"
          + "          \"name\": \"ElementWithIgnoredField\",\n" + "          \"fields\": [\n"
          + "            {\"name\": \"" + ID_FIELD + "\", \"type\": \"string\"},\n" + "            {\"name\": \""
          + IGNORED_FIELD + "\", \"type\": \"string\", \"order\": \"ignore\", \"default\": \"\"}\n" + "          ]\n"
          + "        }\n" + "      },\n" + "      \"default\": []\n" + "    }\n" + "  ]\n" + "}");

  private static final Schema ELEMENT_SCHEMA = VALUE_SCHEMA.getField(LIST_FIELD).schema().getElementType();

  private static final Schema RMD_SCHEMA =
      SchemaUtils.annotateRmdSchema(RmdSchemaGenerator.generateMetadataSchema(VALUE_SCHEMA));
  private static final Schema RMD_TIMESTAMP_SCHEMA =
      RMD_SCHEMA.getField(RmdConstants.TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);

  @Test
  public void testNewerTimestampKeepsStoredElementWhenDisabled() {
    SortBasedCollectionFieldOpHandler handler =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);
    GenericRecord value = valueWithElement(element("a", "old"));
    CollectionRmdTimestamp<Object> rmd = collectionMetadata(1L, Collections.singletonList(5L));

    UpdateResultStatus status = handler.handleModifyList(
        10L,
        rmd,
        value,
        value.getSchema().getField(LIST_FIELD),
        Collections.singletonList(element("a", "new")),
        Collections.emptyList());

    Assert.assertEquals(status, UpdateResultStatus.PARTIALLY_UPDATED);
    List<GenericRecord> result = currentList(value);
    Assert.assertEquals(result.size(), 1);
    // Legacy behavior: the ignored-field content of the incoming element is dropped, only the timestamp is advanced.
    Assert.assertEquals(result.get(0).get(IGNORED_FIELD).toString(), "old");
    Assert.assertEquals(rmd.getActiveElementTimestamps().get(0).longValue(), 10L);
  }

  @Test
  public void testNewerTimestampReplacesStoredElementWhenEnabled() {
    SortBasedCollectionFieldOpHandler handler =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE, true);
    GenericRecord value = valueWithElement(element("a", "old"));
    CollectionRmdTimestamp<Object> rmd = collectionMetadata(1L, Collections.singletonList(5L));

    UpdateResultStatus status = handler.handleModifyList(
        10L,
        rmd,
        value,
        value.getSchema().getField(LIST_FIELD),
        Collections.singletonList(element("a", "new")),
        Collections.emptyList());

    Assert.assertEquals(status, UpdateResultStatus.PARTIALLY_UPDATED);
    List<GenericRecord> result = currentList(value);
    Assert.assertEquals(result.size(), 1);
    // New behavior: the incoming element (newer timestamp) replaces the stored one, propagating the ignored field.
    Assert.assertEquals(result.get(0).get(IGNORED_FIELD).toString(), "new");
    Assert.assertEquals(rmd.getActiveElementTimestamps().get(0).longValue(), 10L);
  }

  @Test
  public void testEqualTimestampReplacesWhenIncomingWinsTieBreak() {
    SortBasedCollectionFieldOpHandler handler =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE, true);
    GenericRecord value = valueWithElement(element("a", "aaa"));
    CollectionRmdTimestamp<Object> rmd = collectionMetadata(1L, Collections.singletonList(5L));

    // Same timestamp as the stored element; "zzz" > "aaa" by full-content comparison, so the incoming element wins.
    handler.handleModifyList(
        5L,
        rmd,
        value,
        value.getSchema().getField(LIST_FIELD),
        Collections.singletonList(element("a", "zzz")),
        Collections.emptyList());

    List<GenericRecord> result = currentList(value);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).get(IGNORED_FIELD).toString(), "zzz");
    Assert.assertEquals(rmd.getActiveElementTimestamps().get(0).longValue(), 5L);
  }

  @Test
  public void testEqualTimestampKeepsExistingWhenIncomingLosesTieBreak() {
    SortBasedCollectionFieldOpHandler handler =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE, true);
    GenericRecord value = valueWithElement(element("a", "zzz"));
    CollectionRmdTimestamp<Object> rmd = collectionMetadata(1L, Collections.singletonList(5L));

    // Same timestamp; "aaa" < "zzz" by full-content comparison, so the stored element is kept (deterministic
    // tie-break).
    handler.handleModifyList(
        5L,
        rmd,
        value,
        value.getSchema().getField(LIST_FIELD),
        Collections.singletonList(element("a", "aaa")),
        Collections.emptyList());

    List<GenericRecord> result = currentList(value);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).get(IGNORED_FIELD).toString(), "zzz");
    Assert.assertEquals(rmd.getActiveElementTimestamps().get(0).longValue(), 5L);
  }

  @Test
  public void testEqualTimestampNoOpWhenContentIsIdentical() {
    SortBasedCollectionFieldOpHandler handler =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE, true);
    GenericRecord value = valueWithElement(element("a", "same"));
    CollectionRmdTimestamp<Object> rmd = collectionMetadata(1L, Collections.singletonList(5L));

    // Same timestamp and identical content (full-content comparison == 0): the deterministic tie-break replaces
    // nothing, so the element is untouched and no update is reported (no Active/Active churn on a redundant re-add).
    UpdateResultStatus status = handler.handleModifyList(
        5L,
        rmd,
        value,
        value.getSchema().getField(LIST_FIELD),
        Collections.singletonList(element("a", "same")),
        Collections.emptyList());

    Assert.assertEquals(status, UpdateResultStatus.NOT_UPDATED_AT_ALL);
    List<GenericRecord> result = currentList(value);
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).get(IGNORED_FIELD).toString(), "same");
    Assert.assertEquals(rmd.getActiveElementTimestamps().get(0).longValue(), 5L);
  }

  private GenericRecord element(String id, String ignoredFieldValue) {
    GenericRecord record = new GenericData.Record(ELEMENT_SCHEMA);
    record.put(ID_FIELD, id);
    record.put(IGNORED_FIELD, ignoredFieldValue);
    return record;
  }

  private GenericRecord valueWithElement(GenericRecord element) {
    GenericRecord value = new GenericData.Record(VALUE_SCHEMA);
    value.put(LIST_FIELD, new ArrayList<>(Collections.singletonList(element)));
    return value;
  }

  @SuppressWarnings("unchecked")
  private List<GenericRecord> currentList(GenericRecord value) {
    return (List<GenericRecord>) value.get(LIST_FIELD);
  }

  private CollectionRmdTimestamp<Object> collectionMetadata(
      long topLevelTimestamp,
      List<Long> activeElementTimestamps) {
    CollectionTimestampBuilder builder = new CollectionTimestampBuilder(ELEMENT_SCHEMA);
    builder.setTopLevelTimestamps(topLevelTimestamp);
    builder.setTopLevelColoID(0);
    builder.setPutOnlyPartLength(0);
    builder.setActiveElementsTimestamps(activeElementTimestamps);
    builder.setDeletedElementTimestamps(Collections.<Long>emptyList());
    builder.setDeletedElements(ELEMENT_SCHEMA, Collections.emptyList());
    builder.setCollectionTimestampSchema(RMD_TIMESTAMP_SCHEMA.getField(LIST_FIELD).schema());
    return new CollectionRmdTimestamp<>(builder.build());
  }
}
