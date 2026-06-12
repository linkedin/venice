package com.linkedin.venice.schema.projection;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.v1.RmdSchemaGeneratorV1;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link VeniceSchemaProjector}.
 *
 * <p>The projector is a pure structural drop: the writer (target) schema is guaranteed to be a strict subset of
 * the input schema (every retained field is deeply, exactly equal; the input may only carry extra top-level fields).
 * So projection just builds a record with the writer schema's fields, copies each by name, and drops the input's
 * extra top-level fields.</p>
 */
public class TestVeniceSchemaProjector {
  private VeniceSchemaProjector projector;

  @BeforeClass
  public void setUp() {
    projector = new VeniceSchemaProjector();
  }

  private static Schema parse(String json) {
    return AvroCompatibilityHelper.parse(json);
  }

  // ----------------------------------------------------------------------------------------------------------
  // Value projection
  // ----------------------------------------------------------------------------------------------------------

  @Test
  public void testTombstoneReturnsNull() {
    Schema target = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"f1\", \"type\": \"string\"}\n" + "  ]\n" + "}");
    Assert.assertNull(projector.projectValue(null, target));
  }

  @Test
  public void testIdenticalSchemaCopiesAllFields() {
    Schema schema = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"f1\", \"type\": \"string\"},\n" + "    {\"name\": \"f2\", \"type\": \"int\"}\n"
            + "  ]\n" + "}");
    GenericRecord src = new GenericData.Record(schema);
    src.put("f1", "hello");
    src.put("f2", 42);

    GenericRecord out = projector.projectValue(src, schema);

    Assert.assertEquals(out.getSchema(), schema);
    Assert.assertEquals(out.get("f1").toString(), "hello");
    Assert.assertEquals(out.get("f2"), 42);
  }

  @Test
  public void testExtraTopLevelSourceFieldIsDropped() {
    // Input has a resurrected/deleted field f2 that the writer schema does not declare -> dropped.
    Schema input = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"f1\", \"type\": \"string\"},\n"
            + "    {\"name\": \"f2\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
            + "    {\"name\": \"f3\", \"type\": \"int\"}\n" + "  ]\n" + "}");
    Schema writer = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"f1\", \"type\": \"string\"},\n" + "    {\"name\": \"f3\", \"type\": \"int\"}\n"
            + "  ]\n" + "}");
    GenericRecord src = new GenericData.Record(input);
    src.put("f1", "keep");
    src.put("f2", "drop");
    src.put("f3", 7);

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertEquals(out.getSchema(), writer);
    Assert.assertEquals(out.get("f1").toString(), "keep");
    Assert.assertEquals(out.get("f3"), 7);
    Assert.assertNull(out.getSchema().getField("f2"), "extra source field must be dropped");
  }

  @Test
  public void testNullableFieldWithNullValuePreserved() {
    // Shared field is nullable in both input and writer (guaranteed by strict subset), value null -> kept null.
    Schema schema = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"f1\", \"type\": [\"null\", \"string\"], \"default\": null}\n" + "  ]\n" + "}");
    GenericRecord src = new GenericData.Record(schema);
    src.put("f1", null);

    GenericRecord out = projector.projectValue(src, schema);

    Assert.assertNull(out.get("f1"));
  }

  @Test
  public void testNestedRecordCopiedWholesale() {
    // The nested record is identical in input and writer (strict subset); only the extra top-level field differs.
    Schema input = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"extra\", \"type\": [\"null\", \"string\"], \"default\": null},\n" + "    {\n"
            + "      \"name\": \"address\",\n" + "      \"type\": {\n" + "        \"type\": \"record\",\n"
            + "        \"name\": \"Address\",\n" + "        \"fields\": [\n"
            + "          {\"name\": \"city\", \"type\": \"string\"},\n"
            + "          {\"name\": \"zip\", \"type\": \"string\"}\n" + "        ]\n" + "      }\n" + "    }\n"
            + "  ]\n" + "}");
    Schema writer = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"address\",\n" + "      \"type\": {\n" + "        \"type\": \"record\",\n"
            + "        \"name\": \"Address\",\n" + "        \"fields\": [\n"
            + "          {\"name\": \"city\", \"type\": \"string\"},\n"
            + "          {\"name\": \"zip\", \"type\": \"string\"}\n" + "        ]\n" + "      }\n" + "    }\n"
            + "  ]\n" + "}");
    GenericRecord addr = new GenericData.Record(input.getField("address").schema());
    addr.put("city", "NYC");
    addr.put("zip", "10001");
    GenericRecord src = new GenericData.Record(input);
    src.put("extra", "drop");
    src.put("address", addr);

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertNull(out.getSchema().getField("extra"));
    GenericRecord outAddr = (GenericRecord) out.get("address");
    Assert.assertEquals(outAddr.get("city").toString(), "NYC");
    Assert.assertEquals(outAddr.get("zip").toString(), "10001");
  }

  @Test
  public void testRetainedValuesAreDeepCopiedAndIndependent() {
    Schema input = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"nums\", \"type\": {\"type\": \"array\", \"items\": \"int\"}},\n"
            + "    {\"name\": \"extra\", \"type\": [\"null\", \"string\"], \"default\": null}\n" + "  ]\n" + "}");
    Schema writer = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"nums\", \"type\": {\"type\": \"array\", \"items\": \"int\"}}\n" + "  ]\n" + "}");
    List<Integer> srcList = new ArrayList<>(Arrays.asList(1, 2, 3));
    GenericRecord src = new GenericData.Record(input);
    src.put("nums", srcList);
    src.put("extra", "drop");

    GenericRecord out = projector.projectValue(src, writer);

    @SuppressWarnings("unchecked")
    List<Object> outList = (List<Object>) out.get("nums");
    Assert.assertEquals(outList.size(), 3);
    srcList.clear();
    Assert.assertEquals(outList.size(), 3, "mutating the source list must not affect the projected output");
  }

  @Test
  public void testWriterFieldAbsentInSourceSchemaThrowsInvariant() {
    // Writer declares f2 but the input schema has no f2 -> writer is not a subset of input -> invalid hint.
    // Preflight should reject this; the projector backstops it as a VeniceException.
    Schema input = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"f1\", \"type\": \"string\"}\n" + "  ]\n" + "}");
    Schema writer = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"f1\", \"type\": \"string\"},\n" + "    {\"name\": \"f2\", \"type\": \"string\"}\n"
            + "  ]\n" + "}");
    GenericRecord src = new GenericData.Record(input);
    src.put("f1", "v");

    Assert.expectThrows(VeniceException.class, () -> projector.projectValue(src, writer));
  }

  // ----------------------------------------------------------------------------------------------------------
  // RMD projection
  // ----------------------------------------------------------------------------------------------------------

  private static final RmdSchemaGeneratorV1 RMD_GEN = new RmdSchemaGeneratorV1();

  @Test
  public void testRmdWholeRecordLongTimestampCopied() {
    Schema valueSchema = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"f1\", \"type\": \"string\"}\n" + "  ]\n" + "}");
    Schema rmdSchema = RMD_GEN.generateMetadataSchema(valueSchema);
    GenericRecord srcRmd = new GenericData.Record(rmdSchema);
    srcRmd.put(TIMESTAMP_FIELD_NAME, 12345L);
    srcRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, new ArrayList<>(Arrays.asList(7L, 8L)));

    GenericRecord out = projector.projectRmd(srcRmd, rmdSchema);

    Assert.assertEquals(out.get(TIMESTAMP_FIELD_NAME), 12345L);
    @SuppressWarnings("unchecked")
    List<Long> rcv = (List<Long>) out.get(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME);
    Assert.assertEquals(rcv, Arrays.asList(7L, 8L));
  }

  @Test
  public void testRmdPerFieldDropsExtraSourceEntry() {
    // Input value has f1 + f2 (extra); writer value has only f1. The per-field RMD entry for f2 is dropped.
    Schema inputValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"f1\", \"type\": \"string\"},\n"
            + "    {\"name\": \"f2\", \"type\": [\"null\", \"string\"], \"default\": null}\n" + "  ]\n" + "}");
    Schema writerValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"f1\", \"type\": \"string\"}\n" + "  ]\n" + "}");
    Schema inputRmd = RMD_GEN.generateMetadataSchema(inputValue);
    Schema writerRmd = RMD_GEN.generateMetadataSchema(writerValue);

    Schema perFieldSchema = inputRmd.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    GenericRecord perField = new GenericData.Record(perFieldSchema);
    perField.put("f1", 100L);
    perField.put("f2", 200L);
    GenericRecord srcRmd = new GenericData.Record(inputRmd);
    srcRmd.put(TIMESTAMP_FIELD_NAME, perField);
    srcRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, new ArrayList<>());

    GenericRecord out = projector.projectRmd(srcRmd, writerRmd);

    GenericRecord outPerField = (GenericRecord) out.get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(outPerField.get("f1"), 100L);
    Assert.assertNull(outPerField.getSchema().getField("f2"));
  }

  @Test
  public void testRmdCollectionMetadataReWrappedUnderTargetName() {
    // Input value has an extra collection field so the CollectionMetadata counter (and record name) differs from
    // the writer's. The surviving entry must be re-wrapped under the writer's record name.
    Schema inputValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"pad\", \"type\": {\"type\": \"array\", \"items\": \"long\"}},\n"
            + "    {\"name\": \"tags\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}\n" + "  ]\n" + "}");
    Schema writerValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"tags\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}\n"
            + "  ]\n" + "}");
    Schema inputRmd = RMD_GEN.generateMetadataSchema(inputValue);
    Schema writerRmd = RMD_GEN.generateMetadataSchema(writerValue);

    Schema srcPerFieldSchema = inputRmd.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    Schema srcCollSchema = srcPerFieldSchema.getField("tags").schema();
    Schema writerPerFieldSchema = writerRmd.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    Schema writerCollSchema = writerPerFieldSchema.getField("tags").schema();
    Assert.assertNotEquals(
        srcCollSchema.getName(),
        writerCollSchema.getName(),
        "precondition: source and target CollectionMetadata record names must differ");

    GenericRecord srcColl = new GenericData.Record(srcCollSchema);
    srcColl.put("topLevelFieldTimestamp", 55L);
    srcColl.put("topLevelColoID", -1);
    srcColl.put("putOnlyPartLength", 2);
    srcColl.put("activeElementsTimestamps", new ArrayList<>(Arrays.asList(1L, 2L)));
    srcColl.put("deletedElementsIdentities", new ArrayList<>());
    srcColl.put("deletedElementsTimestamps", new ArrayList<>());

    GenericRecord perField = new GenericData.Record(srcPerFieldSchema);
    perField.put("pad", 0L);
    perField.put("tags", srcColl);
    GenericRecord srcRmd = new GenericData.Record(inputRmd);
    srcRmd.put(TIMESTAMP_FIELD_NAME, perField);
    srcRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, new ArrayList<>());

    GenericRecord out = projector.projectRmd(srcRmd, writerRmd);

    GenericRecord outPerField = (GenericRecord) out.get(TIMESTAMP_FIELD_NAME);
    GenericRecord outColl = (GenericRecord) outPerField.get("tags");
    Assert.assertEquals(outColl.getSchema().getName(), writerCollSchema.getName());
    Assert.assertEquals(outColl.get("topLevelFieldTimestamp"), 55L);
    Assert.assertEquals(outColl.get("putOnlyPartLength"), 2);
  }

  @Test
  public void testRmdPerFieldMissingSourceEntryThrowsInvariant() {
    // Writer value declares f1 + f2 but the source per-field RMD only has f1 -> invariant violation.
    Schema writerValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"f1\", \"type\": \"string\"},\n"
            + "    {\"name\": \"f2\", \"type\": \"string\"}\n" + "  ]\n" + "}");
    Schema sourceValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"f1\", \"type\": \"string\"}\n" + "  ]\n" + "}");
    Schema writerRmd = RMD_GEN.generateMetadataSchema(writerValue);
    Schema sourceRmd = RMD_GEN.generateMetadataSchema(sourceValue);

    Schema srcPerFieldSchema = sourceRmd.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    GenericRecord perField = new GenericData.Record(srcPerFieldSchema);
    perField.put("f1", 1L);
    GenericRecord srcRmd = new GenericData.Record(sourceRmd);
    srcRmd.put(TIMESTAMP_FIELD_NAME, perField);
    srcRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, new ArrayList<>());

    Assert.expectThrows(VeniceException.class, () -> projector.projectRmd(srcRmd, writerRmd));
  }

  @Test
  public void testRmdPerFieldMultipleSurvivingFieldsCopiedByName() {
    // Input value has a, b, c; writer drops the MIDDLE field b. So surviving field c is at source position 2 but
    // target position 1. With distinct timestamps, this proves entries are copied BY NAME (not by position) and that
    // multiple surviving per-field timestamps are all preserved correctly.
    Schema inputValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"a\", \"type\": \"string\"},\n"
            + "    {\"name\": \"b\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
            + "    {\"name\": \"c\", \"type\": \"string\"}\n" + "  ]\n" + "}");
    Schema writerValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"a\", \"type\": \"string\"},\n"
            + "    {\"name\": \"c\", \"type\": \"string\"}\n" + "  ]\n" + "}");
    Schema inputRmd = RMD_GEN.generateMetadataSchema(inputValue);
    Schema writerRmd = RMD_GEN.generateMetadataSchema(writerValue);

    Schema srcPerFieldSchema = inputRmd.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    GenericRecord perField = new GenericData.Record(srcPerFieldSchema);
    perField.put("a", 100L);
    perField.put("b", 200L);
    perField.put("c", 300L);
    GenericRecord srcRmd = new GenericData.Record(inputRmd);
    srcRmd.put(TIMESTAMP_FIELD_NAME, perField);
    srcRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, new ArrayList<>());

    GenericRecord out = projector.projectRmd(srcRmd, writerRmd);

    GenericRecord outPerField = (GenericRecord) out.get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(outPerField.get("a"), 100L);
    Assert.assertEquals(outPerField.get("c"), 300L, "surviving field must be copied by name, not by source position");
    Assert.assertNull(outPerField.getSchema().getField("b"), "dropped field must not appear in the projected RMD");
  }

  @Test
  public void testProjectRmdNullReturnsNull() {
    // A record may carry no RMD (e.g. a batch record in a hybrid store) -> projection yields no RMD.
    Schema valueSchema = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"f1\", \"type\": \"string\"}\n" + "  ]\n" + "}");
    Schema rmdSchema = RMD_GEN.generateMetadataSchema(valueSchema);
    Assert.assertNull(projector.projectRmd(null, rmdSchema));
  }

  @Test
  public void testRmdMapCollectionMetadataReWrappedUnderTargetName() {
    // Same as the array re-home test, but for a MAP field (separate RMD generation path). The extra map field shifts
    // the CollectionMetadata counter so the surviving field's record name differs between source and writer.
    Schema inputValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"pad\", \"type\": {\"type\": \"map\", \"values\": \"long\"}},\n"
            + "    {\"name\": \"attrs\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}\n" + "  ]\n" + "}");
    Schema writerValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"attrs\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}\n"
            + "  ]\n" + "}");
    Schema inputRmd = RMD_GEN.generateMetadataSchema(inputValue);
    Schema writerRmd = RMD_GEN.generateMetadataSchema(writerValue);

    Schema srcPerFieldSchema = inputRmd.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    Schema srcCollSchema = srcPerFieldSchema.getField("attrs").schema();
    Schema writerPerFieldSchema = writerRmd.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    Schema writerCollSchema = writerPerFieldSchema.getField("attrs").schema();
    Assert.assertNotEquals(
        srcCollSchema.getName(),
        writerCollSchema.getName(),
        "precondition: source and target CollectionMetadata record names must differ");

    GenericRecord srcColl = new GenericData.Record(srcCollSchema);
    srcColl.put("topLevelFieldTimestamp", 77L);
    srcColl.put("topLevelColoID", -1);
    srcColl.put("putOnlyPartLength", 3);
    srcColl.put("activeElementsTimestamps", new ArrayList<>(Arrays.asList(70L, 71L)));
    srcColl.put("deletedElementsIdentities", new ArrayList<>());
    srcColl.put("deletedElementsTimestamps", new ArrayList<>());

    GenericRecord perField = new GenericData.Record(srcPerFieldSchema);
    perField.put("pad", 0L);
    perField.put("attrs", srcColl);
    GenericRecord srcRmd = new GenericData.Record(inputRmd);
    srcRmd.put(TIMESTAMP_FIELD_NAME, perField);
    srcRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, new ArrayList<>());

    GenericRecord out = projector.projectRmd(srcRmd, writerRmd);

    GenericRecord outPerField = (GenericRecord) out.get(TIMESTAMP_FIELD_NAME);
    GenericRecord outColl = (GenericRecord) outPerField.get("attrs");
    Assert.assertEquals(outColl.getSchema().getName(), writerCollSchema.getName());
    Assert.assertEquals(outColl.get("topLevelFieldTimestamp"), 77L);
    Assert.assertEquals(outColl.get("putOnlyPartLength"), 3);
  }

  @Test
  public void testRmdCollectionMetadataIsDeepCopiedAndIndependent() {
    // Mutating a nested list inside the source CollectionMetadata after projection must not affect the output.
    Schema value = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"tags\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}\n"
            + "  ]\n" + "}");
    Schema rmdSchema = RMD_GEN.generateMetadataSchema(value);
    Schema perFieldSchema = rmdSchema.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    Schema collSchema = perFieldSchema.getField("tags").schema();

    List<Long> activeTs = new ArrayList<>(Arrays.asList(10L, 20L));
    GenericRecord srcColl = new GenericData.Record(collSchema);
    srcColl.put("topLevelFieldTimestamp", 5L);
    srcColl.put("topLevelColoID", -1);
    srcColl.put("putOnlyPartLength", 0);
    srcColl.put("activeElementsTimestamps", activeTs);
    srcColl.put("deletedElementsIdentities", new ArrayList<>());
    srcColl.put("deletedElementsTimestamps", new ArrayList<>());

    GenericRecord perField = new GenericData.Record(perFieldSchema);
    perField.put("tags", srcColl);
    GenericRecord srcRmd = new GenericData.Record(rmdSchema);
    srcRmd.put(TIMESTAMP_FIELD_NAME, perField);
    srcRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, new ArrayList<>());

    GenericRecord out = projector.projectRmd(srcRmd, rmdSchema);

    GenericRecord outColl = (GenericRecord) ((GenericRecord) out.get(TIMESTAMP_FIELD_NAME)).get("tags");
    @SuppressWarnings("unchecked")
    List<Long> outActiveTs = (List<Long>) outColl.get("activeElementsTimestamps");
    Assert.assertEquals(outActiveTs.size(), 2);
    activeTs.clear();
    Assert.assertEquals(outActiveTs.size(), 2, "mutating the source collection metadata must not affect the output");
  }

  @Test
  public void testRmdWholeRecordLongTimestampWithDivergentSchemas() {
    // Source RMD carries the value-level long branch while source/writer value schemas differ. The long must be
    // copied as-is and the result must be under the writer RMD schema.
    Schema inputValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"f1\", \"type\": \"string\"},\n"
            + "    {\"name\": \"f2\", \"type\": [\"null\", \"string\"], \"default\": null}\n" + "  ]\n" + "}");
    Schema writerValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"f1\", \"type\": \"string\"}\n" + "  ]\n" + "}");
    Schema inputRmd = RMD_GEN.generateMetadataSchema(inputValue);
    Schema writerRmd = RMD_GEN.generateMetadataSchema(writerValue);

    GenericRecord srcRmd = new GenericData.Record(inputRmd);
    srcRmd.put(TIMESTAMP_FIELD_NAME, 999L);
    srcRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, new ArrayList<>(Arrays.asList(3L)));

    GenericRecord out = projector.projectRmd(srcRmd, writerRmd);

    Assert.assertEquals(out.getSchema(), writerRmd);
    Assert.assertEquals(out.get(TIMESTAMP_FIELD_NAME), 999L);
    @SuppressWarnings("unchecked")
    List<Long> rcv = (List<Long>) out.get(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME);
    Assert.assertEquals(rcv, Arrays.asList(3L));
  }

  @Test
  public void testReplicationCheckpointVectorIsDeepCopied() {
    Schema valueSchema = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"f1\", \"type\": \"string\"}\n" + "  ]\n" + "}");
    Schema rmdSchema = RMD_GEN.generateMetadataSchema(valueSchema);
    List<Long> rcv = new ArrayList<>(Arrays.asList(1L, 2L, 3L));
    GenericRecord srcRmd = new GenericData.Record(rmdSchema);
    srcRmd.put(TIMESTAMP_FIELD_NAME, 1L);
    srcRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, rcv);

    GenericRecord out = projector.projectRmd(srcRmd, rmdSchema);

    @SuppressWarnings("unchecked")
    List<Long> outRcv = (List<Long>) out.get(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME);
    Assert.assertEquals(outRcv.size(), 3);
    rcv.clear();
    Assert.assertEquals(outRcv.size(), 3, "mutating the source RCV must not affect the projected output");
  }
}
