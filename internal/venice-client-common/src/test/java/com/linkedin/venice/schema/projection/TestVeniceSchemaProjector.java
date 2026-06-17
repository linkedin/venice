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
  public void testRmdPerFieldTimestampThrows() {
    // Per-field timestamps only arise with write compute (partial updates), where the superset schema is used and no
    // projection runs. Encountering one in the projection path indicates misuse and must be rejected.
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

    Assert.expectThrows(VeniceException.class, () -> projector.projectRmd(srcRmd, writerRmd));
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
