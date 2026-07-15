package com.linkedin.venice.schema.projection;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.v1.RmdSchemaGeneratorV1;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.utils.AvroSchemaUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link VeniceSchemaProjector}.
 *
 * <p>The projector is a structural drop that follows the writer (target) schema, which is guaranteed to be a
 * projection-subset of the input schema: every retained field matches the writer's, except that the input may wrap
 * fields as a nullable union ([null, X] vs writer X) and may carry extra fields -- both at any nesting level (records,
 * array elements, map values), since the OH superset accumulates fields recursively. Projection rebuilds each record
 * under the writer schema, unwrapping nullable inputs and dropping the input's extra fields at every level. Subtrees
 * that need no projection (input subschema already equals the writer subschema) are copied by reference.</p>
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
  public void testRetainedValuesAreSharedByReference() {
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

    Assert.assertSame(out.get("nums"), srcList);
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

  @Test
  public void testNullableInputFieldProjectedToNonNullableWriterField() {
    // OH schema evolution wrapped the field as [null, string]; the chosen target writer schema keeps it
    // non-nullable. Projection must drop the union wrapper and copy the (present) value across.
    Schema input = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"f1\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
            + "    {\"name\": \"f2\", \"type\": \"int\"},\n"
            + "    {\"name\": \"extra\", \"type\": [\"null\", \"string\"], \"default\": null}\n" + "  ]\n" + "}");
    Schema writer = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"f1\", \"type\": \"string\"},\n" + "    {\"name\": \"f2\", \"type\": \"int\"}\n"
            + "  ]\n" + "}");
    GenericRecord src = new GenericData.Record(input);
    src.put("f1", "hi");
    src.put("f2", 9);
    src.put("extra", "drop");

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertEquals(out.getSchema(), writer);
    Assert.assertEquals(out.get("f1").toString(), "hi");
    Assert.assertEquals(out.get("f2"), 9);
    Assert.assertNull(out.getSchema().getField("extra"), "extra source field must be dropped");
  }

  @Test
  public void testNullableInputRecordFieldProjectedToNonNullableWriterField() {
    // The top-level field's type evolved from Address to [null, Address]; the writer keeps it non-nullable. With a
    // present value, projection copies the record wholesale under the non-nullable writer field.
    Schema input = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"address\",\n" + "      \"type\": [\"null\", {\n" + "        \"type\": \"record\",\n"
            + "        \"name\": \"Address\",\n" + "        \"fields\": [\n"
            + "          {\"name\": \"city\", \"type\": \"string\"}\n" + "        ]\n" + "      }],\n"
            + "      \"default\": null\n" + "    }\n" + "  ]\n" + "}");
    Schema writer = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"address\",\n" + "      \"type\": {\n" + "        \"type\": \"record\",\n"
            + "        \"name\": \"Address\",\n" + "        \"fields\": [\n"
            + "          {\"name\": \"city\", \"type\": \"string\"}\n" + "        ]\n" + "      }\n" + "    }\n"
            + "  ]\n" + "}");
    Schema inputAddressSchema = input.getField("address").schema().getTypes().get(1);
    GenericRecord addr = new GenericData.Record(inputAddressSchema);
    addr.put("city", "NYC");
    GenericRecord src = new GenericData.Record(input);
    src.put("address", addr);

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertEquals(out.getSchema(), writer);
    GenericRecord outAddr = (GenericRecord) out.get("address");
    Assert.assertEquals(outAddr.get("city").toString(), "NYC");
  }

  @Test
  public void testNestedRecordExtraFieldDropped() {
    // OH retains deleted columns at every level, so a NESTED input record may carry an extra field absent from the
    // writer's nested record. Projection must rebuild the nested record under the writer schema and drop the extra.
    Schema input = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"nested\",\n" + "      \"type\": {\n" + "        \"type\": \"record\",\n"
            + "        \"name\": \"N\",\n" + "        \"fields\": [\n"
            + "          {\"name\": \"b\", \"type\": \"int\"},\n"
            + "          {\"name\": \"c\", \"type\": \"int\", \"default\": 5}\n" + "        ]\n" + "      }\n"
            + "    }\n" + "  ]\n" + "}");
    Schema writer = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"nested\",\n" + "      \"type\": {\n" + "        \"type\": \"record\",\n"
            + "        \"name\": \"N\",\n" + "        \"fields\": [\n"
            + "          {\"name\": \"b\", \"type\": \"int\"}\n" + "        ]\n" + "      }\n" + "    }\n" + "  ]\n"
            + "}");
    GenericRecord nested = new GenericData.Record(input.getField("nested").schema());
    nested.put("b", 1);
    nested.put("c", 2);
    GenericRecord src = new GenericData.Record(input);
    src.put("nested", nested);

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertEquals(out.getSchema(), writer);
    GenericRecord outNested = (GenericRecord) out.get("nested");
    Assert.assertEquals(outNested.get("b"), 1);
    Assert.assertNull(outNested.getSchema().getField("c"), "extra nested field must be dropped");
  }

  @Test
  public void testNestedNullableFieldUnwrapped() {
    // A field added to a nested record arrives as [null, X] in the input while the writer keeps it non-nullable. With
    // a present value, projection must unwrap the union and copy the value under the writer's non-nullable field.
    Schema input = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"nested\",\n" + "      \"type\": {\n" + "        \"type\": \"record\",\n"
            + "        \"name\": \"N\",\n" + "        \"fields\": [\n"
            + "          {\"name\": \"city\", \"type\": [\"null\", \"string\"], \"default\": null}\n" + "        ]\n"
            + "      }\n" + "    }\n" + "  ]\n" + "}");
    Schema writer = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"nested\",\n" + "      \"type\": {\n" + "        \"type\": \"record\",\n"
            + "        \"name\": \"N\",\n" + "        \"fields\": [\n"
            + "          {\"name\": \"city\", \"type\": \"string\"}\n" + "        ]\n" + "      }\n" + "    }\n"
            + "  ]\n" + "}");
    GenericRecord nested = new GenericData.Record(input.getField("nested").schema());
    nested.put("city", "NYC");
    GenericRecord src = new GenericData.Record(input);
    src.put("nested", nested);

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertEquals(out.getSchema(), writer);
    GenericRecord outNested = (GenericRecord) out.get("nested");
    Assert.assertEquals(outNested.get("city").toString(), "NYC");
    Assert.assertEquals(outNested.getSchema().getField("city").schema().getType(), Schema.Type.STRING);
  }

  @Test
  public void testArrayOfRecordsElementProjected() {
    // The element record evolved via OH (extra retained field zip); the writer keeps the original element record.
    // Projection must rebuild every element under the writer element schema and drop the extra field.
    Schema input = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"addresses\",\n" + "      \"type\": {\n" + "        \"type\": \"array\",\n"
            + "        \"items\": {\n" + "          \"type\": \"record\",\n" + "          \"name\": \"Address\",\n"
            + "          \"fields\": [\n" + "            {\"name\": \"city\", \"type\": \"string\"},\n"
            + "            {\"name\": \"zip\", \"type\": [\"null\", \"int\"], \"default\": null}\n" + "          ]\n"
            + "        }\n" + "      }\n" + "    }\n" + "  ]\n" + "}");
    Schema writer = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"addresses\",\n" + "      \"type\": {\n" + "        \"type\": \"array\",\n"
            + "        \"items\": {\n" + "          \"type\": \"record\",\n" + "          \"name\": \"Address\",\n"
            + "          \"fields\": [\n" + "            {\"name\": \"city\", \"type\": \"string\"}\n" + "          ]\n"
            + "        }\n" + "      }\n" + "    }\n" + "  ]\n" + "}");
    Schema inputElementSchema = input.getField("addresses").schema().getElementType();
    GenericRecord a1 = new GenericData.Record(inputElementSchema);
    a1.put("city", "NYC");
    a1.put("zip", 10001);
    GenericRecord a2 = new GenericData.Record(inputElementSchema);
    a2.put("city", "SF");
    a2.put("zip", 94016);
    GenericRecord src = new GenericData.Record(input);
    src.put("addresses", new ArrayList<>(Arrays.asList(a1, a2)));

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertEquals(out.getSchema(), writer);
    @SuppressWarnings("unchecked")
    List<GenericRecord> outAddresses = (List<GenericRecord>) out.get("addresses");
    Assert.assertEquals(outAddresses.size(), 2);
    Assert.assertEquals(outAddresses.get(0).get("city").toString(), "NYC");
    Assert.assertEquals(outAddresses.get(1).get("city").toString(), "SF");
    Assert.assertNull(
        outAddresses.get(0).getSchema().getField("zip"),
        "extra field must be dropped from every array element");
  }

  @Test
  public void testMapOfRecordsValueProjected() {
    // Mirror of the array case for map values: the value record evolved with an extra retained field that projection
    // must drop while rebuilding each value under the writer value schema.
    Schema input = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"byId\",\n" + "      \"type\": {\n" + "        \"type\": \"map\",\n"
            + "        \"values\": {\n" + "          \"type\": \"record\",\n" + "          \"name\": \"Address\",\n"
            + "          \"fields\": [\n" + "            {\"name\": \"city\", \"type\": \"string\"},\n"
            + "            {\"name\": \"zip\", \"type\": [\"null\", \"int\"], \"default\": null}\n" + "          ]\n"
            + "        }\n" + "      }\n" + "    }\n" + "  ]\n" + "}");
    Schema writer = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"byId\",\n" + "      \"type\": {\n" + "        \"type\": \"map\",\n"
            + "        \"values\": {\n" + "          \"type\": \"record\",\n" + "          \"name\": \"Address\",\n"
            + "          \"fields\": [\n" + "            {\"name\": \"city\", \"type\": \"string\"}\n" + "          ]\n"
            + "        }\n" + "      }\n" + "    }\n" + "  ]\n" + "}");
    Schema inputValueSchema = input.getField("byId").schema().getValueType();
    GenericRecord v = new GenericData.Record(inputValueSchema);
    v.put("city", "NYC");
    v.put("zip", 10001);
    Map<String, GenericRecord> srcMap = new HashMap<>();
    srcMap.put("a", v);
    GenericRecord src = new GenericData.Record(input);
    src.put("byId", srcMap);

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertEquals(out.getSchema(), writer);
    @SuppressWarnings("unchecked")
    Map<CharSequence, GenericRecord> outMap = (Map<CharSequence, GenericRecord>) out.get("byId");
    Assert.assertEquals(outMap.size(), 1);
    GenericRecord outValue = outMap.values().iterator().next();
    Assert.assertEquals(outValue.get("city").toString(), "NYC");
    Assert.assertNull(outValue.getSchema().getField("zip"), "extra field must be dropped from every map value");
  }

  @Test
  public void testNullableUnionWriterFieldProjectedAndExtraDropped() {
    // A nullable-union writer [null, Addr{city}] against a nullable-union input whose non-null branch evolved into a
    // superset [null, Addr{city, zip}]: the present record value is rebuilt under the writer's non-null branch and the
    // extra `zip` field is dropped.
    Schema writer = parse(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"addr\",\"type\":[\"null\","
            + "{\"type\":\"record\",\"name\":\"Addr\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"}]}],"
            + "\"default\":null}]}");
    Schema input = parse(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"addr\",\"type\":[\"null\","
            + "{\"type\":\"record\",\"name\":\"Addr\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"},"
            + "{\"name\":\"zip\",\"type\":[\"null\",\"int\"],\"default\":null}]}],\"default\":null}]}");
    Schema inputAddrSchema = input.getField("addr").schema().getTypes().get(1);
    GenericRecord addr = new GenericData.Record(inputAddrSchema);
    addr.put("city", "NYC");
    addr.put("zip", 10001);
    GenericRecord src = new GenericData.Record(input);
    src.put("addr", addr);

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertEquals(out.getSchema(), writer);
    GenericRecord outAddr = (GenericRecord) out.get("addr");
    Assert.assertEquals(outAddr.getSchema().getName(), "Addr");
    Assert.assertEquals(outAddr.get("city").toString(), "NYC");
    Assert.assertNull(outAddr.getSchema().getField("zip"), "extra field in the nullable-union branch must be dropped");
  }

  @Test
  public void testNullableUnionWriterFieldWithNullValueProjectedToNull() {
    // Null value against a nullable-union writer [null, Addr] projects to null (the null branch needs no rebuild).
    Schema writer = parse(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"addr\",\"type\":[\"null\","
            + "{\"type\":\"record\",\"name\":\"Addr\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"}]}],"
            + "\"default\":null}]}");
    Schema input = parse(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"addr\",\"type\":[\"null\","
            + "{\"type\":\"record\",\"name\":\"Addr\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"},"
            + "{\"name\":\"zip\",\"type\":[\"null\",\"int\"],\"default\":null}]}],\"default\":null}]}");
    GenericRecord src = new GenericData.Record(input);
    src.put("addr", null);

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertEquals(out.getSchema(), writer);
    Assert.assertNull(out.get("addr"));
  }

  @Test
  public void testSingleElementUnionWriterFieldProjectsValue() {
    // Input field: nullable union [null, long]. Writer field: single-element union [long] (equivalent to bare long).
    // The present value projects onto the writer's single-element union.
    Schema writer = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"f1\",\"type\":[\"long\"]}]}");
    Schema input = parse(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"f1\",\"type\":[\"null\",\"long\"],\"default\":null}]}");
    GenericRecord src = new GenericData.Record(input);
    src.put("f1", 42L);

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertEquals(out.getSchema(), writer);
    Assert.assertEquals(out.get("f1"), 42L);
  }

  @Test
  public void testEmptyStructWriterDropsDummyFillerField() {
    // OH cannot represent an empty struct, so it fills it with a synthetic dummy field. The registered writer keeps the
    // empty struct; projection must rebuild the (empty) writer record and drop the input's dummy filler field.
    Schema writer = parse(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"recordWithEmptyStruct\",\"type\":[\"null\","
            + "{\"type\":\"record\",\"name\":\"emptyStructRecord\","
            + "\"namespace\":\"com.linkedin.SchemaWithEmptyStructs.emptyStruct\",\"doc\":\"record with empty struct\","
            + "\"fields\":[{\"name\":\"emptyStructUnionField\",\"type\":[\"null\","
            + "{\"type\":\"record\",\"name\":\"emptyStructField\",\"fields\":[]}],\"default\":null}]}],"
            + "\"default\":null}]}");
    Schema input = parse(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"recordWithEmptyStruct\",\"type\":[\"null\","
            + "{\"type\":\"record\",\"name\":\"emptyStructRecord\","
            + "\"namespace\":\"com.linkedin.SchemaWithEmptyStructs.emptyStruct\",\"doc\":\"record with empty struct\","
            + "\"fields\":[{\"name\":\"emptyStructUnionField\",\"type\":[\"null\","
            + "{\"type\":\"record\",\"name\":\"emptyStructField\",\"fields\":[{\"name\":"
            + "\"__dummy_field_to_fill_empty_struct__\",\"type\":\"int\",\"doc\":"
            + "\"Dummy field added to handle empty struct records.\",\"default\":-1}]}],\"default\":null}]}],"
            + "\"default\":null}]}");
    Schema inputEmptyStructRecord = input.getField("recordWithEmptyStruct").schema().getTypes().get(1);
    Schema inputEmptyStructField = inputEmptyStructRecord.getField("emptyStructUnionField").schema().getTypes().get(1);
    GenericRecord innerStruct = new GenericData.Record(inputEmptyStructField);
    innerStruct.put("__dummy_field_to_fill_empty_struct__", -1);
    GenericRecord outerStruct = new GenericData.Record(inputEmptyStructRecord);
    outerStruct.put("emptyStructUnionField", innerStruct);
    GenericRecord src = new GenericData.Record(input);
    src.put("recordWithEmptyStruct", outerStruct);

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertEquals(out.getSchema(), writer);
    GenericRecord outOuter = (GenericRecord) out.get("recordWithEmptyStruct");
    Assert.assertEquals(outOuter.getSchema().getName(), "emptyStructRecord");
    GenericRecord outInner = (GenericRecord) outOuter.get("emptyStructUnionField");
    Assert.assertEquals(outInner.getSchema().getName(), "emptyStructField");
    Assert.assertTrue(outInner.getSchema().getFields().isEmpty(), "projected empty struct must have no fields");
    Assert.assertNull(
        outInner.getSchema().getField("__dummy_field_to_fill_empty_struct__"),
        "OH dummy filler field must be dropped");
  }

  @Test
  public void testDeeplyNestedExtraFieldDropped() {
    // Projection recurses to arbitrary depth: an extra retained field two record levels down (R -> outer -> inner)
    // must still be dropped while the deeper non-nullable leaf is copied across.
    Schema input = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"outer\",\n" + "      \"type\": {\n" + "        \"type\": \"record\",\n"
            + "        \"name\": \"Outer\",\n" + "        \"fields\": [\n" + "          {\n"
            + "            \"name\": \"inner\",\n" + "            \"type\": {\n"
            + "              \"type\": \"record\",\n" + "              \"name\": \"Inner\",\n"
            + "              \"fields\": [\n" + "                {\"name\": \"leaf\", \"type\": \"string\"},\n"
            + "                {\"name\": \"extra\", \"type\": \"int\", \"default\": 0}\n" + "              ]\n"
            + "            }\n" + "          }\n" + "        ]\n" + "      }\n" + "    }\n" + "  ]\n" + "}");
    Schema writer = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"outer\",\n" + "      \"type\": {\n" + "        \"type\": \"record\",\n"
            + "        \"name\": \"Outer\",\n" + "        \"fields\": [\n" + "          {\n"
            + "            \"name\": \"inner\",\n" + "            \"type\": {\n"
            + "              \"type\": \"record\",\n" + "              \"name\": \"Inner\",\n"
            + "              \"fields\": [\n" + "                {\"name\": \"leaf\", \"type\": \"string\"}\n"
            + "              ]\n" + "            }\n" + "          }\n" + "        ]\n" + "      }\n" + "    }\n"
            + "  ]\n" + "}");
    Schema inputOuterSchema = input.getField("outer").schema();
    Schema inputInnerSchema = inputOuterSchema.getField("inner").schema();
    GenericRecord inner = new GenericData.Record(inputInnerSchema);
    inner.put("leaf", "deep");
    inner.put("extra", 9);
    GenericRecord outer = new GenericData.Record(inputOuterSchema);
    outer.put("inner", inner);
    GenericRecord src = new GenericData.Record(input);
    src.put("outer", outer);

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertEquals(out.getSchema(), writer);
    GenericRecord outInner = (GenericRecord) ((GenericRecord) out.get("outer")).get("inner");
    Assert.assertEquals(outInner.get("leaf").toString(), "deep");
    Assert.assertNull(outInner.getSchema().getField("extra"), "extra deeply-nested field must be dropped");
  }

  @Test
  public void testNullableInputFieldWithNullValueCopiedThroughToNonNullableWriter() {
    // OH should not emit null for a field the writer keeps non-nullable. If it does anyway, the projector adds no early
    // guard: it copies the null through under the writer's non-nullable field, and the failure surfaces later at
    // serialization (Avro rejects null for a non-nullable type). This pins that deferred-failure contract -- projection
    // itself does not throw.
    Schema input = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"f1\", \"type\": [\"null\", \"string\"], \"default\": null}\n" + "  ]\n" + "}");
    Schema writer = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n"
            + "    {\"name\": \"f1\", \"type\": \"string\"}\n" + "  ]\n" + "}");
    GenericRecord src = new GenericData.Record(input);
    src.put("f1", null);

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertEquals(out.getSchema(), writer);
    Assert.assertNull(out.get("f1"), "null is copied through; serialization (not projection) rejects it");
  }

  @Test
  public void testArrayElementNullableFieldUnwrapped() {
    // Recursion must unwrap nullable fields INSIDE array element records (not just drop extras): the element's city
    // evolved to [null, string] while the writer keeps it non-nullable, with a present value.
    Schema input = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"addresses\",\n" + "      \"type\": {\n" + "        \"type\": \"array\",\n"
            + "        \"items\": {\n" + "          \"type\": \"record\",\n" + "          \"name\": \"Address\",\n"
            + "          \"fields\": [\n"
            + "            {\"name\": \"city\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
            + "          ]\n" + "        }\n" + "      }\n" + "    }\n" + "  ]\n" + "}");
    Schema writer = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"addresses\",\n" + "      \"type\": {\n" + "        \"type\": \"array\",\n"
            + "        \"items\": {\n" + "          \"type\": \"record\",\n" + "          \"name\": \"Address\",\n"
            + "          \"fields\": [\n" + "            {\"name\": \"city\", \"type\": \"string\"}\n" + "          ]\n"
            + "        }\n" + "      }\n" + "    }\n" + "  ]\n" + "}");
    Schema inputElementSchema = input.getField("addresses").schema().getElementType();
    GenericRecord a1 = new GenericData.Record(inputElementSchema);
    a1.put("city", "NYC");
    GenericRecord src = new GenericData.Record(input);
    src.put("addresses", new ArrayList<>(Arrays.asList(a1)));

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertEquals(out.getSchema(), writer);
    @SuppressWarnings("unchecked")
    List<GenericRecord> outAddresses = (List<GenericRecord>) out.get("addresses");
    Assert.assertEquals(outAddresses.get(0).get("city").toString(), "NYC");
    Assert.assertEquals(
        outAddresses.get(0).getSchema().getField("city").schema().getType(),
        Schema.Type.STRING,
        "nullable wrapper inside the array element must be unwrapped");
  }

  @Test
  public void testDeeplyNestedNullableFieldUnwrapped() {
    // Mirror of the deep extra-field-drop on the unwrap side: a nullable leaf two record levels down is unwrapped to
    // the writer's non-nullable leaf, with a present value.
    Schema input = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"outer\",\n" + "      \"type\": {\n" + "        \"type\": \"record\",\n"
            + "        \"name\": \"Outer\",\n" + "        \"fields\": [\n" + "          {\n"
            + "            \"name\": \"inner\",\n" + "            \"type\": {\n"
            + "              \"type\": \"record\",\n" + "              \"name\": \"Inner\",\n"
            + "              \"fields\": [\n"
            + "                {\"name\": \"leaf\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
            + "              ]\n" + "            }\n" + "          }\n" + "        ]\n" + "      }\n" + "    }\n"
            + "  ]\n" + "}");
    Schema writer = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"fields\": [\n" + "    {\n"
            + "      \"name\": \"outer\",\n" + "      \"type\": {\n" + "        \"type\": \"record\",\n"
            + "        \"name\": \"Outer\",\n" + "        \"fields\": [\n" + "          {\n"
            + "            \"name\": \"inner\",\n" + "            \"type\": {\n"
            + "              \"type\": \"record\",\n" + "              \"name\": \"Inner\",\n"
            + "              \"fields\": [\n" + "                {\"name\": \"leaf\", \"type\": \"string\"}\n"
            + "              ]\n" + "            }\n" + "          }\n" + "        ]\n" + "      }\n" + "    }\n"
            + "  ]\n" + "}");
    Schema inputOuterSchema = input.getField("outer").schema();
    Schema inputInnerSchema = inputOuterSchema.getField("inner").schema();
    GenericRecord inner = new GenericData.Record(inputInnerSchema);
    inner.put("leaf", "deep");
    GenericRecord outer = new GenericData.Record(inputOuterSchema);
    outer.put("inner", inner);
    GenericRecord src = new GenericData.Record(input);
    src.put("outer", outer);

    GenericRecord out = projector.projectValue(src, writer);

    Assert.assertEquals(out.getSchema(), writer);
    GenericRecord outInner = (GenericRecord) ((GenericRecord) out.get("outer")).get("inner");
    Assert.assertEquals(outInner.get("leaf").toString(), "deep");
    Assert.assertEquals(
        outInner.getSchema().getField("leaf").schema().getType(),
        Schema.Type.STRING,
        "deeply-nested nullable wrapper must be unwrapped");
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
  public void testRmdPerFieldTimestampProjectedForNullableDrift() {
    // OH nullable drift: the input value wraps f2 as [null, string] while the writer keeps it non-nullable. The
    // per-field RMD is identical on both sides (the RMD generator unwraps nullable unions), so projecting the per-field
    // timestamp preserves both entries.
    Schema inputValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"f1\", \"type\": \"string\"},\n"
            + "    {\"name\": \"f2\", \"type\": [\"null\", \"string\"], \"default\": null}\n" + "  ]\n" + "}");
    Schema writerValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"f1\", \"type\": \"string\"},\n"
            + "    {\"name\": \"f2\", \"type\": \"string\"}\n" + "  ]\n" + "}");
    Schema inputRmd = RMD_GEN.generateMetadataSchema(inputValue);
    Schema writerRmd = RMD_GEN.generateMetadataSchema(writerValue);

    Schema inputPerFieldSchema = inputRmd.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    GenericRecord perField = new GenericData.Record(inputPerFieldSchema);
    perField.put("f1", 100L);
    perField.put("f2", 200L);
    GenericRecord srcRmd = new GenericData.Record(inputRmd);
    srcRmd.put(TIMESTAMP_FIELD_NAME, perField);
    srcRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, new ArrayList<>(Arrays.asList(3L)));

    GenericRecord out = projector.projectRmd(srcRmd, writerRmd);

    Assert.assertEquals(out.getSchema(), writerRmd);
    GenericRecord outPerField = (GenericRecord) out.get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(outPerField.get("f1"), 100L);
    Assert.assertEquals(outPerField.get("f2"), 200L);
    @SuppressWarnings("unchecked")
    List<Long> rcv = (List<Long>) out.get(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME);
    Assert.assertEquals(rcv, Arrays.asList(3L));
  }

  @Test
  public void testRmdPerFieldTimestampProjectedDropsRemovedField() {
    // The writer value schema has fewer fields than the input (no f2). Projecting the per-field RMD keeps f1's
    // timestamp and drops f2's, in lockstep with the value projection dropping the f2 field.
    Schema inputValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"f1\", \"type\": \"string\"},\n"
            + "    {\"name\": \"f2\", \"type\": [\"null\", \"string\"], \"default\": null}\n" + "  ]\n" + "}");
    Schema writerValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"f1\", \"type\": \"string\"}\n" + "  ]\n" + "}");
    Schema inputRmd = RMD_GEN.generateMetadataSchema(inputValue);
    Schema writerRmd = RMD_GEN.generateMetadataSchema(writerValue);

    Schema inputPerFieldSchema = inputRmd.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    GenericRecord perField = new GenericData.Record(inputPerFieldSchema);
    perField.put("f1", 100L);
    perField.put("f2", 200L);
    GenericRecord srcRmd = new GenericData.Record(inputRmd);
    srcRmd.put(TIMESTAMP_FIELD_NAME, perField);
    srcRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, new ArrayList<>());

    GenericRecord out = projector.projectRmd(srcRmd, writerRmd);

    Assert.assertEquals(out.getSchema(), writerRmd);
    GenericRecord outPerField = (GenericRecord) out.get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(outPerField.get("f1"), 100L);
    Assert.assertNull(outPerField.getSchema().getField("f2"), "dropped value field's per-field timestamp must be gone");
  }

  @Test
  public void testRmdPerFieldTimestampWithCollectionFieldSerializesRoundTrip() {
    // The in-memory assertions above never serialize the projected RMD. Here we serialize it against the writer RMD
    // schema (as the real push does) and read it back. A per-field timestamp for a COLLECTION field is itself a record
    // (CollectionMetadata), and the input has an extra collection field (droppedList) before the shared one (tags), so
    // the input and writer collection-metadata records carry different generated names -- this proves projection +
    // serialization tolerate that divergence.
    Schema inputValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"name\", \"type\": \"string\"},\n"
            + "    {\"name\": \"droppedList\", \"type\": {\"type\": \"array\", \"items\": \"int\"}},\n"
            + "    {\"name\": \"tags\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}\n" + "  ]\n" + "}");
    Schema writerValue = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"name\", \"type\": \"string\"},\n"
            + "    {\"name\": \"tags\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}\n" + "  ]\n" + "}");
    Schema inputRmd = RMD_GEN.generateMetadataSchema(inputValue);
    Schema writerRmd = RMD_GEN.generateMetadataSchema(writerValue);

    Schema inputPerFieldSchema = inputRmd.getField(TIMESTAMP_FIELD_NAME).schema().getTypes().get(1);
    GenericRecord perField = new GenericData.Record(inputPerFieldSchema);
    perField.put("name", 100L);
    perField
        .put("droppedList", AvroSchemaUtils.createGenericRecord(inputPerFieldSchema.getField("droppedList").schema()));
    GenericRecord tagsMd = AvroSchemaUtils.createGenericRecord(inputPerFieldSchema.getField("tags").schema());
    tagsMd.put("topLevelFieldTimestamp", 200L);
    perField.put("tags", tagsMd);

    GenericRecord srcRmd = new GenericData.Record(inputRmd);
    srcRmd.put(TIMESTAMP_FIELD_NAME, perField);
    srcRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, new ArrayList<>(Arrays.asList(9L)));

    GenericRecord projected = projector.projectRmd(srcRmd, writerRmd);

    // Round-trip through the same serializer the push pipeline uses.
    byte[] bytes = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(writerRmd).serialize(projected);
    GenericRecord roundTripped =
        FastSerializerDeserializerFactory.<GenericRecord>getFastAvroGenericDeserializer(writerRmd, writerRmd)
            .deserialize(bytes);

    Assert.assertEquals(roundTripped.getSchema(), writerRmd);
    GenericRecord rtPerField = (GenericRecord) roundTripped.get(TIMESTAMP_FIELD_NAME);
    Assert.assertEquals(rtPerField.get("name"), 100L);
    Assert.assertNull(rtPerField.getSchema().getField("droppedList"), "dropped collection field's RMD must be gone");
    GenericRecord rtTagsMd = (GenericRecord) rtPerField.get("tags");
    Assert.assertEquals(rtTagsMd.get("topLevelFieldTimestamp"), 200L);
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
  public void testReplicationCheckpointVectorIsSharedByReference() {
    Schema valueSchema = parse(
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"R\",\n" + "  \"namespace\": \"ns\",\n"
            + "  \"fields\": [\n" + "    {\"name\": \"f1\", \"type\": \"string\"}\n" + "  ]\n" + "}");
    Schema rmdSchema = RMD_GEN.generateMetadataSchema(valueSchema);
    List<Long> rcv = new ArrayList<>(Arrays.asList(1L, 2L, 3L));
    GenericRecord srcRmd = new GenericData.Record(rmdSchema);
    srcRmd.put(TIMESTAMP_FIELD_NAME, 1L);
    srcRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME, rcv);

    GenericRecord out = projector.projectRmd(srcRmd, rmdSchema);

    Assert.assertSame(out.get(REPLICATION_CHECKPOINT_VECTOR_FIELD_NAME), rcv);
  }
}
