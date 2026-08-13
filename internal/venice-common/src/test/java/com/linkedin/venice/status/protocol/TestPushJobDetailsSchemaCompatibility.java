package com.linkedin.venice.status.protocol;

import static java.util.Collections.emptyList;

import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPushJobDetailsSchemaCompatibility {
  private static final int ACTIVATED_SCHEMA_ID = 6;
  private static final int PREVIOUS_SCHEMA_ID = ACTIVATED_SCHEMA_ID - 1;
  private static final String ADDED_FIELD = "additionalPushMetrics";

  private Map<Integer, Schema> schemaVersionMap =
      Utils.getAllSchemasFromResources(AvroProtocolDefinition.PUSH_JOB_DETAILS);

  @Test
  public void testPushJobStatusValueSchemaCompatibility() {
    Assert.assertTrue(AvroProtocolDefinition.PUSH_JOB_DETAILS.currentProtocolVersion.isPresent());
    int latestSchemaId = AvroProtocolDefinition.PUSH_JOB_DETAILS.currentProtocolVersion.get();
    Schema latestSchema = schemaVersionMap.get(latestSchemaId);
    schemaVersionMap.forEach((schemaId, schema) -> {
      if (schemaId == latestSchemaId) {
        return;
      }
      SchemaCompatibility.SchemaPairCompatibility backwardCompatibility =
          SchemaCompatibility.checkReaderWriterCompatibility(latestSchema, schema);
      Assert.assertEquals(
          backwardCompatibility.getType(),
          SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
          "PushJobDetails schema version " + schemaId + " is incompatible with the latest schema " + "version of "
              + latestSchemaId);
    });
  }

  /**
   * v6 was registered by #2971 with the generated classes pinned to v5. This PR removes that pin and activates
   * it, so the compiled class and the protocol definition must now both be on v6, and the nullable
   * additionalPushMetrics map must be the only addition on top of v5.
   */
  @Test
  public void testV6IsActivatedAndOnlyAppendsTheAdditionalPushMetricsMap() {
    Assert.assertEquals(
        AvroProtocolDefinition.PUSH_JOB_DETAILS.currentProtocolVersion.get().intValue(),
        ACTIVATED_SCHEMA_ID,
        "PushJobDetails must be activated at v6 for the push job to report the write timings");
    Schema activeSchema = schemaVersionMap.get(ACTIVATED_SCHEMA_ID);
    Assert.assertNotNull(activeSchema, "The compiled PushJobDetails class must be v6, i.e. no versionOverrides pin");

    Schema previousSchema = schemaVersionMap.get(PREVIOUS_SCHEMA_ID);
    List<String> addedFields = activeSchema.getFields()
        .stream()
        .map(Schema.Field::name)
        .filter(name -> previousSchema.getField(name) == null)
        .collect(Collectors.toList());
    Assert.assertEquals(
        addedFields,
        Collections.singletonList(ADDED_FIELD),
        "v6 must only append the additionalPushMetrics map on top of v5");

    Schema.Field field = activeSchema.getField(ADDED_FIELD);
    Assert.assertEquals(field.schema().getType(), Schema.Type.UNION, ADDED_FIELD + " must be nullable");
    Schema mapBranch = field.schema()
        .getTypes()
        .stream()
        .filter(branch -> branch.getType() == Schema.Type.MAP)
        .findFirst()
        .orElse(null);
    Assert.assertNotNull(mapBranch, ADDED_FIELD + " must carry a map branch");
    Assert.assertEquals(mapBranch.getValueType().getType(), Schema.Type.LONG, "Metric values must be longs");
    Assert.assertTrue(field.hasDefaultValue(), ADDED_FIELD + " must carry a default so v5 records stay readable");
    Assert.assertNull(
        GenericData.get().getDefaultValue(field),
        ADDED_FIELD + " must default to null, meaning no additional metrics were reported");
  }

  /**
   * The two timing keys are the contract between the push job that writes them and the controller that reads
   * them, so they must come from one shared place rather than being retyped on either side.
   */
  @Test
  public void testTimingMetricKeysAreShared() {
    Assert.assertEquals(PushJobDetailsAdditionalMetrics.EXTERNAL_STORAGE_WRITE_TIME_MS, "externalStorageWriteTimeMs");
    Assert.assertEquals(PushJobDetailsAdditionalMetrics.VENICE_WRITE_TIME_MS, "veniceWriteTimeMs");
  }

  /**
   * A null map and a present-but-missing key both mean "not reported" and must be distinguishable from a
   * reported zero, otherwise the controller records a bogus zero-millisecond observation. Reads must also work
   * on a record that came off the wire, whose map keys are Utf8 rather than String.
   */
  @Test
  public void testAdditionalPushMetricsAccessorsDistinguishAbsentFromZero() {
    PushJobDetails details = new PushJobDetails();

    Assert.assertNull(details.getAdditionalPushMetrics(), "A fresh record must report no additional metrics");
    Assert.assertNull(
        PushJobDetailsAdditionalMetrics.getMetric(details, PushJobDetailsAdditionalMetrics.VENICE_WRITE_TIME_MS),
        "A null map must read back as not reported");

    PushJobDetailsAdditionalMetrics
        .putMetric(details, PushJobDetailsAdditionalMetrics.EXTERNAL_STORAGE_WRITE_TIME_MS, 0L);
    Assert.assertEquals(
        PushJobDetailsAdditionalMetrics
            .getMetric(details, PushJobDetailsAdditionalMetrics.EXTERNAL_STORAGE_WRITE_TIME_MS),
        Long.valueOf(0L),
        "Zero is a legitimate reported duration and must not read back as absent");
    Assert.assertNull(
        PushJobDetailsAdditionalMetrics.getMetric(details, PushJobDetailsAdditionalMetrics.VENICE_WRITE_TIME_MS),
        "A key the push never wrote must read back as not reported");

    // Deserialized records carry Utf8 keys, so a String-keyed lookup must still resolve them.
    PushJobDetails fromTheWire = new PushJobDetails();
    Map<CharSequence, Long> utf8Keyed = new HashMap<>();
    utf8Keyed.put(new Utf8(PushJobDetailsAdditionalMetrics.VENICE_WRITE_TIME_MS), 42L);
    fromTheWire.setAdditionalPushMetrics(utf8Keyed);
    Assert.assertEquals(
        PushJobDetailsAdditionalMetrics.getMetric(fromTheWire, PushJobDetailsAdditionalMetrics.VENICE_WRITE_TIME_MS),
        Long.valueOf(42L),
        "Utf8 map keys must resolve for a String lookup");
  }

  /**
   * A controller running the v6 reader must be able to read records still produced by push jobs that serialize v5,
   * resolving the appended map to its null default. That is what keeps the rollout safe while older push jobs are
   * still in flight, and what lets the controller treat a null map as "not reported" instead of recording a bogus
   * zero-millisecond observation.
   */
  @Test
  public void testV6ReaderResolvesMissingAdditionalPushMetricsFromV5WriterToNull() throws IOException {
    Schema v5Schema = schemaVersionMap.get(PREVIOUS_SCHEMA_ID);
    Schema v6Schema = schemaVersionMap.get(ACTIVATED_SCHEMA_ID);

    GenericRecord v5Record = new GenericData.Record(v5Schema);
    for (Schema.Field field: v5Schema.getFields()) {
      if (field.hasDefaultValue()) {
        v5Record.put(field.name(), GenericData.get().getDefaultValue(field));
      }
    }
    // The v5 fields without a default must be populated explicitly.
    v5Record.put("clusterName", "cluster0");
    v5Record.put("reportTimestamp", 1234L);
    v5Record.put("overallStatus", new GenericData.Array<>(v5Schema.getField("overallStatus").schema(), emptyList()));
    v5Record.put("pushId", "push-from-an-older-vpj");
    v5Record.put("jobDurationInMs", 60000L);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(v5Schema);
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
    writer.write(v5Record, encoder);
    encoder.flush();

    DatumReader<GenericRecord> reader = new GenericDatumReader<>(v5Schema, v6Schema);
    GenericRecord v6Record = reader.read(null, DecoderFactory.get().binaryDecoder(output.toByteArray(), null));

    Assert.assertNull(v6Record.get(ADDED_FIELD), "A v5 record must resolve to a null additionalPushMetrics map");
    Assert.assertEquals(v6Record.get("jobDurationInMs"), 60000L);
  }
}
