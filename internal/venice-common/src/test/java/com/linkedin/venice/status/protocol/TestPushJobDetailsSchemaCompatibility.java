package com.linkedin.venice.status.protocol;

import static java.util.Collections.emptyList;

import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
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
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPushJobDetailsSchemaCompatibility {
  private static final int STAGED_SCHEMA_ID = 6;
  private static final int ACTIVE_SCHEMA_ID = STAGED_SCHEMA_ID - 1;
  private static final List<String> STAGED_FIELDS = Arrays.asList("externalStorageWriteTimeMs", "veniceWriteTimeMs");

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
   * v6 is registered but not activated: the schema ships in the resources so controllers register it in the push job
   * details system store, while {@link AvroProtocolDefinition#PUSH_JOB_DETAILS} and the generated classes (pinned to
   * v5 through the {@code versionOverrides} entry in the root build.gradle) stay on v5, so the code keeps
   * serializing v5. The follow-up PR that populates the new fields removes the pin and bumps the protocol
   * definition.
   */
  @Test
  public void testStagedV6IsRegisteredButNotActivated() throws IOException {
    Assert.assertEquals(
        AvroProtocolDefinition.PUSH_JOB_DETAILS.currentProtocolVersion.get().intValue(),
        ACTIVE_SCHEMA_ID,
        "PushJobDetails v6 must stay staged until the push job actually populates the new fields");
    Assert.assertNull(
        schemaVersionMap.get(STAGED_SCHEMA_ID),
        "The compiled PushJobDetails class must still be v5 while v6 is only staged");

    Schema activeSchema = schemaVersionMap.get(ACTIVE_SCHEMA_ID);
    Schema stagedSchema = getStagedSchema();
    List<String> addedFields = stagedSchema.getFields()
        .stream()
        .map(Schema.Field::name)
        .filter(name -> activeSchema.getField(name) == null)
        .collect(Collectors.toList());
    Assert.assertEquals(addedFields, STAGED_FIELDS, "v6 must only append the two timing fields on top of v5");
    for (String addedField: STAGED_FIELDS) {
      Schema.Field field = stagedSchema.getField(addedField);
      Assert.assertEquals(field.schema().getType(), Schema.Type.LONG);
      Assert.assertTrue(field.hasDefaultValue(), addedField + " must carry a default so v5 records stay readable");
      Assert.assertEquals(GenericData.get().getDefaultValue(field), -1L, addedField + " must default to -1");
    }

    // Registration only succeeds if v6 is compatible in both directions with the version currently being written.
    assertCompatible(stagedSchema, activeSchema);
    assertCompatible(activeSchema, stagedSchema);
  }

  /**
   * A controller running the v6 reader must be able to read records still produced by push jobs that serialize v5,
   * resolving the two appended duration fields to their -1 defaults. That is what makes the "deploy controllers
   * before push jobs" ordering safe, and what lets the controller treat -1 as "not reported" instead of recording a
   * bogus zero-millisecond observation.
   */
  @Test
  public void testV6ReaderResolvesMissingDurationsFromV5WriterToDefaults() throws IOException {
    Schema v5Schema = schemaVersionMap.get(ACTIVE_SCHEMA_ID);
    Schema v6Schema = getStagedSchema();

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

    Assert.assertEquals(v6Record.get("externalStorageWriteTimeMs"), -1L);
    Assert.assertEquals(v6Record.get("veniceWriteTimeMs"), -1L);
    Assert.assertEquals(v6Record.get("jobDurationInMs"), 60000L);
  }

  /**
   * {@link Utils#getAllSchemasFromResources(AvroProtocolDefinition)} stops at the compiled version, so a staged
   * schema has to be read straight out of the resources.
   */
  private Schema getStagedSchema() throws IOException {
    return Utils.getSchemaFromResource("avro/PushJobDetails/v" + STAGED_SCHEMA_ID + "/PushJobDetails.avsc");
  }

  private void assertCompatible(Schema reader, Schema writer) {
    Assert.assertEquals(
        SchemaCompatibility.checkReaderWriterCompatibility(reader, writer).getType(),
        SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
        "PushJobDetails v" + STAGED_SCHEMA_ID + " must be compatible with v" + ACTIVE_SCHEMA_ID
            + " in both directions");
  }
}
