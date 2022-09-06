package com.linkedin.venice.system.store;

import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.systemstore.schemas.StoreMetaValueWriteOpRecord;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MetaStoreWriteComputeTest {
  private static final String TEST_PATH =
      "venice-common/src/test/java/com/linkedin/venice/system/store/MetaStoreWriteComputeTest.java";

  @Test
  void validateWriteComputeSchema() {
    Schema writeOpSchema = WriteComputeSchemaConverter.getInstance()
        .convertFromValueRecordSchema(
            AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema());
    Assert.assertEquals(
        StoreMetaValueWriteOpRecord.getClassSchema(),
        writeOpSchema,
        "The " + StoreMetaValueWriteOpRecord.class.getSimpleName()
            + " specific record is not compiled from the expected schema. Please copy the expected schema into: "
            + TEST_PATH);
  }
}
