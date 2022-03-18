package com.linkedin.venice.system.store;

import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.systemstore.schemas.StoreMetaValueWriteOpRecord;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MetaStoreWriteComputeTest {
  @Test
  void validateWriteComputeSchema() {
    Schema derivedComputeSchema = WriteComputeSchemaConverter.convertFromValueRecordSchema(
        AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema());

    int unionBranchForTheWriteOp = 0;

    Schema writeOpSchema = derivedComputeSchema.getTypes().get(unionBranchForTheWriteOp);

    String path = "venice-common/src/test/java/com/linkedin/venice/system/store/MetaStoreWriteComputeTest.java";

    Assert.assertEquals(StoreMetaValueWriteOpRecord.getClassSchema(), writeOpSchema,
        "The " + StoreMetaValueWriteOpRecord.class.getSimpleName()
            + " specific record is not compiled from the expected schema. Please copy the expected schema into: " + path);
  }
}
