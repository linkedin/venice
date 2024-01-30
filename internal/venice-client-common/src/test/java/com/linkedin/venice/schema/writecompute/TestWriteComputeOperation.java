package com.linkedin.venice.schema.writecompute;

import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsString;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestWriteComputeOperation {
  @Test
  public void testGetFieldOperationType() {
    Schema updateSchemaForList = WriteComputeSchemaConverter.getInstance()
        .convertFromValueRecordSchema(AvroCompatibilityHelper.parse(loadFileAsString("partialUpdateSchemaV1.avsc")));

    GenericRecord updateRequest = new UpdateBuilderImpl(updateSchemaForList).setNewFieldValue("regularField", "abc")
        .setNewFieldValue("listField", Collections.emptyList())
        .setNewFieldValue("nullableListField", null)
        .setNewFieldValue("mapField", Collections.emptyMap())
        .setNewFieldValue("nullableMapField", null)
        .build();
    Assert.assertEquals(
        WriteComputeOperation.getFieldOperationType(updateRequest.get("regularField")),
        WriteComputeOperation.PUT_NEW_FIELD);
    Assert.assertEquals(
        WriteComputeOperation.getFieldOperationType(updateRequest.get("listField")),
        WriteComputeOperation.PUT_NEW_FIELD);
    Assert.assertEquals(
        WriteComputeOperation.getFieldOperationType(updateRequest.get("nullableListField")),
        WriteComputeOperation.PUT_NEW_FIELD);
    Assert.assertEquals(
        WriteComputeOperation.getFieldOperationType(updateRequest.get("mapField")),
        WriteComputeOperation.PUT_NEW_FIELD);
    Assert.assertEquals(
        WriteComputeOperation.getFieldOperationType(updateRequest.get("nullableMapField")),
        WriteComputeOperation.PUT_NEW_FIELD);

    updateRequest = new UpdateBuilderImpl(updateSchemaForList)
        .setElementsToAddToListField("listField", Collections.singletonList(1))
        .setElementsToRemoveFromListField("nullableListField", Collections.singletonList(1))
        .setEntriesToAddToMapField("mapField", Collections.singletonMap("k1", 1))
        .setKeysToRemoveFromMapField("nullableMapField", Collections.singletonList("k1"))
        .build();
    Assert.assertEquals(
        WriteComputeOperation.getFieldOperationType(updateRequest.get("regularField")),
        WriteComputeOperation.NO_OP_ON_FIELD);
    Assert.assertEquals(
        WriteComputeOperation.getFieldOperationType(updateRequest.get("listField")),
        WriteComputeOperation.LIST_OPS);
    Assert.assertEquals(
        WriteComputeOperation.getFieldOperationType(updateRequest.get("nullableListField")),
        WriteComputeOperation.LIST_OPS);
    Assert.assertEquals(
        WriteComputeOperation.getFieldOperationType(updateRequest.get("mapField")),
        WriteComputeOperation.MAP_OPS);
    Assert.assertEquals(
        WriteComputeOperation.getFieldOperationType(updateRequest.get("nullableMapField")),
        WriteComputeOperation.MAP_OPS);
  }
}
