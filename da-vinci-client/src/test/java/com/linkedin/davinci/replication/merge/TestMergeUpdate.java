package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.schema.SchemaUtils;
import com.linkedin.venice.schema.writecompute.WriteComputeConstants;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;


/**
 * This is a base class for testing the handling of Update (Write Compute) request.
 */
public class TestMergeUpdate extends TestMergeConflictResolver {
  protected GenericRecord createListMergeRecord(
      String fieldName,
      List<?> toAdd,
      List<?> toRemove,
      Schema writeComputeSchema) {
    Schema listMergeSchema = writeComputeSchema.getTypes().get(0).getField(fieldName).schema().getTypes().get(1);
    GenericRecord listMerge = SchemaUtils.createGenericRecord(listMergeSchema);
    listMerge.put(WriteComputeConstants.SET_UNION, toAdd);
    listMerge.put(WriteComputeConstants.SET_DIFF, toRemove);
    return listMerge;
  }

  protected GenericRecord createMapMergeRecord(
      String fieldName,
      Map<String, ?> toAddEntries,
      List<String> toRemoveKeys,
      Schema writeComputeSchema) {
    Schema mapMergeSchema = writeComputeSchema.getTypes().get(0).getField(fieldName).schema().getTypes().get(1);
    GenericRecord mapMerge = SchemaUtils.createGenericRecord(mapMergeSchema);
    mapMerge.put(WriteComputeConstants.MAP_UNION, toAddEntries);
    mapMerge.put(WriteComputeConstants.MAP_DIFF, toRemoveKeys);
    return mapMerge;
  }

  protected Utf8 toUtf8(String str) {
    return new Utf8(str);
  }
}
