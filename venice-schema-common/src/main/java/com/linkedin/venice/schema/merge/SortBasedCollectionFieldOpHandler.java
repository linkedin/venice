package com.linkedin.venice.schema.merge;

import com.linkedin.venice.utils.IndexedHashMap;
import com.linkedin.venice.schema.rmd.v2.CollectionReplicationMetadata;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;


public class SortBasedCollectionFieldOpHandler extends CollectionFieldOperationHandler {

  public SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator elementComparator) {
    super(elementComparator);
  }

  @Override
  public UpdateResultStatus handlePutList(
      final long putTimestamp,
      final int coloID,
      List<Object> toPutList,
      CollectionReplicationMetadata collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName
  ) {
    // TODO: implement it
    return null;
  }


  @Override
  public UpdateResultStatus handlePutMap(
      final long putTimestamp,
      final int coloID,
      IndexedHashMap<String, Object> toPutMap,
      CollectionReplicationMetadata collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName
  ) {
    // TODO: implement it
    return null;
  }


  @Override
  public UpdateResultStatus handleDeleteList(
      final long deleteTimestamp,
      final int coloID,
      CollectionReplicationMetadata collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName
  ) {
    // TODO: implement it
    return null;
  }


  @Override
  public UpdateResultStatus handleDeleteMap(
      final long deleteTimestamp,
      final int coloID,
      CollectionReplicationMetadata collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName
  ) {
    // TODO: implement it
    return null;
  }

  @Override
  public UpdateResultStatus handleModifyList(
      final long modifyTimestamp,
      CollectionReplicationMetadata collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName,
      List<Object> toAddElements,
      List<Object> toRemoveElements
  ) {
    // TODO: implement it
    return null;
  }

  @Override
  public UpdateResultStatus handleModifyMap(
      final long modifyTimestamp,
      CollectionReplicationMetadata collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName,
      Map<String, Object> newEntries,
      List<String> toRemoveKeys
  ) {
    // TODO: implement it
    return null;
  }
}
