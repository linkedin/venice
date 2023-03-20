package com.linkedin.venice.schema.merge;

import com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp;
import com.linkedin.venice.utils.IndexedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.Validate;


/**
 * This class handles all operations on a collection field with replication metadata.
 */
@ThreadSafe
public abstract class CollectionFieldOperationHandler {
  protected final AvroCollectionElementComparator avroElementComparator;

  public CollectionFieldOperationHandler(AvroCollectionElementComparator avroElementComparator) {
    Validate.notNull(avroElementComparator);
    this.avroElementComparator = avroElementComparator;
  }

  public abstract UpdateResultStatus handlePutList(
      final long putTimestamp,
      final int coloID,
      List<Object> newFieldValue,
      CollectionRmdTimestamp<Object> collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName);

  public abstract UpdateResultStatus handlePutMap(
      final long putTimestamp,
      final int coloID,
      IndexedHashMap<String, Object> newFieldValue,
      CollectionRmdTimestamp<String> collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName);

  public abstract UpdateResultStatus handleDeleteList(
      final long deleteTimestamp,
      final int coloID,
      CollectionRmdTimestamp<Object> collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName);

  public abstract UpdateResultStatus handleDeleteMap(
      final long deleteTimestamp,
      final int coloID,
      CollectionRmdTimestamp<String> collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName);

  public abstract UpdateResultStatus handleModifyList(
      final long modifyTimestamp,
      CollectionRmdTimestamp<Object> collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName,
      List<Object> newEntries,
      List<Object> toRemoveKeys);

  public abstract UpdateResultStatus handleModifyMap(
      final long modifyTimestamp,
      CollectionRmdTimestamp<String> collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName,
      Map<String, Object> newEntries,
      List<String> toRemoveKeys);
}
