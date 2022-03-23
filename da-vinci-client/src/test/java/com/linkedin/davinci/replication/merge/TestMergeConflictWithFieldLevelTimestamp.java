package com.linkedin.davinci.replication.merge;

import com.linkedin.davinci.replication.ReplicationMetadataWithValueSchemaId;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Collections;
import java.util.Optional;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.schema.rmd.ReplicationMetadataConstants.*;
import static org.mockito.Mockito.*;


public class TestMergeConflictWithFieldLevelTimestamp extends TestMergeConflictWithValueLevelTimestamp {

  @Test
  public void testNewPutIgnored() {
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);
    doReturn(new SchemaEntry(1, valueRecordSchemaV1)).when(schemaRepository).getValueSchema(storeName, 1);
    doReturn(new SchemaEntry(2, valueRecordSchemaV2)).when(schemaRepository).getValueSchema(storeName, 2);

    MergeConflictResolver mergeConflictResolver = new MergeConflictResolver(
        schemaRepository,
        storeName,
        valueSchemaID -> null
    );

    GenericRecord rmdRecord = new GenericData.Record(rmdSchemaV1);
    GenericRecord fieldTimestampsRecord = new GenericData.Record(rmdSchemaV1.getFields().get(0).schema().getTypes().get(1));
    fieldTimestampsRecord.put("id", 10L);
    fieldTimestampsRecord.put("name", 20L);
    fieldTimestampsRecord.put("age", 30L);
    rmdRecord.put(TIMESTAMP_FIELD_NAME, fieldTimestampsRecord);
    rmdRecord.put(REPLICATION_CHECKPOINT_VECTOR_FIELD, Collections.emptyList());
    final int oldValueSchemaID = 1;

    // Case 1: A new Put with timestamp that is strictly smaller than all existing fields' timestamps.
    MergeConflictResult mergeResult = mergeConflictResolver.put(
        Lazy.of(() -> null),
        Optional.of(new ReplicationMetadataWithValueSchemaId(oldValueSchemaID, rmdRecord)),
        null,
        9L,
        1, // Same as the old value schema ID.
        1L,
        0,
        0
    );
    Assert.assertTrue(mergeResult.isUpdateIgnored());

    // Case 2: A new Put with timestamp that is NOT strictly smaller than all existing fields' timestamps.
    // Currently the new-Put-not-ignored case has not been handled yet. So, we expect this exception to be a signal that
    // the new Put is not ignored.
    Assert.assertThrows(VeniceUnsupportedOperationException.class,() -> mergeConflictResolver.put(
        Lazy.of(() -> null),
        Optional.of(new ReplicationMetadataWithValueSchemaId(oldValueSchemaID, rmdRecord)),
        null,
        11L,
        1, // same as the old value schema ID.
        1L,
        0,
        0
    ));

    // Case 3: A new Put with more fields than existing value and new timestamp is smaller than all existing timestamps.
    Assert.assertThrows(VeniceUnsupportedOperationException.class,() -> mergeConflictResolver.put(
        Lazy.of(() -> null),
        Optional.of(new ReplicationMetadataWithValueSchemaId(oldValueSchemaID, rmdRecord)),
        null,
        8L,
        2, // Greater than the old value schema ID.
        1L,
        0,
        0
    ));

    // Case 4: A new Put with more fields than existing value and new timestamp is greater than all existing timestamps.
    Assert.assertThrows(VeniceUnsupportedOperationException.class,() -> mergeConflictResolver.put(
        Lazy.of(() -> null),
        Optional.of(new ReplicationMetadataWithValueSchemaId(oldValueSchemaID, rmdRecord)),
        null,
        100L,
        2, // Greater than the old value schema ID.
        1L,
        0,
        0
    ));
  }
}
