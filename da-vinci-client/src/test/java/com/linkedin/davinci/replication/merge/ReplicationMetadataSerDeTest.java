package com.linkedin.davinci.replication.merge;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.replication.ReplicationMetadataWithValueSchemaId;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaEntry;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaGenerator;
import com.linkedin.venice.schema.rmd.v1.CollectionReplicationMetadata;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class ReplicationMetadataSerDeTest {


  /**
   * A schema that contains primitive fields and collection fields, specifically, a list field and a map field.
   */
  private static final String VALUE_SCHEMA_STR = "{"
      + "   \"type\" : \"record\","
      + "   \"namespace\" : \"com.linkedin.avro\","
      + "   \"name\" : \"Person\","
      + "   \"fields\" : ["
      + "      { \"name\" : \"Name\" , \"type\" : \"string\", \"default\" : \"unknown\" },"
      + "      { \"name\" : \"Age\" , \"type\" : \"int\", \"default\" : -1 },"
      + "      { \"name\" : \"Items\" , \"type\" : {\"type\" : \"array\", \"items\" : \"string\"}, \"default\" : [] },"
      + "      { \"name\" : \"PetNameToAge\" , \"type\" : [\"null\" , {\"type\" : \"map\", \"values\" : \"int\"}], \"default\" : null }"
      + "   ]"
      + "}";

  @Test
  public void testSerDeRmd() {
    final int valueSchemaID = 1214;
    final int rmdVersionID = 1;
    final String storeName = "test_store_name";
    // Generate RMD schema and record from value schema.
    Schema valueSchema = AvroCompatibilityHelper.parse(VALUE_SCHEMA_STR);
    Schema rmdSchema = ReplicationMetadataSchemaGenerator.generateMetadataSchema(valueSchema);
    GenericRecord rmd = createRmdWithCollectionTimestamp(rmdSchema);

    // Prepare the object under test, mocks, etc
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);
    ReplicationMetadataSchemaEntry rmdSchemaEntry = mock(ReplicationMetadataSchemaEntry.class);
    Mockito.doReturn(rmdSchema).when(rmdSchemaEntry).getSchema();
    Mockito.doReturn(rmdSchemaEntry).when(schemaRepository).getReplicationMetadataSchema(storeName, valueSchemaID, rmdVersionID);
    ReplicationMetadataSerDe rmdSerDe = new ReplicationMetadataSerDe(schemaRepository, storeName, rmdVersionID);

    // Serialize this RMD record to bytes.
    Schema actualRmdSchema = rmdSerDe.getReplicationMetadataSchema(valueSchemaID);
    Assert.assertEquals(actualRmdSchema, rmdSchema);
    ByteBuffer rmdBytes = rmdSerDe.serializeReplicationMetadata(valueSchemaID, rmd);

    // Prepend value schema ID to RMD bytes.
    ByteBuffer rmdAndValueSchemaIDBytes = ByteBuffer.allocate(Integer.BYTES + rmdBytes.remaining());
    rmdAndValueSchemaIDBytes.putInt(valueSchemaID);
    rmdAndValueSchemaIDBytes.put(rmdBytes.array());

    // Deserialize all bytes and expect to get value schema ID and RMD record back.
    ReplicationMetadataWithValueSchemaId rmdAndValueID = rmdSerDe.deserializeValueSchemaIdPrependedRmdBytes(rmdAndValueSchemaIDBytes.array());
    Assert.assertEquals(rmdAndValueID.getValueSchemaId(), valueSchemaID);
    Assert.assertEquals(rmdAndValueID.getReplicationMetadataRecord(), rmd);
  }

  private GenericRecord createRmdWithCollectionTimestamp(Schema rmdSchema) {
    Schema rmdTimestampSchema = rmdSchema.getField("timestamp").schema().getTypes().get(1);
    GenericRecord rmdTimestamp = new GenericData.Record(rmdTimestampSchema);
    rmdTimestamp.put("Name", 1L);
    rmdTimestamp.put("Age", 1L);
    rmdTimestamp.put("Items", createCollectionFieldMetadataRecord(
        rmdTimestampSchema.getField("Items").schema(),
        23L,
        1,
        3,
        Arrays.asList(1L, 2L, 3L),
        Arrays.asList("foo", "bar"),
        Arrays.asList(1L, 100L)
    ));
    rmdTimestamp.put("PetNameToAge", createCollectionFieldMetadataRecord(
        rmdTimestampSchema.getField("PetNameToAge").schema(),
        24L,
        2,
        5,
        Arrays.asList(1L, 2L, 3L, 4L, 5L),
        Arrays.asList("foo", "bar", "qaz"),
        Arrays.asList(1L, 2L, 3L)
    ));
    GenericRecord rmd = new GenericData.Record(rmdSchema);
    rmd.put("timestamp", rmdTimestamp);
    rmd.put("replication_checkpoint_vector", Arrays.asList(1L, 2L, 3L));
    return rmd;
  }

  private GenericRecord createCollectionFieldMetadataRecord(
      Schema collectionFieldMetadataSchema,
      long topLevelTimestamp,
      int topLevelColoID,
      int putOnlyPartLen,
      List<Long> activeElementsTimestamps,
      List<Object> deletedElements,
      List<Long> deletedElementsTimestamps
  ) {
    GenericRecord collectionFieldMetadataRecord = new GenericData.Record(collectionFieldMetadataSchema);
    collectionFieldMetadataRecord.put(
        CollectionReplicationMetadata.COLLECTION_TOP_LEVEL_TS_FIELD_NAME,
        topLevelTimestamp
    );
    collectionFieldMetadataRecord.put(
        CollectionReplicationMetadata.COLLECTION_TOP_LEVEL_TS_FIELD_NAME,
        topLevelTimestamp
    );
    collectionFieldMetadataRecord.put(
        CollectionReplicationMetadata.COLLECTION_TOP_LEVEL_COLO_ID_FIELD_NAME,
        topLevelColoID
    );
    collectionFieldMetadataRecord.put(
        CollectionReplicationMetadata.COLLECTION_PUT_ONLY_PART_LENGTH_FIELD_NAME,
        putOnlyPartLen
    );
    collectionFieldMetadataRecord.put(
        CollectionReplicationMetadata.COLLECTION_ACTIVE_ELEM_TS_FIELD_NAME,
        activeElementsTimestamps
    );
    collectionFieldMetadataRecord.put(
        CollectionReplicationMetadata.COLLECTION_DELETED_ELEM_FIELD_NAME,
        deletedElements
    );
    collectionFieldMetadataRecord.put(
        CollectionReplicationMetadata.COLLECTION_DELETED_ELEM_TS_FIELD_NAME,
        deletedElementsTimestamps
    );
    return collectionFieldMetadataRecord;
  }
}
