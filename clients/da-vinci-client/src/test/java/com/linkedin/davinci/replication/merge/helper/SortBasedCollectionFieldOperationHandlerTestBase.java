package com.linkedin.davinci.replication.merge.helper;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.replication.merge.helper.utils.CollectionOperation;
import com.linkedin.davinci.replication.merge.helper.utils.CollectionOperationSequenceBuilder;
import com.linkedin.davinci.replication.merge.helper.utils.DeleteListOperation;
import com.linkedin.davinci.replication.merge.helper.utils.DeleteMapOperation;
import com.linkedin.davinci.replication.merge.helper.utils.ExpectedCollectionResults;
import com.linkedin.davinci.replication.merge.helper.utils.MergeListOperation;
import com.linkedin.davinci.replication.merge.helper.utils.MergeMapOperation;
import com.linkedin.davinci.replication.merge.helper.utils.PutListOperation;
import com.linkedin.davinci.replication.merge.helper.utils.PutMapOperation;
import com.linkedin.venice.schema.merge.AvroCollectionElementComparator;
import com.linkedin.venice.schema.merge.CollectionTimestampBuilder;
import com.linkedin.venice.schema.merge.SortBasedCollectionFieldOpHandler;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp;
import com.linkedin.venice.utils.IndexedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;


// TODO: add MORE test cases to cover a broader range of scenarios.
public class SortBasedCollectionFieldOperationHandlerTestBase {
  private static final Logger LOGGER = LogManager.getLogger(SortBasedCollectionFieldOperationHandlerTestBase.class);

  /**
   * A schema that contains a list field.
   */
  private static final Schema VALUE_SCHEMA = AvroCompatibilityHelper.parse(
      "{" + "   \"type\" : \"record\"," + "   \"namespace\" : \"com.linkedin.avro\"," + "   \"name\" : \"TestRecord\","
          + "   \"fields\" : ["
          + "      { \"name\" : \"Items\" , \"type\" : {\"type\" : \"array\", \"items\" : \"int\"}, \"default\" : [] },"
          + "      { \"name\" : \"PetNameToAge\" , \"type\" : [\"null\" , {\"type\" : \"map\", \"values\" : \"int\"}], \"default\" : null }"
          + "   ]" + "}");
  protected static final String LIST_FIELD_NAME = "Items";
  protected static final String MAP_FIELD_NAME = "PetNameToAge";
  private static final Schema RMD_TIMESTAMP_SCHEMA;

  static {
    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(VALUE_SCHEMA);
    RMD_TIMESTAMP_SCHEMA = rmdSchema.getField("timestamp").schema().getTypes().get(1);
  }
  protected static final int COLO_ID_1 = 1;
  protected static final int COLO_ID_2 = 2;
  protected static final int COLO_ID_3 = 3;
  protected static final int COLO_ID_4 = 4;
  protected static final int COLO_ID_5 = 5;
  protected static final int COLO_ID_6 = 6;

  protected void applyAllOperationsOnValue(
      List<CollectionOperation> allCollectionOps,
      ExpectedCollectionResults<?, ?> expectedCollectionResults) {
    CollectionOperationSequenceBuilder builder = new CollectionOperationSequenceBuilder();
    for (CollectionOperation collectionOperation: allCollectionOps) {
      builder.addOperation(collectionOperation);
    }
    List<List<CollectionOperation>> allOpSequences = builder.build();
    LOGGER.info("All operation sequences: {}", allOpSequences);

    GenericRecord currValueRecord = new GenericData.Record(VALUE_SCHEMA);
    CollectionTimestampBuilder collectionTimestampBuilder =
        new CollectionTimestampBuilder(Schema.create(Schema.Type.LONG));
    collectionTimestampBuilder.setTopLevelColoID(1);
    collectionTimestampBuilder.setPutOnlyPartLength(0);
    collectionTimestampBuilder.setTopLevelTimestamps(0);
    collectionTimestampBuilder.setActiveElementsTimestamps(new LinkedList<>());
    collectionTimestampBuilder.setDeletedElementTimestamps(new LinkedList<>());
    collectionTimestampBuilder.setDeletedElements(Schema.create(Schema.Type.LONG), new LinkedList<>());
    collectionTimestampBuilder.setCollectionTimestampSchema(RMD_TIMESTAMP_SCHEMA.getField(LIST_FIELD_NAME).schema());
    CollectionRmdTimestamp collectionMetadata = new CollectionRmdTimestamp(collectionTimestampBuilder.build());
    SortBasedCollectionFieldOpHandler handlerToTest =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);

    GenericRecord prevValueRecord = null;
    CollectionRmdTimestamp prevCollectionRmd = null;

    for (int i = 0; i < allOpSequences.size(); i++) {
      GenericRecord currValueRecordCopy = GenericData.get().deepCopy(VALUE_SCHEMA, currValueRecord);
      CollectionRmdTimestamp<?> collectionRmdCopy = new CollectionRmdTimestamp<>(collectionMetadata);
      List<CollectionOperation> opSequence = allOpSequences.get(i);
      LOGGER.info("Applying operation sequence: {}", opSequence);

      for (CollectionOperation op: opSequence) {
        applyOperationOnValue(op, collectionRmdCopy, handlerToTest, currValueRecordCopy);
      }
      LOGGER.info("Post-merge value record: {}", currValueRecordCopy);
      final String listFieldName = expectedCollectionResults.getListFieldName();
      final String mapFieldName = expectedCollectionResults.getMapFieldName();

      if (prevValueRecord == null) { // After applying the first sequence of operations, there is no prev value yet.
        if (listFieldName != null) {
          Assert.assertEquals(currValueRecordCopy.get(listFieldName), expectedCollectionResults.getExpectedList());
        }
        if (mapFieldName != null) {
          validateMapsEqual(
              (Map<String, ?>) currValueRecordCopy.get(mapFieldName),
              expectedCollectionResults.getExpectedMap());
        }
        prevValueRecord = currValueRecordCopy;
        prevCollectionRmd = collectionRmdCopy;

      } else {
        if (listFieldName != null) {
          if (GenericData.get()
              .compare(
                  currValueRecordCopy.get(listFieldName),
                  prevValueRecord.get(listFieldName),
                  VALUE_SCHEMA.getField(listFieldName).schema()) != 0) {
            Assert.fail(
                String.format(
                    "Current value record is different from the previous value record. "
                        + "Current: [%s] Previous: [%s]",
                    currValueRecordCopy,
                    prevValueRecord));
          }
        }
        if (mapFieldName != null) {
          validateMapsEqual(
              (Map<String, ?>) currValueRecordCopy.get(mapFieldName),
              (Map<String, ?>) prevValueRecord.get(mapFieldName));
        }

        if (!prevCollectionRmd.equals(collectionRmdCopy)) {
          Assert.fail(String.format("Current RMD is %s and previous RMD is %s", collectionRmdCopy, prevCollectionRmd));
        }
      }
    }
  }

  private void validateMapsEqual(Map<String, ?> m1, Map<String, ?> m2) {
    if (m1 instanceof IndexedHashMap && m2 instanceof IndexedHashMap) {
      if (!mapsEqual((IndexedHashMap<String, ?>) m1, (IndexedHashMap<String, ?>) m2)) {
        Assert.fail("Maps are different. Got: " + m1 + ", and " + m2);
      }
    } else {
      Assert.assertEquals(m1, m2);
    }
  }

  private boolean mapsEqual(IndexedHashMap<String, ?> m1, IndexedHashMap<String, ?> m2) {
    if (m1.size() != m2.size()) {
      return false;
    }
    for (int i = 0; i < m1.size(); i++) {
      Map.Entry<String, ?> entry1 = m1.getByIndex(i);
      Map.Entry<String, ?> entry2 = m2.getByIndex(i);
      if (!Objects.equals(entry1, entry2)) {
        return false;
      }
    }
    return true;
  }

  private void applyOperationOnValue(
      CollectionOperation op,
      CollectionRmdTimestamp collectionMetadata,
      SortBasedCollectionFieldOpHandler handlerToTest,
      GenericRecord currValueRecord) {
    if (op instanceof PutListOperation) {
      handlerToTest.handlePutList(
          op.getOpTimestamp(),
          op.getOpColoID(),
          ((PutListOperation) op).getNewList(),
          (CollectionRmdTimestamp<Object>) collectionMetadata,
          currValueRecord,
          op.getFieldName());

    } else if (op instanceof PutMapOperation) {
      handlerToTest.handlePutMap(
          op.getOpTimestamp(),
          op.getOpColoID(),
          ((PutMapOperation) op).getNewMap(),
          (CollectionRmdTimestamp<String>) collectionMetadata,
          currValueRecord,
          op.getFieldName());

    } else if (op instanceof MergeListOperation) {
      handlerToTest.handleModifyList(
          op.getOpTimestamp(),
          (CollectionRmdTimestamp<Object>) collectionMetadata,
          currValueRecord,
          op.getFieldName(),
          ((MergeListOperation) op).getNewElements(),
          ((MergeListOperation) op).getToRemoveElements());

    } else if (op instanceof MergeMapOperation) {
      handlerToTest.handleModifyMap(
          op.getOpTimestamp(),
          (CollectionRmdTimestamp<String>) collectionMetadata,
          currValueRecord,
          op.getFieldName(),
          ((MergeMapOperation) op).getNewEntries(),
          ((MergeMapOperation) op).getToRemoveKeys());

    } else if (op instanceof DeleteListOperation) {
      handlerToTest.handleDeleteList(
          op.getOpTimestamp(),
          op.getOpColoID(),
          (CollectionRmdTimestamp<Object>) collectionMetadata,
          currValueRecord,
          op.getFieldName());

    } else if (op instanceof DeleteMapOperation) {
      handlerToTest.handleDeleteMap(
          op.getOpTimestamp(),
          op.getOpColoID(),
          (CollectionRmdTimestamp<String>) collectionMetadata,
          currValueRecord,
          op.getFieldName());
    } else {
      throw new IllegalStateException("Unknown operation type: Got: " + op.getClass());
    }
  }
}
