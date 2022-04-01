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
import com.linkedin.davinci.writecompute.CollectionTimestampBuilder;
import com.linkedin.venice.schema.merge.AvroCollectionElementComparator;
import com.linkedin.venice.schema.merge.SortBasedCollectionFieldOpHandler;
import com.linkedin.venice.schema.rmd.ReplicationMetadataSchemaGenerator;
import com.linkedin.venice.schema.rmd.v1.CollectionReplicationMetadata;
import com.linkedin.venice.utils.IndexedHashMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

// TODO: add MORE test cases to cover a broader range of scenarios.
public class SortBasedCollectionFieldOperationHandlerTest {
  private static final Logger logger = LogManager.getLogger(SortBasedCollectionFieldOperationHandlerTest.class);

  /**
   * A schema that contains a list field.
   */
  private static final Schema VALUE_SCHEMA = AvroCompatibilityHelper.parse(
      "{"
      + "   \"type\" : \"record\","
      + "   \"namespace\" : \"com.linkedin.avro\","
      + "   \"name\" : \"TestRecord\","
      + "   \"fields\" : ["
      + "      { \"name\" : \"Items\" , \"type\" : {\"type\" : \"array\", \"items\" : \"int\"}, \"default\" : [] },"
      + "      { \"name\" : \"PetNameToAge\" , \"type\" : [\"null\" , {\"type\" : \"map\", \"values\" : \"int\"}], \"default\" : null }"
      + "   ]"
      + "}"
  );
  private static final String LIST_FIELD_NAME = "Items";
  private static final String MAP_FIELD_NAME = "PetNameToAge";
  private static final Schema RMD_TIMESTAMP_SCHEMA;

  static {
    Schema rmdSchema = ReplicationMetadataSchemaGenerator.generateMetadataSchema(VALUE_SCHEMA);
    RMD_TIMESTAMP_SCHEMA = rmdSchema.getField("timestamp").schema().getTypes().get(1);
  }
  private static final int COLO_ID_1 = 1;
  private static final int COLO_ID_2 = 2;

  @Test
  public void testHandleListOpsCase1() {
    /**
     * A classic example:
     *
     *  - Event 1, at T1, in DC1, put {3, 2, 1}
     *  - Event 2, at T1, in DC2, put {2, 3, 4}
     *  - Event 3, at T2, in DC1, collection merging operation to add 5
     *  - Event 4, at T3, in DC2, collection merging operation to remove 3
     */
    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new PutListOperation(3L, COLO_ID_1, Arrays.asList(3, 2, 1), LIST_FIELD_NAME),
        new MergeListOperation(6L, COLO_ID_1, Collections.singletonList(5), Collections.emptyList(), LIST_FIELD_NAME),
        new PutListOperation(3L, COLO_ID_2, Arrays.asList(2, 3, 4), LIST_FIELD_NAME),
        new MergeListOperation(7L, COLO_ID_2, Collections.emptyList(), Collections.singletonList(3), LIST_FIELD_NAME)
    );
    List<Integer> expectedItemsResult = Arrays.asList(2, 4, 5);
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
  }

  @Test
  public void testHandleListOpsCase2() {
    /**
     * A classic example:
     *
     *  - Event 1, at T1, in DC1, put {2, 3, 4}
     *  - Event 2, at T1, in DC2, put {3, 2, 1}
     *  - Event 3, at T2, in DC1, collection merging operation to add 5
     *  - Event 4, at T3, in DC2, collection merging operation to remove 3
     */
    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new PutListOperation(3L, COLO_ID_1, Arrays.asList(2, 3, 4), LIST_FIELD_NAME),
        new MergeListOperation(6L, COLO_ID_1, Collections.singletonList(5), Collections.emptyList(), LIST_FIELD_NAME),
        new PutListOperation(3L, COLO_ID_2, Arrays.asList(3, 2, 1), LIST_FIELD_NAME),
        new MergeListOperation(7L, COLO_ID_2, Collections.emptyList(), Collections.singletonList(3), LIST_FIELD_NAME)
    );
    List<Integer> expectedItemsResult = Arrays.asList(2, 1, 5);
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
  }

  @Test
  public void testHandleListOpsCase3() {
    /**
     * Many add-to-list operations after initial put:
     *
     *  - Event 1, at T1, in DC1, put {2, 3, 4}
     *  - Event 2, at T1, in DC2, put {3, 2, 1}
     *  - Event 3, at T2, in DC1, collection merging operation to add 5
     *  - Event 4, at T3, in DC2, collection merging operation to add 6
     *  - Event 5, at T4, in DC1, collection merging operation to add 7
     *  - Event 6, at T5, in DC2, collection merging operation to add 8
     *  - Event 7, at T6, in DC1, collection merging operation to add 6
     */
    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new PutListOperation(3L, COLO_ID_1, Arrays.asList(2, 3, 4), LIST_FIELD_NAME),
        new PutListOperation(3L, COLO_ID_2, Arrays.asList(3, 2, 1), LIST_FIELD_NAME),
        new MergeListOperation(6L, COLO_ID_1, Collections.singletonList(5), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(7L, COLO_ID_2, Collections.singletonList(6), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(8L, COLO_ID_1, Collections.singletonList(7), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(9L, COLO_ID_2, Collections.singletonList(8), Collections.emptyList(), LIST_FIELD_NAME),
        // This operation should move element 5 to the end of the list.
        new MergeListOperation(10L, COLO_ID_1, Collections.singletonList(5), Collections.emptyList(), LIST_FIELD_NAME)
    );
    List<Integer> expectedItemsResult = Arrays.asList(3, 2, 1, 6, 7, 8, 5);
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
  }

  @Test
  public void testHandleListOpsCase4() {
    /**
     * Many remove-from-list operations after initial put:
     *
     *  - Event 1, at T1, in DC1, put {2, 3, 4}
     *  - Event 2, at T1, in DC2, put {3, 2, 1}
     *  - Event 3, at T2, in DC1, collection merging operation to remove 2
     *  - Event 4, at T3, in DC2, collection merging operation to remove 3
     *  - Event 5, at T4, in DC1, collection merging operation to remove 1
     */
    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new PutListOperation(3L, COLO_ID_1, Arrays.asList(2, 3, 4), LIST_FIELD_NAME),
        new PutListOperation(3L, COLO_ID_2, Arrays.asList(3, 2, 1), LIST_FIELD_NAME),
        new MergeListOperation(6L, COLO_ID_1, Collections.emptyList(), Collections.singletonList(2), LIST_FIELD_NAME),
        new MergeListOperation(7L, COLO_ID_2, Collections.emptyList(), Collections.singletonList(3), LIST_FIELD_NAME),
        new MergeListOperation(8L, COLO_ID_1, Collections.emptyList(), Collections.singletonList(1), LIST_FIELD_NAME)
    );
    List<Integer> expectedItemsResult = Collections.emptyList();
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
  }

  @Test
  public void testHandleListOpsCase5() {
    /**
     * All operations are put or delete:
     *
     *  - Event 1, at T1, in DC1, put {1, 2}
     *  - Event 2, at T1, in DC2, put {3, 4}
     *  - Event 3, at T2, in DC1, put {5, 6}
     *  - Event 4, at T3, in DC2, put {7, 8}
     *  - Event 3, at T3, in DC1, delete
     */
    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new PutListOperation(3L, COLO_ID_1, Arrays.asList(1, 2), LIST_FIELD_NAME),
        new PutListOperation(4L, COLO_ID_1, Arrays.asList(5, 6), LIST_FIELD_NAME),
        new DeleteListOperation(5L, COLO_ID_1, LIST_FIELD_NAME),
        new PutListOperation(3L, COLO_ID_2, Arrays.asList(3, 4), LIST_FIELD_NAME),
        new PutListOperation(5L, COLO_ID_2, Arrays.asList(7, 8), LIST_FIELD_NAME)
    );
    List<Integer> expectedItemsResult = Arrays.asList(7, 8);
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
  }

  @Test
  public void testHandleListOpsCase6() {
    /**
     * All operations are collection merge (e.g. add to list / delete from list):
     *
     *  - Event 1, at T1, in DC1, add 1 to list
     *  - Event 2, at T1, in DC2, add 2 to list
     *  - Event 3, at T2, in DC1, add 3 to list
     *  - Event 4, at T3, in DC2, add 4 to list
     *  - Event 5, at T4, in DC1, add 5 to list
     *  - Event 6, at T4, in DC2, remove 5 to list
     *  - Event 7, at T5, in DC1, add 6 to list
     */
    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new MergeListOperation(3L, COLO_ID_1, Collections.singletonList(1), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(4L, COLO_ID_1, Collections.singletonList(3), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(6L, COLO_ID_1, Collections.singletonList(5), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(7L, COLO_ID_1, Collections.singletonList(6), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(3L, COLO_ID_2, Collections.singletonList(2), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(5L, COLO_ID_2, Collections.singletonList(4), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(6L, COLO_ID_2, Collections.emptyList(), Collections.singletonList(5), LIST_FIELD_NAME)
    );
    List<Integer> expectedItemsResult = Arrays.asList(1, 2, 3, 4, 6);
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
  }

  @Test
  public void testHandleListOpsCase7() {
    /**
     * Put overrides collection-merge-only value:
     *
     *  - Event 1, at T1, in DC1, add 1 to list
     *  - Event 2, at T1, in DC2, add 2 to list
     *  - Event 3, at T2, in DC1, add 3 to list
     *  - Event 4, at T3, in DC2, add 4 to list
     *  - Event 5, at T4, in DC1, add 5 to list
     *  - Event 6, at T4, in DC2, put {7, 8, 9}
     */
    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new MergeListOperation(3L, COLO_ID_1, Collections.singletonList(1), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(4L, COLO_ID_1, Collections.singletonList(3), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(6L, COLO_ID_1, Collections.singletonList(5), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(3L, COLO_ID_2, Collections.singletonList(2), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(5L, COLO_ID_2, Collections.singletonList(4), Collections.emptyList(), LIST_FIELD_NAME),
        new PutListOperation(6L, COLO_ID_2, Arrays.asList(7, 8, 9), LIST_FIELD_NAME)
    );
    List<Integer> expectedItemsResult = Arrays.asList(7, 8, 9);
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
  }

  @Test
  public void testHandleListOpsCase8() {
    /**
     * Delete overrides collection-merge-only value:
     *
     *  - Event 1, at T1, in DC1, add 1 to list
     *  - Event 2, at T1, in DC2, add 2 to list
     *  - Event 3, at T2, in DC1, add 3 to list
     *  - Event 4, at T3, in DC2, add 4 to list
     *  - Event 5, at T4, in DC1, add 5 to list
     *  - Event 6, at T4, in DC2, delete
     */
    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new MergeListOperation(3L, COLO_ID_1, Collections.singletonList(1), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(4L, COLO_ID_1, Collections.singletonList(3), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(6L, COLO_ID_1, Collections.singletonList(5), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(3L, COLO_ID_2, Collections.singletonList(2), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(5L, COLO_ID_2, Collections.singletonList(4), Collections.emptyList(), LIST_FIELD_NAME),
        new DeleteListOperation(6L, COLO_ID_2, LIST_FIELD_NAME)
    );
    List<Integer> expectedItemsResult = Collections.emptyList();
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
  }

  @Test
  public void testHandleListOpsCase9() {
    /**
     * Delete partially overrides collection-merge-only value:
     *
     *  - Event 1, at T1, in DC1, add 1 to list
     *  - Event 2, at T1, in DC2, add 2 to list
     *  - Event 3, at T2, in DC1, add 3 to list
     *  - Event 4, at T3, in DC2, add 4 to list
     *  - Event 5, at T5, in DC1, add 5 to list
     *  - Event 6, at T4, in DC2, delete
     */
    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new MergeListOperation(3L, COLO_ID_1, Collections.singletonList(1), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(4L, COLO_ID_1, Collections.singletonList(3), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(7L, COLO_ID_1, Collections.singletonList(5), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(3L, COLO_ID_2, Collections.singletonList(2), Collections.emptyList(), LIST_FIELD_NAME),
        new MergeListOperation(5L, COLO_ID_2, Collections.singletonList(4), Collections.emptyList(), LIST_FIELD_NAME),
        new DeleteListOperation(6L, COLO_ID_2, LIST_FIELD_NAME)
    );
    List<Integer> expectedItemsResult = Collections.singletonList(5);
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
  }

  @Test
  public void testHandleListOpsCase10() {
    /**
     * Put partially overrides collection-merge-only value:
     *
     *  - Event 1, at T1, in DC1, delete 1 from list
     *  - Event 2, at T1, in DC2, delete 2 from list
     *  - Event 3, at T2, in DC1, delete 3 from list
     *  - Event 4, at T3, in DC2, put {1, 2, 3, 4, 5}
     *  - Event 5, at T4, in DC2, delete 4 from list
     *  - Event 6, at T5, in DC1, delete 5 from list
     */
    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new MergeListOperation(3L, COLO_ID_1, Collections.emptyList(), Collections.singletonList(1), LIST_FIELD_NAME),
        new MergeListOperation(4L, COLO_ID_1, Collections.emptyList(), Collections.singletonList(3), LIST_FIELD_NAME),
        new MergeListOperation(7L, COLO_ID_1, Collections.emptyList(), Collections.singletonList(5), LIST_FIELD_NAME),
        new MergeListOperation(3L, COLO_ID_2, Collections.emptyList(), Collections.singletonList(2), LIST_FIELD_NAME),
        new PutListOperation(5L, COLO_ID_2, Arrays.asList(1, 2, 3, 4, 5), LIST_FIELD_NAME),
        new MergeListOperation(6L, COLO_ID_2, Collections.emptyList(), Collections.singletonList(4), LIST_FIELD_NAME)
    );
    List<Integer> expectedItemsResult = Arrays.asList(1, 2, 3);
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
  }

  @Test
  public void testHandleMapOpsCase1() {
    // Same colo and second Put wins.
    IndexedHashMap<String, Integer> map1 = new IndexedHashMap<>();
    map1.put("k1", 1);
    map1.put("k2", 2);
    IndexedHashMap<String, Integer> map2 = new IndexedHashMap<>();
    map2.put("k3", 3);
    map2.put("k4", 4);

    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new PutMapOperation(10L, COLO_ID_1, map1, MAP_FIELD_NAME),
        new PutMapOperation(12L, COLO_ID_1, map2, MAP_FIELD_NAME)
    );
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, map2));
  }

  @Test
  public void testHandleMapOpsCase2() {
    // Same colo and first Put wins.
    IndexedHashMap<String, Integer> map1 = new IndexedHashMap<>();
    map1.put("k1", 1);
    map1.put("k2", 2);
    IndexedHashMap<String, Integer> map2 = new IndexedHashMap<>();
    map2.put("k3", 3);
    map2.put("k4", 4);

    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new PutMapOperation(13L, COLO_ID_1, map1, MAP_FIELD_NAME),
        new PutMapOperation(12L, COLO_ID_1, map2, MAP_FIELD_NAME)
    );
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, map1));
  }

  @Test
  public void testHandleMapOpsCase3() {
    // Same colo and second delete wins.
    IndexedHashMap<String, Integer> map1 = new IndexedHashMap<>();
    map1.put("k1", 1);
    map1.put("k2", 2);

    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new PutMapOperation(10L, COLO_ID_1, map1, MAP_FIELD_NAME),
        new DeleteMapOperation(12L, COLO_ID_1, MAP_FIELD_NAME) // Delete has a higher timestamp.
    );
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, Collections.emptyMap()));
  }

  @Test
  public void testHandleMapOpsCase4() {
    /**
     * Put Map from two colos with the same timestamp.
     *
     *  - Event 1, at T1, in DC1, put map1
     *  - Event 2, at T1, in DC2, put map2
     *
     *  Expectation: map2 wins because it has a higher colo ID (colo_ID_2 > colo_ID_1).
     */
    IndexedHashMap<String, Integer> map1 = new IndexedHashMap<>();
    map1.put("k1", 1);
    map1.put("k2", 2);
    IndexedHashMap<String, Integer> map2 = new IndexedHashMap<>();
    map2.put("k3", 3);
    map2.put("k4", 4);

    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new PutMapOperation(10L, COLO_ID_1, map1, MAP_FIELD_NAME),
        new PutMapOperation(10L, COLO_ID_2, map2, MAP_FIELD_NAME)
    );
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, map2));
  }

  @Test
  public void testHandleMapOpsCase5() {
    /**
     * Put Map from two colos with the different timestamps.
     *
     *  - Event 1, at T2, in DC1, put map1
     *  - Event 2, at T1, in DC2, put map2
     *
     *  Expectation: map1 wins because it has a higher timestamp (T2 > T1).
     */
    IndexedHashMap<String, Integer> map1 = new IndexedHashMap<>();
    map1.put("k1", 1);
    map1.put("k2", 2);
    IndexedHashMap<String, Integer> map2 = new IndexedHashMap<>();
    map2.put("k3", 3);
    map2.put("k4", 4);

    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new PutMapOperation(12L, COLO_ID_1, map1, MAP_FIELD_NAME),
        new PutMapOperation(11L, COLO_ID_2, map2, MAP_FIELD_NAME)
    );
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, map1));
  }

  @Test
  public void testHandleMapOpsCase6() {
    /**
     * Put Map and Delete Map from two colos with the same timestamps.
     *
     *  - Event 1, at T1, in DC1, put map1 delete map
     *  - Event 2, at T1, in DC2, delete map
     *
     *  Expectation: Delete wins because it has a higher colo ID (colo_ID_2 > colo_ID_1).
     */
    IndexedHashMap<String, Integer> map1 = new IndexedHashMap<>();
    map1.put("k1", 1);
    map1.put("k2", 2);

    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new PutMapOperation(12L, COLO_ID_1, map1, MAP_FIELD_NAME),
        new DeleteMapOperation(12L, COLO_ID_2, MAP_FIELD_NAME)
    );
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, Collections.emptyMap()));
  }

  @Test
  public void testHandleMapOpsCase7() {
    /**
     * Put Map and Delete Map from two colos with the different timestamps and Put Map wins.
     *
     *  - Event 1, at T1, in DC1, delete map
     *  - Event 2, at T2, in DC2, put map1
     */
    IndexedHashMap<String, Integer> map1 = new IndexedHashMap<>();
    map1.put("k1", 1);
    map1.put("k2", 2);

    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new DeleteMapOperation(10L, COLO_ID_1, MAP_FIELD_NAME),
        new PutMapOperation(12L, COLO_ID_2, map1, MAP_FIELD_NAME)
    );
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, map1));
  }

  @Test
  public void testHandleMapOpsCase8() {
    /**
     * Put Map and Delete Map from two colos with the different timestamps and Delete Map wins.
     *
     *  - Event 1, at T2, in DC1, delete map
     *  - Event 2, at T1, in DC2, put map1
     */
    IndexedHashMap<String, Integer> map1 = new IndexedHashMap<>();
    map1.put("k1", 1);
    map1.put("k2", 2);

    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new DeleteMapOperation(12L, COLO_ID_1, MAP_FIELD_NAME),
        new PutMapOperation(10L, COLO_ID_2, map1, MAP_FIELD_NAME)
    );
    applyAllOperationsOnValue(allCollectionOps, ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, Collections.emptyMap()));
  }

  private void applyAllOperationsOnValue(List<CollectionOperation> allCollectionOps, ExpectedCollectionResults<?, ?> expectedCollectionResults) {
    CollectionOperationSequenceBuilder builder = new CollectionOperationSequenceBuilder();
    for (CollectionOperation collectionOperation : allCollectionOps) {
      builder.addOperation(collectionOperation);
    }
    List<List<CollectionOperation>> allOpSequences = builder.build();
    logger.info("All operation sequences: " + allOpSequences);

    GenericRecord currValueRecord = new GenericData.Record(VALUE_SCHEMA);
    CollectionTimestampBuilder collectionTimestampBuilder = new CollectionTimestampBuilder(Schema.create(Schema.Type.LONG));
    collectionTimestampBuilder.setTopLevelColoID(1);
    collectionTimestampBuilder.setPutOnlyPartLength(0);
    collectionTimestampBuilder.setTopLevelTimestamps(0);
    collectionTimestampBuilder.setActiveElementsTimestamps(new LinkedList<>());
    collectionTimestampBuilder.setDeletedElementTimestamps(new LinkedList<>());
    collectionTimestampBuilder.setDeletedElements(Schema.create(Schema.Type.LONG), new LinkedList<>());
    collectionTimestampBuilder.setCollectionTimestampSchema(RMD_TIMESTAMP_SCHEMA.getField(LIST_FIELD_NAME).schema());
    CollectionReplicationMetadata collectionMetadata = new CollectionReplicationMetadata(collectionTimestampBuilder.build());
    SortBasedCollectionFieldOpHandler handler =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);

    GenericRecord prevValueRecord = null;
    CollectionReplicationMetadata prevCollectionRmd = null;

    for (int i = 0; i < allOpSequences.size(); i++) {
      GenericRecord currValueRecordCopy = GenericData.get().deepCopy(VALUE_SCHEMA, currValueRecord);
      CollectionReplicationMetadata collectionRmdCopy = new CollectionReplicationMetadata(collectionMetadata);
      List<CollectionOperation> opSequence = allOpSequences.get(i);
      logger.info("Applying operation sequence: " + opSequence);

      for (CollectionOperation op : opSequence) {
        applyOperationOnValue(op, collectionRmdCopy, handler, currValueRecordCopy);
      }
      logger.info("Post-merge value record: " + currValueRecordCopy);
      final String listFieldName = expectedCollectionResults.getListFieldName();
      final String mapFieldName = expectedCollectionResults.getMapFieldName();

      if (prevValueRecord == null) { // After applying the first sequence of operations, there is no prev value yet.
        if (listFieldName != null) {
          Assert.assertEquals(currValueRecordCopy.get(listFieldName), expectedCollectionResults.getExpectedList());
        }
        if (mapFieldName != null) {
          Assert.assertEquals(currValueRecordCopy.get(mapFieldName), expectedCollectionResults.getExpectedMap());
        }
        prevValueRecord = currValueRecordCopy;
        prevCollectionRmd = collectionRmdCopy;

      } else {
        if (listFieldName != null) {
          if (GenericData.get().compare(currValueRecordCopy.get(listFieldName), prevValueRecord.get(listFieldName), VALUE_SCHEMA.getField(listFieldName).schema()) != 0) {
            Assert.fail(String.format("Current value record is different from the previous value record. "
                + "Current: [%s] Previous: [%s]", currValueRecordCopy, prevValueRecord));
          }
        }
        if (mapFieldName != null) {
          if (AvroCollectionElementComparator.INSTANCE.compare(currValueRecordCopy.get(mapFieldName), prevValueRecord.get(mapFieldName), VALUE_SCHEMA.getField(mapFieldName).schema()) != 0) {
            Assert.fail(String.format("Current value record is different from the previous value record. "
                + "Current: [%s] Previous: [%s]", currValueRecordCopy, prevValueRecord));
          }
        }

        if (!prevCollectionRmd.equals(collectionRmdCopy)) {
          Assert.fail(String.format("Current RMD is %s and previous RMD is %s", collectionRmdCopy, prevCollectionRmd));
        }
      }
    }
  }

  private void applyOperationOnValue(
      CollectionOperation op,
      CollectionReplicationMetadata collectionMetadata,
      SortBasedCollectionFieldOpHandler handler,
      GenericRecord currValueRecord
  ) {
    if (op instanceof PutListOperation) {
      handler.handlePutList(
          op.getOpTimestamp(),
          op.getOpColoID(),
          ((PutListOperation) op).getNewList(),
          (CollectionReplicationMetadata<Object>) collectionMetadata,
          currValueRecord,
          op.getFieldName()
      );

    } else if (op instanceof PutMapOperation) {
      handler.handlePutMap(
          op.getOpTimestamp(),
          op.getOpColoID(),
          ((PutMapOperation) op).getNewMap(),
          (CollectionReplicationMetadata<String>) collectionMetadata,
          currValueRecord,
          op.getFieldName()
      );

    } else if (op instanceof MergeListOperation) {
      handler.handleModifyList(
          op.getOpTimestamp(),
          (CollectionReplicationMetadata<Object>) collectionMetadata,
          currValueRecord,
          op.getFieldName(),
          ((MergeListOperation) op).getNewElements(),
          ((MergeListOperation) op).getToRemoveElements()
      );

    } else if (op instanceof MergeMapOperation) {
      handler.handleModifyMap(
          op.getOpTimestamp(),
          (CollectionReplicationMetadata<String>) collectionMetadata,
          currValueRecord,
          op.getFieldName(),
          ((MergeMapOperation) op).getNewEntries(),
          ((MergeMapOperation) op).getToRemoveKeys()
      );

    } else if (op instanceof DeleteListOperation) {
      handler.handleDeleteList(
          op.getOpTimestamp(),
          op.getOpColoID(),
          (CollectionReplicationMetadata<Object>) collectionMetadata,
          currValueRecord,
          op.getFieldName()
      );

    } else if (op instanceof DeleteMapOperation) {
      handler.handleDeleteMap(
          op.getOpTimestamp(),
          op.getOpColoID(),
          (CollectionReplicationMetadata<String>) collectionMetadata,
          currValueRecord,
          op.getFieldName()
      );
    } else {
      throw new IllegalStateException("Unknown operation type: Got: " + op.getClass());
    }
  }
}
