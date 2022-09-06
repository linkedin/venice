package com.linkedin.davinci.replication.merge.helper;

import com.linkedin.davinci.replication.merge.helper.utils.CollectionOperation;
import com.linkedin.davinci.replication.merge.helper.utils.DeleteMapOperation;
import com.linkedin.davinci.replication.merge.helper.utils.ExpectedCollectionResults;
import com.linkedin.davinci.replication.merge.helper.utils.MergeMapOperation;
import com.linkedin.davinci.replication.merge.helper.utils.PutMapOperation;
import com.linkedin.venice.utils.IndexedHashMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class MapCollectionMergeTest extends SortBasedCollectionFieldOperationHandlerTestBase {
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
        new PutMapOperation(12L, COLO_ID_1, map2, MAP_FIELD_NAME));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, map2));
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
        new PutMapOperation(12L, COLO_ID_1, map2, MAP_FIELD_NAME));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, map1));
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
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, Collections.emptyMap()));
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
        new PutMapOperation(10L, COLO_ID_2, map2, MAP_FIELD_NAME));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, map2));
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
        new PutMapOperation(11L, COLO_ID_2, map2, MAP_FIELD_NAME));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, map1));
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
        new DeleteMapOperation(12L, COLO_ID_2, MAP_FIELD_NAME));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, Collections.emptyMap()));
  }

  @Test
  public void testHandleMapOpsCase7() {
    /**
     * Put Map and Delete Map from two colos with the different timestamps.
     *
     *  - Event 1, at T1, in DC1, delete map
     *  - Event 2, at T2, in DC2, put map1
     *
     *  Expectation: Put Map wins because it has a higher timestamp.
     */
    IndexedHashMap<String, Integer> map1 = new IndexedHashMap<>();
    map1.put("k1", 1);
    map1.put("k2", 2);

    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new DeleteMapOperation(10L, COLO_ID_1, MAP_FIELD_NAME),
        new PutMapOperation(12L, COLO_ID_2, map1, MAP_FIELD_NAME));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, map1));
  }

  @Test
  public void testHandleMapOpsCase8() {
    /**
     * Put Map and Delete Map from two colos with the different timestamps.
     *
     *  - Event 1, at T2, in DC1, delete map
     *  - Event 2, at T1, in DC2, put map1
     *
     *  Expectation: Delete Map wins.
     */
    IndexedHashMap<String, Integer> map1 = new IndexedHashMap<>();
    map1.put("k1", 1);
    map1.put("k2", 2);

    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new DeleteMapOperation(12L, COLO_ID_1, MAP_FIELD_NAME),
        new PutMapOperation(10L, COLO_ID_2, map1, MAP_FIELD_NAME));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, Collections.emptyMap()));
  }

  @Test
  public void testHandleMapOpsCase9() {
    /**
     * One add-to-map op and one remove-from-map op after initial Put:
     *
     *  - Event 1, at T1, in DC1, put {k1->1, k2->2}
     *  - Event 2, at T1, in DC2, put {k4->4, k3->3, k2->2}
     *  - Event 3, at T2, in DC1, collection merging operation to add K5->5
     *  - Event 4, at T3, in DC2, collection merging operation to remove key K3
     *
     *  Expect: {k4->4, k2->2, k5->5}
     *  Explanation:
     *    Entry k3->3 is removed and entry k5->5 is added. The order of the remaining part of the put-only part does not
     *    change.
     */
    List<CollectionOperation> allCollectionOps = Arrays.asList(
        new PutMapOperation(3L, COLO_ID_1, createMap(Arrays.asList("k1", "k2"), Arrays.asList(1, 2)), MAP_FIELD_NAME),
        new MergeMapOperation(
            6L,
            COLO_ID_1,
            createMap(Collections.singletonList("k5"), Collections.singletonList(5)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new PutMapOperation(
            3L,
            COLO_ID_2,
            createMap(Arrays.asList("k4", "k3", "k2"), Arrays.asList(4, 3, 2)),
            MAP_FIELD_NAME),
        new MergeMapOperation(7L, COLO_ID_2, Collections.emptyMap(), Collections.singletonList("k3"), MAP_FIELD_NAME));
    Map<String, Object> expectedMapResult = createMap(Arrays.asList("k4", "k2", "k5"), Arrays.asList(4, 2, 5));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, expectedMapResult));
  }

  @Test
  public void testHandleMapOpsCase10() {
    /**
     * Many add-to-map ops after initial Put:
     *
     *  - Event 1, at T1, in DC1, put {k2->2, k3->3, k4->4}
     *  - Event 2, at T2, in DC2, put {k1->1, k2->2}
     *  - Event 3, at T3, in DC1, collection merging operation to add k10->10
     *  - Event 4, at T4, in DC2, collection merging operation to add k9->9
     *  - Event 5, at T5, in DC1, collection merging operation to add k8->8
     *  - Event 6, at T6, in DC2, collection merging operation to add k7->7
     *  - Event 7, at T7, in DC1, collection merging operation to add k6->6
     *  - Event 8, at T8, in DC2, collection merging operation to add k5->5
     *
     *  Expect: {k1->1, k2->2, k10->10, k9->9, k8->8, k7->7, k6->6, k5->5}
     */
    List<CollectionOperation> allCollectionOps = getCollectionOpsForTestCase10();
    Map<String, Object> expectedMapResult = createMap(
        Arrays.asList("k1", "k2", "k10", "k9", "k8", "k7", "k6", "k5"),
        Arrays.asList(1, 2, 10, 9, 8, 7, 6, 5));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, expectedMapResult));
  }

  private List<CollectionOperation> getCollectionOpsForTestCase10() {
    return Arrays.asList(
        new PutMapOperation(
            1L,
            COLO_ID_1,
            createMap(Arrays.asList("k2", "k3", "k4"), Arrays.asList(2, 3, 4)),
            MAP_FIELD_NAME),
        new PutMapOperation(2L, COLO_ID_2, createMap(Arrays.asList("k1", "k2"), Arrays.asList(1, 2)), MAP_FIELD_NAME),
        new MergeMapOperation(
            3L,
            COLO_ID_1,
            createMap(Collections.singletonList("k10"), Collections.singletonList(10)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            4L,
            COLO_ID_2,
            createMap(Collections.singletonList("k9"), Collections.singletonList(9)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            5L,
            COLO_ID_1,
            createMap(Collections.singletonList("k8"), Collections.singletonList(8)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            6L,
            COLO_ID_2,
            createMap(Collections.singletonList("k7"), Collections.singletonList(7)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            7L,
            COLO_ID_1,
            createMap(Collections.singletonList("k6"), Collections.singletonList(6)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            8L,
            COLO_ID_2,
            createMap(Collections.singletonList("k5"), Collections.singletonList(5)),
            Collections.emptyList(),
            MAP_FIELD_NAME));
  }

  @Test
  public void testHandleMapOpsCase11() {
    /**
     * Many add-to-map ops (each from a different colo/fabric) with a same timestamp after initial Put:
     *
     *  - Event 1, at T1, in DC1, put {k2->2, k3->3, k4->4}
     *  - Event 2, at T2, in DC2, put {k1->1, k2->2}
     *  - Event 3, at T3, in DC1, collection merging operation to add k10->10
     *  - Event 4, at T3, in DC2, collection merging operation to add k9->9
     *  - Event 5, at T3, in DC3, collection merging operation to add k8->8
     *  - Event 6, at T3, in DC4, collection merging operation to add k7->7
     *  - Event 7, at T3, in DC5, collection merging operation to add k6->6
     *  - Event 8, at T3, in DC6, collection merging operation to add k5->5
     *
     *  Expect: {k1->1, k2->2, k10->10, k5->5, k6->6, k7->7, k8->8, k9->9}
     *  Explanation:
     *      For the collection-merge part, sort by value when timestamps are the same. Note that k10->10 is the first
     *      entry in the collection-merge part because lexicographically the string "k10" is smaller than "k5", ..., "k9"
     */
    List<CollectionOperation> allCollectionOps = getCollectionOpsForTestCase11();
    Map<String, Object> expectedMapResult = createMap(
        Arrays.asList("k1", "k2", "k10", "k5", "k6", "k7", "k8", "k9"),
        Arrays.asList(1, 2, 10, 5, 6, 7, 8, 9));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, expectedMapResult));
  }

  private List<CollectionOperation> getCollectionOpsForTestCase11() {
    return Arrays.asList(
        new PutMapOperation(
            1L,
            COLO_ID_1,
            createMap(Arrays.asList("k2", "k3", "k4"), Arrays.asList(2, 3, 4)),
            MAP_FIELD_NAME),
        new PutMapOperation(2L, COLO_ID_2, createMap(Arrays.asList("k1", "k2"), Arrays.asList(1, 2)), MAP_FIELD_NAME),
        new MergeMapOperation(
            3L,
            COLO_ID_1,
            createMap(Collections.singletonList("k10"), Collections.singletonList(10)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            3L,
            COLO_ID_2,
            createMap(Collections.singletonList("k9"), Collections.singletonList(9)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            3L,
            COLO_ID_3,
            createMap(Collections.singletonList("k8"), Collections.singletonList(8)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            3L,
            COLO_ID_4,
            createMap(Collections.singletonList("k7"), Collections.singletonList(7)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            3L,
            COLO_ID_5,
            createMap(Collections.singletonList("k6"), Collections.singletonList(6)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            3L,
            COLO_ID_6,
            createMap(Collections.singletonList("k5"), Collections.singletonList(5)),
            Collections.emptyList(),
            MAP_FIELD_NAME));
  }

  @Test
  public void testHandleMapOpsCase12() {
    /**
     * Many add-to-map ops to add existing k-v entries after initial Put:
     *
     *  - Event 1, at T1, in DC1, put {k2->2, k3->3, k4->4}
     *  - Event 2, at T2, in DC2, put {k9->9, k8->8, k7->7, k6->6}
     *  - Event 3, at T3, in DC1, collection merging operation to add k6->6
     *  - Event 4, at T4, in DC2, collection merging operation to add k7->7
     *  - Event 5, at T5, in DC1, collection merging operation to add k8->8
     *  - Event 6, at T6, in DC2, collection merging operation to add k9->9
     *
     *  Expect: {k6->6, k7->7, k8->8, k9->9}
     *  Explanation:
     *    At the end, all entries are in the collection-merge part. IOW, the length of the put-only part is 0. The order
     *    of these entries are determined by timestamps of when they are added.
     */
    List<CollectionOperation> allCollectionOps = getCollectionOpsForTestCase12();
    Map<String, Object> expectedMapResult = createMap(Arrays.asList("k6", "k7", "k8", "k9"), Arrays.asList(6, 7, 8, 9));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, expectedMapResult));
  }

  private List<CollectionOperation> getCollectionOpsForTestCase12() {
    return Arrays.asList(
        new PutMapOperation(
            1L,
            COLO_ID_1,
            createMap(Arrays.asList("k2", "k3", "k4"), Arrays.asList(2, 3, 4)),
            MAP_FIELD_NAME),
        new PutMapOperation(
            2L,
            COLO_ID_2,
            createMap(Arrays.asList("k9", "k8", "k7", "k6"), Arrays.asList(9, 8, 7, 6)),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            3L,
            COLO_ID_1,
            createMap(Collections.singletonList("k6"), Collections.singletonList(6)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            4L,
            COLO_ID_2,
            createMap(Collections.singletonList("k7"), Collections.singletonList(7)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            5L,
            COLO_ID_1,
            createMap(Collections.singletonList("k8"), Collections.singletonList(8)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            6L,
            COLO_ID_2,
            createMap(Collections.singletonList("k9"), Collections.singletonList(9)),
            Collections.emptyList(),
            MAP_FIELD_NAME));
  }

  @Test
  public void testHandleMapOpsCase13() {
    /**
     * Many remove-from-map ops to remove existing k-v entries after initial Put:
     *
     *  - Event 1, at T1, in DC1, put {k2->2, k3->3, k4->4}
     *  - Event 2, at T2, in DC2, put {k9->9, k8->8, k7->7, k6->6}
     *  - Event 3, at T3, in DC1, collection merging operation to remove k11 // // Remove a non-existing key
     *  - Event 4, at T4, in DC2, collection merging operation to remove k7
     *  - Event 5, at T5, in DC1, collection merging operation to remove k10 // Remove a non-existing key
     *  - Event 6, at T6, in DC2, collection merging operation to remove k9
     *
     *  Expect: {k8->8, k6->6}
     *  Explanation:
     *    All keys are removed except k8 and k6. The relative order between k8 and k6 remains the same.
     */
    List<CollectionOperation> allCollectionOps = getCollectionOpsForTestCase13();
    Map<String, Object> expectedMapResult = createMap(Arrays.asList("k8", "k6"), Arrays.asList(8, 6));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, expectedMapResult));
  }

  private List<CollectionOperation> getCollectionOpsForTestCase13() {
    return Arrays.asList(
        new PutMapOperation(
            1L,
            COLO_ID_1,
            createMap(Arrays.asList("k2", "k3", "k4"), Arrays.asList(2, 3, 4)),
            MAP_FIELD_NAME),
        new PutMapOperation(
            2L,
            COLO_ID_2,
            createMap(Arrays.asList("k9", "k8", "k7", "k6"), Arrays.asList(9, 8, 7, 6)),
            MAP_FIELD_NAME),
        new MergeMapOperation(3L, COLO_ID_1, Collections.emptyMap(), Collections.singletonList("k11"), MAP_FIELD_NAME),
        new MergeMapOperation(4L, COLO_ID_2, Collections.emptyMap(), Collections.singletonList("k7"), MAP_FIELD_NAME),
        new MergeMapOperation(5L, COLO_ID_1, Collections.emptyMap(), Collections.singletonList("k10"), MAP_FIELD_NAME),
        new MergeMapOperation(6L, COLO_ID_2, Collections.emptyMap(), Collections.singletonList("k9"), MAP_FIELD_NAME));
  }

  @Test
  public void testHandleMapOpsCase14() {
    /**
     * Many add-to-map ops first. Then there comes a Put with the lowest timestamp. So, Put is prepended at the front:
     *
     *  - Event 1, at T1, in DC1, collection merging operation to add k1->1
     *  - Event 2, at T1, in DC2, collection merging operation to add k2->2
     *  - Event 3, at T2, in DC1, collection merging operation to add k3->3
     *  - Event 4, at T2, in DC2, collection merging operation to add k4->4
     *  - Event 5, at T0, in DC1, put {k7->7, k8->8, k9->9}
     *
     *  Expect: {k7->7, k8->8, k9->9, k1->1, k2->2, k3->3, k4->4}
     *  Explanation:
     *    All entries are added by the collection-merge add ops and the Put is inserted at the front part as the put-only
     *    part. Note that collection merges with higher timestamps do NOT prevent Put from being applied.
     */
    List<CollectionOperation> allCollectionOps = getCollectionOpsForTestCase14();
    Map<String, Object> expectedMapResult =
        createMap(Arrays.asList("k7", "k8", "k9", "k1", "k2", "k3", "k4"), Arrays.asList(7, 8, 9, 1, 2, 3, 4));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, expectedMapResult));
  }

  private List<CollectionOperation> getCollectionOpsForTestCase14() {
    return Arrays.asList(
        new MergeMapOperation(
            1L,
            COLO_ID_1,
            createMap(Collections.singletonList("k1"), Collections.singletonList(1)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            1L,
            COLO_ID_2,
            createMap(Collections.singletonList("k2"), Collections.singletonList(2)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            2L,
            COLO_ID_1,
            createMap(Collections.singletonList("k3"), Collections.singletonList(3)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            2L,
            COLO_ID_2,
            createMap(Collections.singletonList("k4"), Collections.singletonList(4)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new PutMapOperation(
            0L,
            COLO_ID_1,
            createMap(Arrays.asList("k7", "k8", "k9"), Arrays.asList(7, 8, 9)),
            MAP_FIELD_NAME));
  }

  @Test
  public void testHandleMapOpsCase15() {
    /**
     * Many add-to-map ops first. Then there comes a Put with the highest timestamp. So, Put overwrites everything:
     *
     *  - Event 1, at T1, in DC1, collection merging operation to add k1->1
     *  - Event 2, at T1, in DC2, collection merging operation to add k2->2
     *  - Event 3, at T2, in DC1, collection merging operation to add k3->3
     *  - Event 4, at T2, in DC2, collection merging operation to add k4->4
     *  - Event 5, at T3, in DC1, put {k7->7, k8->8, k9->9}
     *
     *  Expect: {k7->7, k8->8, k9->9}
     *  Explanation:
     *    The last Put has the highest timestamp. So, it overwrites everything else.
     */
    List<CollectionOperation> allCollectionOps = getCollectionOpsForTestCase15();
    Map<String, Object> expectedMapResult = createMap(Arrays.asList("k7", "k8", "k9"), Arrays.asList(7, 8, 9));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, expectedMapResult));
  }

  private List<CollectionOperation> getCollectionOpsForTestCase15() {
    return Arrays.asList(
        new MergeMapOperation(
            1L,
            COLO_ID_1,
            createMap(Collections.singletonList("k1"), Collections.singletonList(1)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            1L,
            COLO_ID_2,
            createMap(Collections.singletonList("k2"), Collections.singletonList(2)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            2L,
            COLO_ID_1,
            createMap(Collections.singletonList("k3"), Collections.singletonList(3)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            2L,
            COLO_ID_2,
            createMap(Collections.singletonList("k4"), Collections.singletonList(4)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new PutMapOperation(
            3L,
            COLO_ID_1,
            createMap(Arrays.asList("k7", "k8", "k9"), Arrays.asList(7, 8, 9)),
            MAP_FIELD_NAME));
  }

  @Test
  public void testHandleMapOpsCase16() {
    /**
     * Many add-to-map ops first. Then there comes a Put with that overwrites some (but NOT all) existing entries:
     *
     *  - Event 1, at T1, in DC1, collection merging operation to add k1->1
     *  - Event 2, at T1, in DC2, collection merging operation to add k2->2
     *  - Event 3, at T3, in DC1, collection merging operation to add k3->3
     *  - Event 4, at T3, in DC2, collection merging operation to add k4->4
     *  - Event 5, at T2, in DC1, put {k7->7, k8->8, k9->9}
     *
     *  Expect: {k7->7, k8->8, k9->9, k3->3, k4->4}
     *  Explanation:
     *    The first 3 entries in the result map are the new put-only part and it is the same as the last Put.
     *    The last 2 entries in the result map are the collection-merge-added entries.
     *    The first 2 collection-merge-added entries are removed by the Put (which has a higher timestamp).
     */
    List<CollectionOperation> allCollectionOps = getCollectionOpsForTestCase16();
    Map<String, Object> expectedMapResult =
        createMap(Arrays.asList("k7", "k8", "k9", "k3", "k4"), Arrays.asList(7, 8, 9, 3, 4));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, expectedMapResult));
  }

  private List<CollectionOperation> getCollectionOpsForTestCase16() {
    return Arrays.asList(
        new MergeMapOperation(
            1L,
            COLO_ID_1,
            createMap(Collections.singletonList("k1"), Collections.singletonList(1)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            1L,
            COLO_ID_2,
            createMap(Collections.singletonList("k2"), Collections.singletonList(2)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            3L,
            COLO_ID_1,
            createMap(Collections.singletonList("k3"), Collections.singletonList(3)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            3L,
            COLO_ID_2,
            createMap(Collections.singletonList("k4"), Collections.singletonList(4)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new PutMapOperation(
            2L,
            COLO_ID_1,
            createMap(Arrays.asList("k7", "k8", "k9"), Arrays.asList(7, 8, 9)),
            MAP_FIELD_NAME));
  }

  @Test
  public void testHandleMapOpsCase17() {
    /**
     * Many remove-from-map ops first. Then there comes a Put with the lowest timestamp. So, most of the Put entries are
     * deleted:
     *
     *  - Event 1, at T1, in DC1, collection merging operation to remove k1
     *  - Event 2, at T1, in DC2, collection merging operation to remove k2
     *  - Event 3, at T2, in DC1, collection merging operation to remove k3
     *  - Event 4, at T2, in DC2, collection merging operation to remove k4
     *  - Event 5, at T0, in DC1, put {k1->1, k2->2, k3->3, k4->4, k5->5}
     *
     *  Expect: {k5->5}
     *  Explanation:
     *    The first 4 entries in the Put are removed.
     */
    List<CollectionOperation> allCollectionOps = getCollectionOpsForTestCase17();
    Map<String, Object> expectedMapResult = createMap(Collections.singletonList("k5"), Collections.singletonList(5));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, expectedMapResult));
  }

  private List<CollectionOperation> getCollectionOpsForTestCase17() {
    return Arrays.asList(
        new MergeMapOperation(1L, COLO_ID_1, Collections.emptyMap(), Collections.singletonList("k1"), MAP_FIELD_NAME),
        new MergeMapOperation(1L, COLO_ID_2, Collections.emptyMap(), Collections.singletonList("k2"), MAP_FIELD_NAME),
        new MergeMapOperation(2L, COLO_ID_1, Collections.emptyMap(), Collections.singletonList("k3"), MAP_FIELD_NAME),
        new MergeMapOperation(2L, COLO_ID_2, Collections.emptyMap(), Collections.singletonList("k4"), MAP_FIELD_NAME),
        new PutMapOperation(
            0L,
            COLO_ID_1,
            createMap(Arrays.asList("k1", "k2", "k3", "k4", "k5"), Arrays.asList(1, 2, 3, 4, 5)),
            MAP_FIELD_NAME));
  }

  @Test
  public void testHandleMapOpsCase18() {
    /**
     * Many remove-from-map ops first. Then there comes a Put with the highest timestamp. So, Put is (mostly) ignored:
     *
     *  - Event 1, at T1, in DC1, collection merging operation to remove k1
     *  - Event 2, at T1, in DC2, collection merging operation to remove k2
     *  - Event 3, at T2, in DC1, collection merging operation to remove k3
     *  - Event 4, at T2, in DC2, collection merging operation to remove k4
     *  - Event 5, at T3, in DC1, put {k1->1, k2->2, k3->3, k4->4, k5->5}
     *
     *  Expect: {k1->1, k2->2, k3->3, k4->4, k5->5}
     *  Explanation:
     *    The last Put has the highest timestamp. So, no entry is removed from it.
     */
    List<CollectionOperation> allCollectionOps = getCollectionOpsForTestCase18();
    Map<String, Object> expectedMapResult =
        createMap(Arrays.asList("k1", "k2", "k3", "k4", "k5"), Arrays.asList(1, 2, 3, 4, 5));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, expectedMapResult));
  }

  private List<CollectionOperation> getCollectionOpsForTestCase18() {
    return Arrays.asList(
        new MergeMapOperation(1L, COLO_ID_1, Collections.emptyMap(), Collections.singletonList("k1"), MAP_FIELD_NAME),
        new MergeMapOperation(1L, COLO_ID_2, Collections.emptyMap(), Collections.singletonList("k2"), MAP_FIELD_NAME),
        new MergeMapOperation(2L, COLO_ID_1, Collections.emptyMap(), Collections.singletonList("k3"), MAP_FIELD_NAME),
        new MergeMapOperation(2L, COLO_ID_2, Collections.emptyMap(), Collections.singletonList("k4"), MAP_FIELD_NAME),
        new PutMapOperation(
            3L,
            COLO_ID_1,
            createMap(Arrays.asList("k1", "k2", "k3", "k4", "k5"), Arrays.asList(1, 2, 3, 4, 5)),
            MAP_FIELD_NAME));
  }

  @Test
  public void testHandleMapOpsCase19() {
    /**
     * Many remove-from-map ops first. Then there comes a Put which has some entries deleted:
     *
     *  - Event 1, at T1, in DC1, collection merging operation to remove k1
     *  - Event 2, at T1, in DC2, collection merging operation to remove k2
     *  - Event 3, at T3, in DC1, collection merging operation to remove k3
     *  - Event 4, at T3, in DC2, collection merging operation to remove k4
     *  - Event 5, at T2, in DC1, put {k1->1, k2->2, k3->3, k4->4, k5->5}
     *
     *  Expect: {k1->1, k2->2, k5->5}
     *  Explanation:
     *    Entries k3->3 and k4->4 are removed from the last Put.
     */
    List<CollectionOperation> allCollectionOps = getCollectionOpsForTestCase19();
    Map<String, Object> expectedMapResult = createMap(Arrays.asList("k1", "k2", "k5"), Arrays.asList(1, 2, 5));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, expectedMapResult));
  }

  private List<CollectionOperation> getCollectionOpsForTestCase19() {
    return Arrays.asList(
        new MergeMapOperation(1L, COLO_ID_1, Collections.emptyMap(), Collections.singletonList("k1"), MAP_FIELD_NAME),
        new MergeMapOperation(1L, COLO_ID_2, Collections.emptyMap(), Collections.singletonList("k2"), MAP_FIELD_NAME),
        new MergeMapOperation(3L, COLO_ID_1, Collections.emptyMap(), Collections.singletonList("k3"), MAP_FIELD_NAME),
        new MergeMapOperation(3L, COLO_ID_2, Collections.emptyMap(), Collections.singletonList("k4"), MAP_FIELD_NAME),
        new PutMapOperation(
            2L,
            COLO_ID_1,
            createMap(Arrays.asList("k1", "k2", "k3", "k4", "k5"), Arrays.asList(1, 2, 3, 4, 5)),
            MAP_FIELD_NAME));
  }

  @Test
  public void testHandleMapOpsCase20() {
    /**
     * Many add-to-map ops first. Then there comes a Delete with the lowest timestamp. So, Delete is ignored:
     *
     *  - Event 1, at T1, in DC1, collection merging operation to add k1->1
     *  - Event 2, at T1, in DC2, collection merging operation to add k2->2
     *  - Event 3, at T2, in DC1, collection merging operation to add k3->3
     *  - Event 4, at T2, in DC2, collection merging operation to add k4->4
     *  - Event 5, at T0, in DC1, Delete
     *
     *  Expect: {k1->1, k2->2, k3->3, k4->4}
     *  Explanation:
     *    All entries are added by the collection-merge add ops and the Delete is ignored.
     */
    List<CollectionOperation> allCollectionOps = getCollectionOpsForTestCase20();
    Map<String, Object> expectedMapResult = createMap(Arrays.asList("k1", "k2", "k3", "k4"), Arrays.asList(1, 2, 3, 4));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, expectedMapResult));
  }

  private List<CollectionOperation> getCollectionOpsForTestCase20() {
    return Arrays.asList(
        new MergeMapOperation(
            1L,
            COLO_ID_1,
            createMap(Collections.singletonList("k1"), Collections.singletonList(1)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            1L,
            COLO_ID_2,
            createMap(Collections.singletonList("k2"), Collections.singletonList(2)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            2L,
            COLO_ID_1,
            createMap(Collections.singletonList("k3"), Collections.singletonList(3)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            2L,
            COLO_ID_2,
            createMap(Collections.singletonList("k4"), Collections.singletonList(4)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new DeleteMapOperation(0L, COLO_ID_1, MAP_FIELD_NAME));
  }

  @Test
  public void testHandleMapOpsCase21() {
    /**
     * Many add-to-map ops first. Then there comes a Delete which deletes half of the entries.
     *
     *  - Event 1, at T1, in DC1, collection merging operation to add k1->1
     *  - Event 2, at T1, in DC2, collection merging operation to add k2->2
     *  - Event 3, at T3, in DC1, collection merging operation to add k3->3
     *  - Event 4, at T3, in DC2, collection merging operation to add k4->4
     *  - Event 5, at T2, in DC2, Delete
     *
     *  Expect: {k3->3, k4->4}
     *  Explanation:
     *    Entries k1->1 and k2->2 are deleted.
     */
    List<CollectionOperation> allCollectionOps = getCollectionOpsForTestCase21();
    Map<String, Object> expectedMapResult = createMap(Arrays.asList("k3", "k4"), Arrays.asList(3, 4));
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, expectedMapResult));
  }

  private List<CollectionOperation> getCollectionOpsForTestCase21() {
    return Arrays.asList(
        new MergeMapOperation(
            1L,
            COLO_ID_1,
            createMap(Collections.singletonList("k1"), Collections.singletonList(1)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            1L,
            COLO_ID_2,
            createMap(Collections.singletonList("k2"), Collections.singletonList(2)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            3L,
            COLO_ID_1,
            createMap(Collections.singletonList("k3"), Collections.singletonList(3)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            3L,
            COLO_ID_2,
            createMap(Collections.singletonList("k4"), Collections.singletonList(4)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new DeleteMapOperation(2L, COLO_ID_1, MAP_FIELD_NAME));
  }

  @Test
  public void testHandleMapOpsCase22() {
    /**
     * Many add-to-map ops first. Then there comes a Delete with the highest timestamp that deletes all entries.
     *
     *  - Event 1, at T1, in DC1, collection merging operation to add k1->1
     *  - Event 2, at T1, in DC2, collection merging operation to add k2->2
     *  - Event 3, at T2, in DC1, collection merging operation to add k3->3
     *  - Event 4, at T2, in DC2, collection merging operation to add k4->4
     *  - Event 5, at T3, in DC1, Delete
     *
     *  Expect: empty map
     *  Explanation:
     *    All entries are deleted.
     */
    List<CollectionOperation> allCollectionOps = getCollectionOpsForTestCase22();
    Map<String, Object> expectedMapResult = Collections.emptyMap();
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedMapResult(MAP_FIELD_NAME, expectedMapResult));
  }

  private List<CollectionOperation> getCollectionOpsForTestCase22() {
    return Arrays.asList(
        new MergeMapOperation(
            1L,
            COLO_ID_1,
            createMap(Collections.singletonList("k1"), Collections.singletonList(1)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            1L,
            COLO_ID_2,
            createMap(Collections.singletonList("k2"), Collections.singletonList(2)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            2L,
            COLO_ID_1,
            createMap(Collections.singletonList("k3"), Collections.singletonList(3)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new MergeMapOperation(
            2L,
            COLO_ID_2,
            createMap(Collections.singletonList("k4"), Collections.singletonList(4)),
            Collections.emptyList(),
            MAP_FIELD_NAME),
        new DeleteMapOperation(3L, COLO_ID_1, MAP_FIELD_NAME));
  }

  private IndexedHashMap<String, Object> createMap(List<String> keys, List<Integer> values) {
    if (keys.size() != values.size()) {
      throw new IllegalArgumentException();
    }
    IndexedHashMap<String, Object> res = new IndexedHashMap<>();
    for (int i = 0; i < keys.size(); i++) {
      res.put(keys.get(i), values.get(i));
    }
    return res;
  }
}
