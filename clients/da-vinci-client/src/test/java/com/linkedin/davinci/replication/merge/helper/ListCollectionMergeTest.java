package com.linkedin.davinci.replication.merge.helper;

import com.linkedin.davinci.replication.merge.helper.utils.CollectionOperation;
import com.linkedin.davinci.replication.merge.helper.utils.DeleteListOperation;
import com.linkedin.davinci.replication.merge.helper.utils.ExpectedCollectionResults;
import com.linkedin.davinci.replication.merge.helper.utils.MergeListOperation;
import com.linkedin.davinci.replication.merge.helper.utils.PutListOperation;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;


public class ListCollectionMergeTest extends SortBasedCollectionFieldOperationHandlerTestBase {
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
        new MergeListOperation(7L, COLO_ID_2, Collections.emptyList(), Collections.singletonList(3), LIST_FIELD_NAME));
    List<Integer> expectedItemsResult = Arrays.asList(2, 4, 5);
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
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
        new MergeListOperation(7L, COLO_ID_2, Collections.emptyList(), Collections.singletonList(3), LIST_FIELD_NAME));
    List<Integer> expectedItemsResult = Arrays.asList(2, 1, 5);
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
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
        new MergeListOperation(10L, COLO_ID_1, Collections.singletonList(5), Collections.emptyList(), LIST_FIELD_NAME));
    List<Integer> expectedItemsResult = Arrays.asList(3, 2, 1, 6, 7, 8, 5);
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
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
        new MergeListOperation(8L, COLO_ID_1, Collections.emptyList(), Collections.singletonList(1), LIST_FIELD_NAME));
    List<Integer> expectedItemsResult = Collections.emptyList();
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
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
        new PutListOperation(5L, COLO_ID_2, Arrays.asList(7, 8), LIST_FIELD_NAME));
    List<Integer> expectedItemsResult = Arrays.asList(7, 8);
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
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
        new MergeListOperation(6L, COLO_ID_2, Collections.emptyList(), Collections.singletonList(5), LIST_FIELD_NAME));
    List<Integer> expectedItemsResult = Arrays.asList(1, 2, 3, 4, 6);
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
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
        new PutListOperation(6L, COLO_ID_2, Arrays.asList(7, 8, 9), LIST_FIELD_NAME));
    List<Integer> expectedItemsResult = Arrays.asList(7, 8, 9);
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
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
        new DeleteListOperation(6L, COLO_ID_2, LIST_FIELD_NAME));
    List<Integer> expectedItemsResult = Collections.emptyList();
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
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
        new DeleteListOperation(6L, COLO_ID_2, LIST_FIELD_NAME));
    List<Integer> expectedItemsResult = Collections.singletonList(5);
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
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
        new MergeListOperation(6L, COLO_ID_2, Collections.emptyList(), Collections.singletonList(4), LIST_FIELD_NAME));
    List<Integer> expectedItemsResult = Arrays.asList(1, 2, 3);
    applyAllOperationsOnValue(
        allCollectionOps,
        ExpectedCollectionResults.createExpectedListResult(LIST_FIELD_NAME, expectedItemsResult));
  }
}
