package com.linkedin.davinci.replication.merge.helper.utils;

import com.linkedin.venice.utils.IndexedHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * This class should be used to generate all valid sequences (permutations) of collection operations given a set of
 * collection operations to start with. All valid sequences of collection operations simulate all situations/orders where
 * collection merge operations from multiple different colos could arrive at a single colo.
 */
public class CollectionOperationSequenceBuilder {
  private final Map<Integer, List<CollectionOperation>> coloIdToCollectionOps;

  public CollectionOperationSequenceBuilder() {
    this.coloIdToCollectionOps = new HashMap<>();
  }

  public CollectionOperationSequenceBuilder addOperation(CollectionOperation operation) {
    coloIdToCollectionOps.computeIfAbsent(operation.getOpColoID(), coloID -> new LinkedList<>()).add(operation);
    return this;
  }

  public List<List<CollectionOperation>> build() {
    if (coloIdToCollectionOps.isEmpty()) {
      throw new IllegalStateException("Please add collection operation first before calling build().");
    }
    List<List<CollectionOperation>> allColoOperationSequences = new ArrayList<>(coloIdToCollectionOps.size());
    for (Map.Entry<Integer, List<CollectionOperation>> entry: coloIdToCollectionOps.entrySet()) {
      entry.getValue().sort((e1, e2) -> {
        if (e1.getOpColoID() != e2.getOpColoID()) {
          throw new IllegalStateException("Expect all elements to have the same colo ID at this point.");
        }
        if (e1.getOpTimestamp() == e2.getOpTimestamp()) {
          // Do not support same timestamps from the same colo for now.
          throw new IllegalStateException("Timestamps of operations from the same colo have to be different.");
        }
        return Long.compare(e1.getOpTimestamp(), e2.getOpTimestamp());
      });
      allColoOperationSequences.add(entry.getValue());
    }

    return createOperationSequence(allColoOperationSequences);
  }

  private List<List<CollectionOperation>> createOperationSequence(
      List<List<CollectionOperation>> allColoOperationSequences) {
    int totalOpCount = 0;
    for (List<CollectionOperation> ops: allColoOperationSequences) {
      totalOpCount += ops.size();
    }
    List<List<CollectionOperation>> allResults = new ArrayList<>();
    for (int i = 0; i < allColoOperationSequences.size(); i++) {
      createOperationSequenceHelper(
          allColoOperationSequences,
          i,
          totalOpCount,
          new LinkedList<>(),
          allResults,
          new HashSet<>());
    }
    return allResults;
  }

  private void createOperationSequenceHelper(
      List<List<CollectionOperation>> allColoOperationSequences,
      final int currLevel,
      final int totalOpCount,
      List<CollectionOperation> currResultOps,
      List<List<CollectionOperation>> allResults,
      Set<List<CollectionOperation>> allResultsDedupSet) {
    if (currResultOps.size() == totalOpCount) {
      if (allResultsDedupSet.contains(currResultOps)) {
        return;
      }
      List<CollectionOperation> newResult = new ArrayList<>(currResultOps);
      allResults.add(newResult);
      allResultsDedupSet.add(newResult);
      return;
    }
    List<CollectionOperation> ops = allColoOperationSequences.get(currLevel % allColoOperationSequences.size());
    if (ops.isEmpty()) {
      createOperationSequenceHelper(
          allColoOperationSequences,
          currLevel + 1,
          totalOpCount,
          currResultOps,
          allResults,
          allResultsDedupSet);
      return;
    }
    // ops is not empty.
    List<CollectionOperation> opsCopy = new ArrayList<>(ops);
    for (int i = 0; i < opsCopy.size(); i++) {
      currResultOps.add(ops.get(0));
      ops.remove(0);
      createOperationSequenceHelper(
          allColoOperationSequences,
          currLevel + 1,
          totalOpCount,
          currResultOps,
          allResults,
          allResultsDedupSet);
    }
    for (int i = 0; i < opsCopy.size(); i++) {
      currResultOps.remove(currResultOps.size() - 1); // Remove all just-added operations.
    }
    ops.addAll(opsCopy);
  }

  // Manual test and verification.
  public static void main(String[] args) {
    CollectionOperationSequenceBuilder builder = new CollectionOperationSequenceBuilder();
    String fieldName = "test_field_name";
    // Colo 0 ops
    builder.addOperation(new PutListOperation(1, 0, Collections.emptyList(), fieldName));
    builder.addOperation(new MergeListOperation(3, 0, Collections.emptyList(), Collections.emptyList(), fieldName));

    // Colo 1 ops
    builder.addOperation(new PutListOperation(2, 1, Collections.emptyList(), fieldName));
    builder.addOperation(new MergeListOperation(5, 1, Collections.emptyList(), Collections.emptyList(), fieldName));
    builder.addOperation(new PutMapOperation(7, 1, new IndexedHashMap<>(), fieldName));

    List<List<CollectionOperation>> allColoOperationSequences = builder.build();

    int i = 0;
    for (List<CollectionOperation> opSequence: allColoOperationSequences) {
      i++;
      System.out.println("----Colo operation sequence #" + i);
      for (CollectionOperation collectionOperation: opSequence) {
        System.out.println(collectionOperation);
      }
      System.out.println();
    }
  }
}
