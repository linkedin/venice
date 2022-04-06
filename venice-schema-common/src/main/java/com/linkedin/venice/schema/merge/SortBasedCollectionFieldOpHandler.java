package com.linkedin.venice.schema.merge;

import com.linkedin.venice.utils.IndexedHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import com.linkedin.venice.schema.rmd.v1.CollectionReplicationMetadata;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.NotImplementedException;

@ThreadSafe
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
    if (ignoreIncomingRequest(putTimestamp, coloID, collectionFieldRmd)) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    validateFieldSchemaType(currValueRecord, fieldName, Schema.Type.ARRAY, false); // Validate before modifying any state.

    // Current list will be updated.
    final long currTopLevelTimestamp = collectionFieldRmd.getTopLevelFieldTimestamp();
    collectionFieldRmd.setTopLevelFieldTimestamp(putTimestamp);
    collectionFieldRmd.setTopLevelColoID(coloID);

    if (collectionFieldRmd.isInPutOnlyState()) {
      currValueRecord.put(fieldName, toPutList);
      collectionFieldRmd.setPutOnlyPartLength(toPutList.size());
      return UpdateResultStatus.COMPLETELY_UPDATED;
    }
    // The current list is NOT in the put-only state. So we need to de-dup the incoming list.
    deDupListFromEnd(toPutList);

    List<Object> currElements = (List<Object>) currValueRecord.get(fieldName);
    if (currElements == null) {
      currElements = Collections.emptyList();
    }
    final List<Long> currActiveTimestamps = collectionFieldRmd.getActiveElementTimestamps();
    final int putOnlyPartLength = collectionFieldRmd.getPutOnlyPartLength();

    // Below map contains only elements with timestamps that are strictly larger than the Put timestamp.
    final IndexedHashMap<Object, Long> activeElementToTsMap = createElementToActiveTsMap(
        currElements,
        currActiveTimestamps,
        currTopLevelTimestamp,
        putTimestamp,
        putOnlyPartLength
    );

    final List<Object> deletedElements = collectionFieldRmd.getDeletedElements();
    final List<Long> deletedTimestamps = collectionFieldRmd.getDeletedElementTimestamps();
    final IndexedHashMap<Object, Long> deletedElementToTsMap = createDeletedElementToTsMap(
        deletedElements,
        deletedTimestamps,
        putTimestamp
    );

    // Step 1: Check to-be-put list elements against the deleted and existing elements in the list.
    // An iterator is used to iterate the to-be-put list because it is O(1) to remove an element from the to-be-put list
    // using an iterator if the to-be-put list is a linked list.
    Iterator<Object> toPutListIterator = toPutList.iterator();
    while (toPutListIterator.hasNext()) {
      final Object newElement = toPutListIterator.next();
      final Long deletedTs = deletedElementToTsMap.get(newElement);
      if (deletedTs != null) {
        if (deletedTs >= putTimestamp) {
          toPutListIterator.remove(); // This element was already deleted with a higher or equal timestamp.
        } else {
          deletedElementToTsMap.remove(newElement); // This element will be added back.
        }
        continue;
      }
      final Long activeElementTs = activeElementToTsMap.get(newElement);
      if (activeElementTs != null) {
        if (activeElementTs <= putTimestamp) {
          activeElementToTsMap.remove(newElement);
        } else {
          toPutListIterator.remove(); // This element already exists as a current element with a higher or equal timestamp.
        }
      }
    }

    // Step 2: Insert new put-only part elements at the front part.
    final int newPutOnlyPartLength = toPutList.size();
    List<Object> newElements = new ArrayList<>(newPutOnlyPartLength + activeElementToTsMap.size());
    List<Long> newActiveTimestamps = new ArrayList<>(activeElementToTsMap.size());

    collectionFieldRmd.setPutOnlyPartLength(newPutOnlyPartLength);
    // Add elements for the put-only part.
    toPutListIterator = toPutList.iterator();
    while (toPutListIterator.hasNext()) {
      newElements.add(toPutListIterator.next());
    }
    // Add elements and timestamps for the collection-merge part.
    activeElementToTsMap.forEach((activeElement, activeTimestamp) -> {
      newElements.add(activeElement);
      newActiveTimestamps.add(activeTimestamp);
    });

    // Step 3: Set current elements and their active timestamps.
    currValueRecord.put(fieldName, newElements);
    collectionFieldRmd.setActiveElementTimestamps(newActiveTimestamps);

    // Step 4: Set deleted elements and their deleted timestamps.
    List<Object> newDeletedElements = new ArrayList<>(deletedElementToTsMap.size());
    List<Long> newDeletedTimestamps = new ArrayList<>(deletedElementToTsMap.size());
    deletedElementToTsMap.forEach((deletedElement, deletedTimestamp) -> {
      newDeletedElements.add(deletedElement);
      newDeletedTimestamps.add(deletedTimestamp);
    });
    collectionFieldRmd.setDeletedElements(newDeletedElements);
    collectionFieldRmd.setDeletedTimestamps(newDeletedTimestamps);

    return collectionFieldRmd.isInPutOnlyState() ? UpdateResultStatus.COMPLETELY_UPDATED : UpdateResultStatus.PARTIALLY_UPDATED;
  }

  private void deDupListFromEnd(List<Object> list) {
    if (list.isEmpty()) {
      return;
    }
    Set<Object> deDupSet = new HashSet<>();
    ListIterator<Object> iterator = list.listIterator(list.size());

    while (iterator.hasPrevious()) {
      Object newElement = iterator.previous();
      if (deDupSet.contains(newElement)) {
        iterator.remove();
      } else {
        deDupSet.add(newElement);
      }
    }
    deDupSet.clear(); // Try to be more GC friendly.
  }

  private IndexedHashMap<Object, Long> createElementToActiveTsMap(
      List<Object> existingElements,
      List<Long> activeTimestamps,
      final long topLevelTimestamp,
      final long minTimestamp, // Any timestamp smaller than or equal to this one will not be included in the result map.
      final int putOnlyPartLength
  ) {
    IndexedHashMap<Object, Long> activeElementToTsMap = new IndexedHashMap<>(existingElements.size());
    int idx = 0;
    for (Object existingElement : existingElements) {
      final long activeTimestamp;
      if (idx < putOnlyPartLength) {
        activeTimestamp = topLevelTimestamp;
      } else {
        activeTimestamp = activeTimestamps.get(idx - putOnlyPartLength);
      }
      if (activeTimestamp > minTimestamp) {
        activeElementToTsMap.put(existingElement, activeTimestamp);
      }
      idx++;
    }
    return activeElementToTsMap;
  }

  private IndexedHashMap<Object, Long> createDeletedElementToTsMap(
      List<Object> deletedElements,
      List<Long> deletedTimestamps,
      final long minTimestamp
  ) {
    IndexedHashMap<Object, Long> elementToTimestampMap = new IndexedHashMap<>();
    int idx = 0;
    for (long deletedTimestamp : deletedTimestamps) {
      if (deletedTimestamp > minTimestamp) {
        elementToTimestampMap.put(deletedElements.get(idx), deletedTimestamp);
      }
      idx++;
    }
    return elementToTimestampMap;
  }

  private void validateFieldSchemaType(
      GenericRecord currValueRecord,
      String fieldName,
      Schema.Type expectType,
      boolean nullableAllowed
  ) {
    final Schema fieldSchema = currValueRecord.getSchema().getField(fieldName).schema();
    final Schema.Type fieldSchemaType = fieldSchema.getType();
    if (nullableAllowed && fieldSchemaType == Schema.Type.UNION) {
      validateFieldSchemaIsNullableType(fieldSchema, expectType);
      return;
    }

    if (fieldSchemaType != expectType) {
      throw new IllegalStateException(
          String.format("Expect field %s to be of type %s. But got: %s", fieldName, expectType, fieldSchemaType));
    }
  }

  private void validateFieldSchemaIsNullableType(Schema fieldSchema, Schema.Type expectType) {
    // // Expect a nullable type. Expect a union of [null, expected type]
    if (fieldSchema.getType() != Schema.Type.UNION) {
      throw new IllegalStateException("Expect a union. Got field schema: " + fieldSchema);
    }
    if (fieldSchema.getTypes().size() != 2) {
      throw new IllegalStateException("Expect a union of size 2. Got field schema: " + fieldSchema);
    }
    if (fieldSchema.getTypes().get(0).getType() != Schema.Type.NULL) {
      throw new IllegalStateException("Expect the first element in the union to be null. Got field schema: " + fieldSchema);
    }
    if (fieldSchema.getTypes().get(1).getType() != expectType) {
      throw new IllegalStateException("Expect the second element in the union to be the expected type. Got field schema: " + fieldSchema);
    }
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
    if (ignoreIncomingRequest(putTimestamp, coloID, collectionFieldRmd)) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    // Current map will be updated.
    if (collectionFieldRmd.isInPutOnlyState()) {
      collectionFieldRmd.setTopLevelFieldTimestamp(putTimestamp);
      collectionFieldRmd.setTopLevelColoID(coloID);
      currValueRecord.put(fieldName, toPutMap);
      collectionFieldRmd.setPutOnlyPartLength(toPutMap.size());
      return UpdateResultStatus.COMPLETELY_UPDATED;
    } else {
      throw new NotImplementedException("Cannot handle Put on a map that is in the collection merge state.");
    }
  }

  @Override
  public UpdateResultStatus handleDeleteList(
      final long deleteTimestamp,
      final int coloID,
      CollectionReplicationMetadata collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName
  ) {
    if (ignoreIncomingRequest(deleteTimestamp, coloID, collectionFieldRmd)) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    validateFieldSchemaType(currValueRecord, fieldName, Schema.Type.ARRAY, false); // Validate before modifying any state.
    // Current list will be deleted (partially or completely).
    final int currPutOnlyPartLength = collectionFieldRmd.getPutOnlyPartLength();
    collectionFieldRmd.setTopLevelFieldTimestamp(deleteTimestamp);
    collectionFieldRmd.setTopLevelColoID(coloID);
    collectionFieldRmd.setPutOnlyPartLength(0); // No put-only part because it should be deleted completely.

    if (collectionFieldRmd.isInPutOnlyState()) {
      // Do not use Collections.empty() in case this field is modified later.
      currValueRecord.put(fieldName, new ArrayList<>(0));
      return UpdateResultStatus.COMPLETELY_UPDATED;
    }

    // Step 1: Remove all deleted elements and their deleted timestamps with smaller or equal timestamps.
    collectionFieldRmd.removeDeletionInfoWithTimestampsLowerOrEqualTo(deleteTimestamp);

    // Step 2: Remove all active elements with smaller or equal timestamps.
    final int removedActiveTimestampsCount = collectionFieldRmd.removeActiveTimestampsLowerOrEqualTo(deleteTimestamp);
    List<Object> currList = (List<Object>) currValueRecord.get(fieldName);
    List<Object> remainingList = new ArrayList<>(currList.size() - currPutOnlyPartLength - removedActiveTimestampsCount);

    // All elements in the current put-only part should be removed.
    final int remainingElementsStartIdx = currPutOnlyPartLength + removedActiveTimestampsCount;
    Iterator<Object> currListIterator = currList.listIterator(remainingElementsStartIdx);
    while (currListIterator.hasNext()) {
      remainingList.add(currListIterator.next());
    }

    currValueRecord.put(fieldName, remainingList);
    return collectionFieldRmd.isInPutOnlyState() ? UpdateResultStatus.COMPLETELY_UPDATED : UpdateResultStatus.PARTIALLY_UPDATED;
  }

  @Override
  public UpdateResultStatus handleDeleteMap(
      final long deleteTimestamp,
      final int coloID,
      CollectionReplicationMetadata collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName
  ) {
    if (ignoreIncomingRequest(deleteTimestamp, coloID, collectionFieldRmd)) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    if (collectionFieldRmd.isInPutOnlyState()) {
      validateFieldSchemaType(currValueRecord, fieldName, Schema.Type.MAP, true); // Validate before modifying any state.
      collectionFieldRmd.setTopLevelFieldTimestamp(deleteTimestamp);
      collectionFieldRmd.setTopLevelColoID(coloID);
      collectionFieldRmd.setPutOnlyPartLength(0); // No put-only part because it should be deleted completely.
      currValueRecord.put(fieldName, new IndexedHashMap<>(0));
      return UpdateResultStatus.COMPLETELY_UPDATED;
    } else {
      throw new NotImplementedException("Cannot handle Delete on a map that is in the collection merge state.");
    }
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
    // When timestamps are the same, full put/delete on a collection field always takes precedence over collection merge.
    if (ignoreIncomingRequest(modifyTimestamp, Integer.MIN_VALUE, collectionFieldRmd)) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    validateFieldSchemaType(currValueRecord, fieldName, Schema.Type.ARRAY, false);
    Set<Object> toAddElementSet = new HashSet<>(toAddElements);
    Set<Object> toRemoveElementSet = new HashSet<>(toRemoveElements);
    removeIntersectionElements(toAddElementSet, toRemoveElementSet);
    if (toAddElements.isEmpty() && toRemoveElementSet.isEmpty()) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }

    if (collectionFieldRmd.isInPutOnlyState()) {
      return handleModifyPutOnlyList(modifyTimestamp, collectionFieldRmd, currValueRecord, fieldName, toAddElementSet, toRemoveElementSet);
    } else {
      return handleModifyCollectionMergeList(modifyTimestamp, collectionFieldRmd, currValueRecord, fieldName, toAddElementSet, toRemoveElementSet);
    }
  }

  private UpdateResultStatus handleModifyPutOnlyList(
      final long modifyTimestamp,
      CollectionReplicationMetadata collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName,
      Set<Object> toAddElementSet,
      Set<Object> toRemoveElementSet
  ) {
    if (!collectionFieldRmd.isInPutOnlyState()) {
      throw new IllegalStateException("Expect list to be in the put-only state.");
    }
    if (toAddElementSet.isEmpty() && toRemoveElementSet.isEmpty()) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    List<Object> currElements = (List<Object>) currValueRecord.get(fieldName);
    if (currElements == null) {
      currElements = Collections.emptyList();
    } else {
      currElements = (currElements instanceof LinkedList) ? currElements : new LinkedList<>(currElements);
      deDupListFromEnd(currElements);
    }
    final long topLevelTimestamp = collectionFieldRmd.getTopLevelFieldTimestamp();
    final IndexedHashMap<Object, Long> activeElementToTsMap = createElementToActiveTsMap(
        currElements,
        Collections.emptyList(),
        collectionFieldRmd.getTopLevelFieldTimestamp(),
        Long.MIN_VALUE,
        currElements.size()
    );

    int newPutOnlyPartLength = currElements.size();
    // Step 1: Add elements (SET_UNION).
    for (Object toAddElement : toAddElementSet) {
      final Long activeTimestamp = activeElementToTsMap.get(toAddElement);
      if (activeTimestamp == null) {
        activeElementToTsMap.put(toAddElement, modifyTimestamp);

      } else if (activeTimestamp >= modifyTimestamp) {
        throw new IllegalStateException("If active timestamp exists for this element, it must be strictly smaller than "
            + "the modify timestamp because this field is currently in the put-only state.");
      } else {
        // Re-position this element to the end of the map.
        activeElementToTsMap.remove(toAddElement);
        activeElementToTsMap.put(toAddElement, modifyTimestamp);
        newPutOnlyPartLength--;
      }
    }

    // Step 2: Remove elements (SET_DIFF).
    final List<ElementAndTimestamp> deletedElementAndTsList = new ArrayList<>();
    for (Object toRemoveElement: toRemoveElementSet) {
      final Long activeTimestamp = activeElementToTsMap.get(toRemoveElement);
      if (activeTimestamp == null) {
        deletedElementAndTsList.add(new ElementAndTimestamp(toRemoveElement, modifyTimestamp));
      } else if (activeTimestamp <= modifyTimestamp) {
        // Delete existing element
        activeElementToTsMap.remove(toRemoveElement);
        deletedElementAndTsList.add(new ElementAndTimestamp(toRemoveElement, modifyTimestamp));
        if (activeTimestamp == topLevelTimestamp) {
          // Removed an element from the put-only part.
          newPutOnlyPartLength--;
        }
      }
    }

    Supplier<Schema> elementSchemaSupplier = () -> currValueRecord.getSchema().getField(fieldName).schema().getElementType();
    sortElementAndTimestampList(deletedElementAndTsList, elementSchemaSupplier);

    // Step 3: Set new active elements and their active timestamps.
    List<ElementAndTimestamp> activeElementAndTsList = new ArrayList<>(activeElementToTsMap.size());
    for (Map.Entry<Object, Long> entry : activeElementToTsMap.entrySet()) {
      activeElementAndTsList.add(new ElementAndTimestamp(entry.getKey(), entry.getValue()));
    }
    sortElementAndTimestampList(
        // Only sort the collection-merge part of the list and leave the put-only part as is.
        activeElementAndTsList.subList(newPutOnlyPartLength, activeElementAndTsList.size()),
        elementSchemaSupplier
    );
    setNewActiveElementAndTsList(activeElementAndTsList, newPutOnlyPartLength, currValueRecord, fieldName, collectionFieldRmd);

    // Step 4: Set new deleted elements and their deleted timestamps.
    setDeletedDeletedElementAndTsList(deletedElementAndTsList, collectionFieldRmd);

    // Must be partially updated because there must be elements added and/or removed.
    return UpdateResultStatus.PARTIALLY_UPDATED;
  }

  // Current list must be in the collection-merge state where the current list has 2 parts with the first part being
  // the put-only part and the second part being the collection-merge part.
  private UpdateResultStatus handleModifyCollectionMergeList(
      final long modifyTimestamp,
      CollectionReplicationMetadata collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName,
      Set<Object> toAddElementSet,
      Set<Object> toRemoveElementSet
  ) {
    if (collectionFieldRmd.isInPutOnlyState()) {
      throw new IllegalStateException("Expect list to be in the collection-merge state.");
    }
    if (toAddElementSet.isEmpty() && toRemoveElementSet.isEmpty()) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }

    List<Object> currElements = (List<Object>) currValueRecord.get(fieldName);
    if (currElements == null) {
      currElements = Collections.emptyList();
    }
    final List<Long> activeTimestamps = collectionFieldRmd.getActiveElementTimestamps();
    final IndexedHashMap<Object, Long> activeElementToTsMap = createElementToActiveTsMap(
        currElements,
        activeTimestamps,
        collectionFieldRmd.getTopLevelFieldTimestamp(),
        Long.MIN_VALUE,
        collectionFieldRmd.getPutOnlyPartLength()
    );

    final List<Object> deletedElements = collectionFieldRmd.getDeletedElements();
    final List<Long> deletedTimestamps = collectionFieldRmd.getDeletedElementTimestamps();
    final IndexedHashMap<Object, Long> deletedElementToTsMap = createDeletedElementToTsMap(
        deletedElements,
        deletedTimestamps,
        Long.MIN_VALUE
    );

    boolean updated = false;
    int newPutOnlyPartLength = collectionFieldRmd.getPutOnlyPartLength();
    final long topLevelTimestamp = collectionFieldRmd.getTopLevelFieldTimestamp();
    // Step 1: Add elements (SET_UNION).
    for (Object toAddElement : toAddElementSet) {
      final Long deletedTimestamp = deletedElementToTsMap.get(toAddElement);
      if (deletedTimestamp != null) {
        if (deletedTimestamp < modifyTimestamp) {
          // Element will be added back.
          deletedElementToTsMap.remove(toAddElement);
          activeElementToTsMap.put(toAddElement, modifyTimestamp);
          updated = true;
        } // Else: Element remains "deleted".
        continue;
      }

      final Long activeTimestamp = activeElementToTsMap.get(toAddElement);
      if (activeTimestamp != null && activeTimestamp == topLevelTimestamp) {
        // This element exists and it is in the put-only part.
        activeElementToTsMap.remove(toAddElement);
        newPutOnlyPartLength--;
      }

      if (activeTimestamp == null || activeTimestamp != modifyTimestamp) {
        activeElementToTsMap.put(toAddElement, modifyTimestamp);
        updated = true;
      }
    }

    // Step 2: Remove elements (SET_DIFF).
    for (Object toRemoveElement: toRemoveElementSet) {
      final Long deletedTimestamp = deletedElementToTsMap.get(toRemoveElement);
      if (deletedTimestamp != null) {
        if (deletedTimestamp < modifyTimestamp) {
          deletedElementToTsMap.put(toRemoveElement, modifyTimestamp);
          updated = true;
        }
        continue;
      }
      final Long activeTimestamp = activeElementToTsMap.get(toRemoveElement);
      if (activeTimestamp != null) {
        if (activeTimestamp <= modifyTimestamp) {
          // Delete the existing element.
          activeElementToTsMap.remove(toRemoveElement);
          deletedElementToTsMap.put(toRemoveElement, modifyTimestamp);
          if (activeTimestamp == topLevelTimestamp) {
            newPutOnlyPartLength--;
          }
          updated = true;
        } // Else: existing element does not get deleted.
        continue;
      }

      // Element neither existed nor deleted because both it has no deleted timestamp and no active timestamp.
      deletedElementToTsMap.put(toRemoveElement, modifyTimestamp);
      updated = true;
    }

    // Step 3: Set new active elements and their active timestamps.
    final List<ElementAndTimestamp> activeElementAndTsList = new ArrayList<>(activeElementToTsMap.size());
    activeElementToTsMap.forEach((activeElement, activeTimestamp) -> {
      activeElementAndTsList.add(new ElementAndTimestamp(activeElement, activeTimestamp));
    });
    final Supplier<Schema> elementSchemaSupplier = () -> currValueRecord.getSchema().getField(fieldName).schema().getElementType();
    sortElementAndTimestampList(
        // Only sort the collection-merge part of the list and leave the put-only part as is.
        activeElementAndTsList.subList(newPutOnlyPartLength, activeElementAndTsList.size()),
        elementSchemaSupplier
    );
    setNewActiveElementAndTsList(activeElementAndTsList, newPutOnlyPartLength, currValueRecord, fieldName, collectionFieldRmd);

    // Step 4: Set new deleted elements and their deleted timestamps.
    final List<ElementAndTimestamp> deletedElementAndTsList = new ArrayList<>(deletedElementToTsMap.size());
    deletedElementToTsMap.forEach((element, timestamp) -> {
      deletedElementAndTsList.add(new ElementAndTimestamp(element, timestamp));
    });

    sortElementAndTimestampList(deletedElementAndTsList, elementSchemaSupplier);
    setDeletedDeletedElementAndTsList(deletedElementAndTsList, collectionFieldRmd);

    return updated ? UpdateResultStatus.PARTIALLY_UPDATED : UpdateResultStatus.NOT_UPDATED_AT_ALL;
  }

  private void setNewActiveElementAndTsList(
      List<ElementAndTimestamp> activeElementAndTsList,
      int newPutOnlyPartLength,
      GenericRecord currValueRecord,
      String fieldName,
      CollectionReplicationMetadata collectionFieldRmd
  ) {
    List<Object> newActiveElements = new ArrayList<>(activeElementAndTsList.size());
    List<Long> newActiveTimestamps = new ArrayList<>(activeElementAndTsList.size() - newPutOnlyPartLength);
    int idx = 0;
    for (ElementAndTimestamp activeElementAndTs : activeElementAndTsList) {
      newActiveElements.add(activeElementAndTs.getElement());
      if (idx >= newPutOnlyPartLength) {
        newActiveTimestamps.add(activeElementAndTs.getTimestamp());
      }
      idx++;
    }
    collectionFieldRmd.setActiveElementTimestamps(newActiveTimestamps);
    currValueRecord.put(fieldName, newActiveElements);
    collectionFieldRmd.setPutOnlyPartLength(newPutOnlyPartLength);
  }

  private void setDeletedDeletedElementAndTsList(
      List<ElementAndTimestamp> deletedElementAndTsList,
      CollectionReplicationMetadata collectionFieldRmd
  ) {
    List<Object> deletedElements = new ArrayList<>(deletedElementAndTsList.size());
    List<Long> deletedTimestamps = new ArrayList<>(deletedElementAndTsList.size());
    for (ElementAndTimestamp deletedElementAndTs : deletedElementAndTsList) {
      deletedElements.add(deletedElementAndTs.getElement());
      deletedTimestamps.add(deletedElementAndTs.getTimestamp());
    }
    collectionFieldRmd.setDeletedTimestamps(deletedTimestamps);
    collectionFieldRmd.setDeletedElements(deletedElements);
  }

  private void sortElementAndTimestampList(List<ElementAndTimestamp> elementAndTsList, Supplier<Schema> elementSchema) {
    elementAndTsList.sort((o1, o2) -> {
      final int timestampCompareResult = Long.compare(o1.getTimestamp(), o2.getTimestamp());
      if (timestampCompareResult == 0) {
        // TODO: handle the situation where two elements have different schemas (e.g. element schema evolution). Assume
        // element schemas are always the same for now.
        return elementComparator.compare(o1.getElement(), o2.getElement(), elementSchema.get());
      }
      return timestampCompareResult;
    });
  }

  private void removeIntersectionElements(Set<Object> set1, Set<Object> set2) {
    List<Object> intersectionElements = new ArrayList<>();
    for (Object element : set1) {
      if (set2.remove(element)) {
        intersectionElements.add(element);
      }
    }
    // Note that A.removeAll(B) is slow when the size of B is greater than the size of A. However, in our case, B is
    // smaller than A. So it is fine to use the removeAll method.
    set1.removeAll(intersectionElements);
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
    throw new NotImplementedException("Collect merge on map has not been implemented yet.");
  }

  private boolean ignoreIncomingRequest(
      final long incomingRequestTimestamp,
      final int incomingRequestColoID,
      CollectionReplicationMetadata currCollectionFieldRmd
  ) {
    if (currCollectionFieldRmd.getTopLevelFieldTimestamp() > incomingRequestTimestamp) {
      return true;

    } else if (currCollectionFieldRmd.getTopLevelFieldTimestamp() == incomingRequestTimestamp) {
      // When both top-level timestamp and colo ID are the same, always let the incoming request win assuming that the
      // incoming request has a higher offset.
      return currCollectionFieldRmd.getTopLevelColoID() > incomingRequestColoID;
    }
    return false;
  }
}
