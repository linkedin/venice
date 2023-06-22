package com.linkedin.venice.schema.merge;

import com.linkedin.avro.api.PrimitiveLongList;
import com.linkedin.avro.fastserde.primitive.PrimitiveLongArrayList;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp;
import com.linkedin.venice.utils.IndexedHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


@ThreadSafe
public class SortBasedCollectionFieldOpHandler extends CollectionFieldOperationHandler {
  public SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator elementComparator) {
    super(elementComparator);
  }

  @Override
  public UpdateResultStatus handlePutList(
      final long putTimestamp,
      final int coloID,
      List<Object> newFieldValue,
      CollectionRmdTimestamp<Object> collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName) {
    if (ignoreIncomingRequest(putTimestamp, coloID, collectionFieldRmd)) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    validateFieldSchemaType(currValueRecord, fieldName, Schema.Type.ARRAY, true);

    // Current list will be updated.
    final long currTopLevelTimestamp = collectionFieldRmd.getTopLevelFieldTimestamp();
    collectionFieldRmd.setTopLevelFieldTimestamp(putTimestamp);
    collectionFieldRmd.setTopLevelColoID(coloID);

    List<Object> toPutList;
    if (newFieldValue == null) {
      toPutList = Collections.emptyList();
    } else {
      toPutList = newFieldValue;
    }

    if (collectionFieldRmd.isInPutOnlyState()) {
      // This is to make sure we put the exact value when the new value is null.
      currValueRecord.put(fieldName, newFieldValue);
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
    final IndexedHashMap<Object, Long> activeElementToTsMap = Utils.createElementToActiveTsMap(
        currElements,
        currActiveTimestamps,
        currTopLevelTimestamp,
        putTimestamp,
        putOnlyPartLength);

    final List<Object> deletedElements = collectionFieldRmd.getDeletedElements();
    final List<Long> deletedTimestamps = collectionFieldRmd.getDeletedElementTimestamps();
    final IndexedHashMap<Object, Long> deletedElementToTsMap =
        Utils.createDeletedElementToTsMap(deletedElements, deletedTimestamps, putTimestamp);

    // Step 1: Check to-be-put list elements against the deleted and existing elements in the list.
    // An iterator is used to iterate the to-be-put list because it is O(1) to remove an element from the to-be-put list
    // using an iterator if the to-be-put list is a linked list.
    Iterator<Object> toPutListIterator = toPutList.iterator();
    while (toPutListIterator.hasNext()) {
      final Object newElement = toPutListIterator.next();
      final Long deletedTs = deletedElementToTsMap.get(newElement);
      if (deletedTs != null) {
        // Element was deleted before.
        if (deletedTs >= putTimestamp) {
          toPutListIterator.remove(); // This element was already deleted with a higher or equal timestamp.
        } else {
          deletedElementToTsMap.remove(newElement); // This element will be added back.
        }

      } else {
        // Element was not deleted before.
        final Long activeElementTs = activeElementToTsMap.get(newElement);
        if (activeElementTs != null) {
          if (activeElementTs <= putTimestamp) {
            activeElementToTsMap.remove(newElement);
          } else {
            toPutListIterator.remove(); // This element already exists as a current element with a higher or equal
                                        // timestamp.
          }
        }
      }
    }

    // Step 2: Insert new put-only part elements at the front part.
    final int newPutOnlyPartLength = toPutList.size();
    List<Object> newElements = new ArrayList<>(newPutOnlyPartLength + activeElementToTsMap.size());
    PrimitiveLongList newActiveTimestamps = new PrimitiveLongArrayList(activeElementToTsMap.size());

    collectionFieldRmd.setPutOnlyPartLength(newPutOnlyPartLength);
    // Add elements for the put-only part.
    newElements.addAll(toPutList);

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
    PrimitiveLongList newDeletedTimestamps = new PrimitiveLongArrayList(deletedElementToTsMap.size());
    deletedElementToTsMap.forEach((deletedElement, deletedTimestamp) -> {
      newDeletedElements.add(deletedElement);
      newDeletedTimestamps.add(deletedTimestamp);
    });
    collectionFieldRmd.setDeletedElementsAndTimestamps(newDeletedElements, newDeletedTimestamps);
    if (collectionFieldRmd.isInPutOnlyState() && newFieldValue == null) {
      currValueRecord.put(fieldName, null);
    }
    return collectionFieldRmd.isInPutOnlyState()
        ? UpdateResultStatus.COMPLETELY_UPDATED
        : UpdateResultStatus.PARTIALLY_UPDATED;
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

  private void validateFieldSchemaType(
      GenericRecord currValueRecord,
      String fieldName,
      Schema.Type expectType,
      boolean nullableAllowed) {
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
      throw new IllegalStateException(
          "Expect the first element in the union to be null. Got field schema: " + fieldSchema);
    }
    if (fieldSchema.getTypes().get(1).getType() != expectType) {
      throw new IllegalStateException(
          "Expect the second element in the union to be the expected type. Got field schema: " + fieldSchema);
    }
  }

  @Override
  public UpdateResultStatus handlePutMap(
      final long putTimestamp,
      final int coloID,
      IndexedHashMap<String, Object> newFieldValue,
      CollectionRmdTimestamp<String> collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName) {
    if (ignoreIncomingRequest(putTimestamp, coloID, collectionFieldRmd)) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    validateFieldSchemaType(currValueRecord, fieldName, Schema.Type.MAP, true);
    collectionFieldRmd.setTopLevelFieldTimestamp(putTimestamp);
    collectionFieldRmd.setTopLevelColoID(coloID);
    IndexedHashMap<String, Object> toPutMap;
    if (newFieldValue == null) {
      toPutMap = new IndexedHashMap<>();
    } else {
      toPutMap = newFieldValue;
    }

    // Current map will be updated.
    if (collectionFieldRmd.isInPutOnlyState()) {
      // This is to make sure we put the exact value when the new value is null.
      currValueRecord.put(fieldName, newFieldValue);
      collectionFieldRmd.setPutOnlyPartLength(toPutMap.size());
      return UpdateResultStatus.COMPLETELY_UPDATED;
    }

    // Handle Put on a map that is in the collection-merge state.
    IndexedHashMap<String, Object> currMap = (IndexedHashMap<String, Object>) currValueRecord.get(fieldName);
    List<KeyValPair> currKeyValPairs = new ArrayList<>(currMap.size());
    currMap.forEach((key, value) -> currKeyValPairs.add(new KeyValPair(key, value)));

    final List<Long> activeTimestamps = collectionFieldRmd.getActiveElementTimestamps();
    // Below map contains only elements with timestamps that are strictly larger than the Put timestamp.
    final IndexedHashMap<KeyValPair, Long> activeEntriesToTsMap = Utils.createElementToActiveTsMap(
        currKeyValPairs,
        activeTimestamps,
        collectionFieldRmd.getTopLevelFieldTimestamp(),
        putTimestamp,
        collectionFieldRmd.getPutOnlyPartLength());

    final List<String> deletedKeys = collectionFieldRmd.getDeletedElements();
    final List<Long> deletedTimestamps = collectionFieldRmd.getDeletedElementTimestamps();
    final IndexedHashMap<String, Long> deletedKeyToTsMap =
        Utils.createDeletedElementToTsMap(deletedKeys, deletedTimestamps, putTimestamp);

    // Step 1: Check to-be-put map entries against the deleted and existing entries in the map.
    Iterator<Map.Entry<String, Object>> toPutMapIterator = toPutMap.entrySet().iterator();
    while (toPutMapIterator.hasNext()) {
      final Map.Entry<String, Object> newEntry = toPutMapIterator.next();
      final String newKey = newEntry.getKey();
      if (deletedKeyToTsMap.containsKey(newKey)) {
        toPutMapIterator.remove(); // This map entry was already deleted with a higher or equal timestamp.

      } else {
        // This map entry was not deleted before.
        KeyValPair newKeyValue = new KeyValPair(newKey);
        if (activeEntriesToTsMap.containsKey(newKeyValue)) {
          toPutMapIterator.remove(); // This map entry already exists in the map with a higher or equal timestamp.
        }
      }
    }

    // Step 2: Insert new put-only part map entries in the front.
    Map<String, Object> newMap = new IndexedHashMap<>();
    PrimitiveLongList newActiveTimestamps = new PrimitiveLongArrayList(activeEntriesToTsMap.size());
    collectionFieldRmd.setPutOnlyPartLength(toPutMap.size());
    // Add new entries for the put-only part.
    toPutMap.forEach(newMap::put);

    // Add entries and timestamps for the collection-merge part.
    activeEntriesToTsMap.forEach((activeEntry, activeTimestamp) -> {
      newMap.put(activeEntry.getKey(), activeEntry.getVal());
      newActiveTimestamps.add(activeTimestamp);
    });

    // Step 3: Set new map entries and new active timestamps.
    currValueRecord.put(fieldName, newMap);
    collectionFieldRmd.setActiveElementTimestamps(newActiveTimestamps);

    // Step 4: Set deleted keys and their deleted timestamps.
    List<String> newDeletedKeys = new ArrayList<>(deletedKeyToTsMap.size());
    PrimitiveLongList newDeletedTimestamps = new PrimitiveLongArrayList(deletedKeyToTsMap.size());
    deletedKeyToTsMap.forEach((key, ts) -> {
      newDeletedKeys.add(key);
      newDeletedTimestamps.add(ts);
    });

    collectionFieldRmd.setDeletedElementsAndTimestamps(newDeletedKeys, newDeletedTimestamps);
    if (collectionFieldRmd.isInPutOnlyState() && newFieldValue == null) {
      currValueRecord.put(fieldName, null);
    }
    return collectionFieldRmd.isInPutOnlyState()
        ? UpdateResultStatus.COMPLETELY_UPDATED
        : UpdateResultStatus.PARTIALLY_UPDATED;
  }

  @Override
  public UpdateResultStatus handleDeleteList(
      final long deleteTimestamp,
      final int coloID,
      CollectionRmdTimestamp collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName) {
    if (ignoreIncomingRequest(deleteTimestamp, coloID, collectionFieldRmd)) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    validateFieldSchemaType(currValueRecord, fieldName, Schema.Type.ARRAY, true);
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

    // All elements in the current put-only part should be removed.
    final int remainingElementsStartIdx = currPutOnlyPartLength + removedActiveTimestampsCount;
    if (remainingElementsStartIdx == 0) {
      // This indicates no put only item exists prior to DELETE operation and also no active items are removed.
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    Iterator<Object> currListIterator = currList.listIterator(remainingElementsStartIdx);
    List<Object> remainingList =
        new ArrayList<>(currList.size() - currPutOnlyPartLength - removedActiveTimestampsCount);
    while (currListIterator.hasNext()) {
      remainingList.add(currListIterator.next());
    }

    currValueRecord.put(fieldName, remainingList);
    return collectionFieldRmd.isInPutOnlyState()
        ? UpdateResultStatus.COMPLETELY_UPDATED
        : UpdateResultStatus.PARTIALLY_UPDATED;
  }

  @Override
  public UpdateResultStatus handleDeleteMap(
      final long deleteTimestamp,
      final int coloID,
      CollectionRmdTimestamp<String> collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName) {
    if (ignoreIncomingRequest(deleteTimestamp, coloID, collectionFieldRmd)) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    validateFieldSchemaType(currValueRecord, fieldName, Schema.Type.MAP, true);
    // Handle Delete on a map that is in the collection-merge mode.
    final int originalPutOnlyPartLength = collectionFieldRmd.getPutOnlyPartLength();
    final long originalTopLevelFieldTimestamp = collectionFieldRmd.getTopLevelFieldTimestamp();
    collectionFieldRmd.setTopLevelFieldTimestamp(deleteTimestamp);
    collectionFieldRmd.setTopLevelColoID(coloID);
    collectionFieldRmd.setPutOnlyPartLength(0); // No put-only part because it should be deleted completely.

    if (collectionFieldRmd.isInPutOnlyState()) {
      currValueRecord.put(fieldName, new IndexedHashMap<>(0));
      return UpdateResultStatus.COMPLETELY_UPDATED;
    }

    // Step 1: Remove all deleted map keys and their deleted timestamps with smaller or equal timestamps.
    collectionFieldRmd.removeDeletionInfoWithTimestampsLowerOrEqualTo(deleteTimestamp);

    // Step 2: Remove all active entries with smaller or equal timestamps.
    final int removedActiveTimestampsCount = collectionFieldRmd.removeActiveTimestampsLowerOrEqualTo(deleteTimestamp);
    IndexedHashMap<String, Object> currMap = (IndexedHashMap<String, Object>) currValueRecord.get(fieldName);

    // All map entries in the current put-only part should be removed.
    final int remainingEntriesStartIdx = originalPutOnlyPartLength + removedActiveTimestampsCount;
    if (remainingEntriesStartIdx == 0) {
      // This indicates no put only item exists prior to DELETE operation and also no active items are removed.
      collectionFieldRmd.setTopLevelFieldTimestamp(originalTopLevelFieldTimestamp);
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    Map<String, Object> remainingMap = new IndexedHashMap<>();
    for (int i = remainingEntriesStartIdx; i < currMap.size(); i++) {
      Map.Entry<String, Object> remainingEntry = currMap.getByIndex(i);
      remainingMap.put(remainingEntry.getKey(), remainingEntry.getValue());
    }
    currValueRecord.put(fieldName, remainingMap);
    return collectionFieldRmd.isInPutOnlyState()
        ? UpdateResultStatus.COMPLETELY_UPDATED
        : UpdateResultStatus.PARTIALLY_UPDATED;
  }

  @Override
  public UpdateResultStatus handleModifyList(
      final long modifyTimestamp,
      CollectionRmdTimestamp<Object> collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName,
      List<Object> toAddElements,
      List<Object> toRemoveElements) {
    // When timestamps are the same, full put/delete on a collection field always takes precedence over collection
    // merge.
    if (ignoreIncomingRequest(modifyTimestamp, Integer.MIN_VALUE, collectionFieldRmd)) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    validateFieldSchemaType(currValueRecord, fieldName, Schema.Type.ARRAY, true);
    Set<Object> toAddElementSet = new HashSet<>(toAddElements);
    Set<Object> toRemoveElementSet = new HashSet<>(toRemoveElements);
    removeIntersectionElements(toAddElementSet, toRemoveElementSet);
    if (toAddElements.isEmpty() && toRemoveElementSet.isEmpty()) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }

    if (collectionFieldRmd.isInPutOnlyState()) {
      return handleModifyPutOnlyList(
          modifyTimestamp,
          collectionFieldRmd,
          currValueRecord,
          fieldName,
          toAddElementSet,
          toRemoveElementSet);
    } else {
      return handleModifyCollectionMergeList(
          modifyTimestamp,
          collectionFieldRmd,
          currValueRecord,
          fieldName,
          toAddElementSet,
          toRemoveElementSet);
    }
  }

  private UpdateResultStatus handleModifyPutOnlyList(
      final long modifyTimestamp,
      CollectionRmdTimestamp<Object> collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName,
      Set<Object> toAddElementSet,
      Set<Object> toRemoveElementSet) {
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
    final IndexedHashMap<Object, Long> activeElementToTsMap = Utils.createElementToActiveTsMap(
        currElements,
        Collections.emptyList(),
        collectionFieldRmd.getTopLevelFieldTimestamp(),
        Long.MIN_VALUE,
        currElements.size());

    int newPutOnlyPartLength = currElements.size();
    // Step 1: Add elements (SET_UNION).
    for (Object toAddElement: toAddElementSet) {
      final Long activeTimestamp = activeElementToTsMap.get(toAddElement);
      if (activeTimestamp == null) {
        activeElementToTsMap.put(toAddElement, modifyTimestamp);

      } else if (activeTimestamp >= modifyTimestamp) {
        throw new IllegalStateException(
            "If active timestamp exists for this element, it must be strictly smaller than "
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

    final Comparator<Object> listElementComparator = getListElementComparator(currValueRecord, fieldName);
    sortElementAndTimestampList(deletedElementAndTsList, listElementComparator);

    // Step 3: Set new active elements and their active timestamps.
    List<ElementAndTimestamp> activeElementAndTsList = new ArrayList<>(activeElementToTsMap.size());
    for (Map.Entry<Object, Long> entry: activeElementToTsMap.entrySet()) {
      activeElementAndTsList.add(new ElementAndTimestamp(entry.getKey(), entry.getValue()));
    }
    sortElementAndTimestampList(
        // Only sort the collection-merge part of the list and leave the put-only part as is.
        activeElementAndTsList.subList(newPutOnlyPartLength, activeElementAndTsList.size()),
        listElementComparator);
    setNewListActiveElementAndTs(
        activeElementAndTsList,
        newPutOnlyPartLength,
        currValueRecord,
        fieldName,
        collectionFieldRmd);

    // Step 4: Set new deleted elements and their deleted timestamps.
    setDeletedDeletedElementAndTsList(deletedElementAndTsList, collectionFieldRmd);

    // Must be partially updated because there must be elements added and/or removed.
    return UpdateResultStatus.PARTIALLY_UPDATED;
  }

  private Comparator<Object> getListElementComparator(GenericRecord currValueRecord, String fieldName) {
    Supplier<Schema> elementSchemaSupplier = () -> getArraySchema(currValueRecord, fieldName).getElementType();
    // TODO: handle the situation where two elements have different schemas (e.g. element schema evolution). Assume
    // element schemas are always the same for now.
    return (o1, o2) -> this.avroElementComparator.compare(o1, o2, elementSchemaSupplier.get());
  }

  // Current list must be in the collection-merge state where the current list has 2 parts with the first part being
  // the put-only part and the second part being the collection-merge part.
  private UpdateResultStatus handleModifyCollectionMergeList(
      final long modifyTimestamp,
      CollectionRmdTimestamp<Object> collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName,
      Set<Object> toAddElementSet,
      Set<Object> toRemoveElementSet) {
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
    final IndexedHashMap<Object, Long> activeElementToTsMap = Utils.createElementToActiveTsMap(
        currElements,
        activeTimestamps,
        collectionFieldRmd.getTopLevelFieldTimestamp(),
        Long.MIN_VALUE,
        collectionFieldRmd.getPutOnlyPartLength());

    final List<Object> deletedElements = collectionFieldRmd.getDeletedElements();
    final List<Long> deletedTimestamps = collectionFieldRmd.getDeletedElementTimestamps();
    final IndexedHashMap<Object, Long> deletedElementToTsMap =
        Utils.createDeletedElementToTsMap(deletedElements, deletedTimestamps, Long.MIN_VALUE);

    boolean updated = false;
    int newPutOnlyPartLength = collectionFieldRmd.getPutOnlyPartLength();
    final long topLevelTimestamp = collectionFieldRmd.getTopLevelFieldTimestamp();
    // Step 1: Add elements (SET_UNION).
    for (Object toAddElement: toAddElementSet) {
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
      if (activeTimestamp == null) {
        activeElementToTsMap.put(toAddElement, modifyTimestamp);
        updated = true;
      } else if (activeTimestamp != modifyTimestamp) {
        // activeElementToTsMap.remove(toAddElement);
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
    final Comparator<Object> listElementComparator = getListElementComparator(currValueRecord, fieldName);
    sortElementAndTimestampList(
        // Only sort the collection-merge part of the list and leave the put-only part as is.
        activeElementAndTsList.subList(newPutOnlyPartLength, activeElementAndTsList.size()),
        listElementComparator);
    setNewListActiveElementAndTs(
        activeElementAndTsList,
        newPutOnlyPartLength,
        currValueRecord,
        fieldName,
        collectionFieldRmd);

    // Step 4: Set new deleted elements and their deleted timestamps.
    final List<ElementAndTimestamp> deletedElementAndTsList = new ArrayList<>(deletedElementToTsMap.size());
    deletedElementToTsMap.forEach((element, timestamp) -> {
      deletedElementAndTsList.add(new ElementAndTimestamp(element, timestamp));
    });

    sortElementAndTimestampList(deletedElementAndTsList, listElementComparator);
    setDeletedDeletedElementAndTsList(deletedElementAndTsList, collectionFieldRmd);

    return updated ? UpdateResultStatus.PARTIALLY_UPDATED : UpdateResultStatus.NOT_UPDATED_AT_ALL;
  }

  private void setNewListActiveElementAndTs(
      List<ElementAndTimestamp> activeElementAndTsList,
      int newPutOnlyPartLength,
      GenericRecord currValueRecord,
      String fieldName,
      CollectionRmdTimestamp<Object> collectionFieldRmd) {
    List<Object> newActiveElements = new ArrayList<>(activeElementAndTsList.size());
    PrimitiveLongList newActiveTimestamps =
        new PrimitiveLongArrayList(activeElementAndTsList.size() - newPutOnlyPartLength);
    int idx = 0;
    for (ElementAndTimestamp activeElementAndTs: activeElementAndTsList) {
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
      CollectionRmdTimestamp<Object> collectionFieldRmd) {
    List<Object> deletedElements = new ArrayList<>(deletedElementAndTsList.size());
    PrimitiveLongList deletedTimestamps = new PrimitiveLongArrayList(deletedElementAndTsList.size());
    for (ElementAndTimestamp deletedElementAndTs: deletedElementAndTsList) {
      deletedElements.add(deletedElementAndTs.getElement());
      deletedTimestamps.add(deletedElementAndTs.getTimestamp());
    }
    collectionFieldRmd.setDeletedElementsAndTimestamps(deletedElements, deletedTimestamps);
  }

  private void sortElementAndTimestampList(
      List<ElementAndTimestamp> elementAndTsList,
      Comparator<Object> elementComparator) {
    elementAndTsList.sort((o1, o2) -> {
      final int timestampCompareResult = Long.compare(o1.getTimestamp(), o2.getTimestamp());
      if (timestampCompareResult == 0) {
        return elementComparator.compare(o1.getElement(), o2.getElement());
      }
      return timestampCompareResult;
    });
  }

  private void sortElementAndTimestampListInMap(
      List<ElementAndTimestamp> elementAndTsList,
      Function<Object, String> keyExtractor) {
    Comparator<Object> mapKeyComparator = Comparator.comparing(x -> (String) x);
    elementAndTsList.sort((o1, o2) -> {
      final int timestampCompareResult = Long.compare(o1.getTimestamp(), o2.getTimestamp());
      if (timestampCompareResult == 0) {
        return mapKeyComparator.compare(keyExtractor.apply(o1.getElement()), keyExtractor.apply(o2.getElement()));
      }
      return timestampCompareResult;
    });
  }

  private void removeIntersectionElements(Set<Object> set1, Set<Object> set2) {
    List<Object> intersectionElements = new ArrayList<>();
    for (Object element: set1) {
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
      CollectionRmdTimestamp<String> collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName,
      Map<String, Object> newEntries,
      List<String> toRemoveKeys) {
    // When timestamps are the same, full put/delete on a collection field always takes precedence over collection
    // merge.
    if (ignoreIncomingRequest(modifyTimestamp, Integer.MIN_VALUE, collectionFieldRmd)) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    validateFieldSchemaType(currValueRecord, fieldName, Schema.Type.MAP, true);
    if (toRemoveKeys.isEmpty() && newEntries.isEmpty()) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    for (String toRemoveKey: toRemoveKeys) {
      newEntries.remove(toRemoveKey);
    }
    if (collectionFieldRmd.isInPutOnlyState()) {
      return handleModifyPutOnlyMap(
          modifyTimestamp,
          collectionFieldRmd,
          currValueRecord,
          fieldName,
          newEntries,
          toRemoveKeys);
    } else {
      return handleModifyCollectionMergeMap(
          modifyTimestamp,
          collectionFieldRmd,
          currValueRecord,
          fieldName,
          newEntries,
          toRemoveKeys);
    }
  }

  private UpdateResultStatus handleModifyPutOnlyMap(
      final long modifyTimestamp,
      CollectionRmdTimestamp<String> collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName,
      Map<String, Object> newEntries,
      List<String> toRemoveKeys) {
    if (!collectionFieldRmd.isInPutOnlyState()) {
      throw new IllegalStateException("Expect map to be in the put-only state.");
    }
    if (newEntries.isEmpty() && toRemoveKeys.isEmpty()) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    IndexedHashMap<String, Object> currMap = (IndexedHashMap<String, Object>) currValueRecord.get(fieldName);
    final IndexedHashMap<String, Object> putOnlyPartMap =
        currMap == null ? new IndexedHashMap<>() : new IndexedHashMap<>(currMap);
    final List<String> collectionMergePartKeys = new ArrayList<>();
    final List<String> deletedKeys = new ArrayList<>();

    // Step 1: Add elements (MAP_UNION).
    for (String newKey: newEntries.keySet()) {
      putOnlyPartMap.remove(newKey);
      collectionMergePartKeys.add(newKey);
    }
    collectionMergePartKeys.sort(String::compareTo);

    // Step 2: Remove elements (MAP_DIFF).
    for (String toRemoveKey: toRemoveKeys) {
      putOnlyPartMap.remove(toRemoveKey);
      deletedKeys.add(toRemoveKey);
    }
    deletedKeys.sort(String::compareTo);

    // Step 3: Set new active elements and their active timestamps.
    setNewMapActiveElementAndTs(
        putOnlyPartMap,
        collectionMergePartKeys,
        newEntries,
        currValueRecord,
        fieldName,
        collectionFieldRmd,
        modifyTimestamp);

    // Step 4: Set new deleted elements and their deleted timestamps.
    setDeletedMapKeyAndTs(deletedKeys, collectionFieldRmd, modifyTimestamp);

    // Must be partially updated because there must be map entries added and/or removed.
    return UpdateResultStatus.PARTIALLY_UPDATED;
  }

  private void setNewMapActiveElementAndTs(
      final IndexedHashMap<String, Object> putOnlyPartMap,
      final List<String> collectionMergePartKeys,
      Map<String, Object> newEntries,
      GenericRecord currValueRecord,
      String fieldName,
      CollectionRmdTimestamp<String> collectionFieldRmd,
      final long modifyTimestamp) {
    final int newPutOnlyPartLength = putOnlyPartMap.size();
    final IndexedHashMap<String, Object> resMap = putOnlyPartMap; // Rename. It means that resMap starts with the
                                                                  // put-only part.
    for (String newKey: collectionMergePartKeys) {
      resMap.put(newKey, newEntries.get(newKey));
    }
    PrimitiveLongList newActiveTimestamps = new PrimitiveLongArrayList(collectionMergePartKeys.size());
    for (int i = 0; i < collectionMergePartKeys.size(); i++) {
      newActiveTimestamps.add(modifyTimestamp);
    }
    collectionFieldRmd.setActiveElementTimestamps(newActiveTimestamps);
    currValueRecord.put(fieldName, resMap);
    collectionFieldRmd.setPutOnlyPartLength(newPutOnlyPartLength);
  }

  private void setDeletedMapKeyAndTs(
      List<String> deletedKeys,
      CollectionRmdTimestamp<String> collectionFieldRmd,
      final long deleteTimestamp) {
    PrimitiveLongList newDeletedTimestamps = new PrimitiveLongArrayList(deletedKeys.size());
    for (int i = 0; i < deletedKeys.size(); i++) {
      newDeletedTimestamps.add(deleteTimestamp);
    }
    collectionFieldRmd.setDeletedElementsAndTimestamps(deletedKeys, newDeletedTimestamps);
  }

  private UpdateResultStatus handleModifyCollectionMergeMap(
      final long modifyTimestamp,
      CollectionRmdTimestamp<String> collectionFieldRmd,
      GenericRecord currValueRecord,
      String fieldName,
      Map<String, Object> newEntries,
      List<String> toRemoveKeys) {
    if (collectionFieldRmd.isInPutOnlyState()) {
      throw new IllegalStateException("Expect map to be in the collection-merge state. Field name: " + fieldName);
    }
    if (newEntries.isEmpty() && toRemoveKeys.isEmpty()) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }
    IndexedHashMap<String, Object> currMap = (IndexedHashMap<String, Object>) currValueRecord.get(fieldName);
    List<KeyValPair> currKeyValPairs = new ArrayList<>(currMap.size());
    currMap.forEach((key, value) -> currKeyValPairs.add(new KeyValPair(key, value)));

    final List<Long> activeTimestamps = collectionFieldRmd.getActiveElementTimestamps();
    final IndexedHashMap<KeyValPair, Long> activeEntriesToTsMap = Utils.createElementToActiveTsMap(
        currKeyValPairs,
        activeTimestamps,
        collectionFieldRmd.getTopLevelFieldTimestamp(),
        Long.MIN_VALUE,
        collectionFieldRmd.getPutOnlyPartLength());

    final List<String> deletedKeys = collectionFieldRmd.getDeletedElements();
    final List<Long> deletedTimestamps = collectionFieldRmd.getDeletedElementTimestamps();
    final IndexedHashMap<String, Long> deletedKeyToTsMap =
        Utils.createDeletedElementToTsMap(deletedKeys, deletedTimestamps, Long.MIN_VALUE);

    boolean updated = false;
    int newPutOnlyPartLength = collectionFieldRmd.getPutOnlyPartLength();
    final long topLevelTimestamp = collectionFieldRmd.getTopLevelFieldTimestamp();

    // Step 1: Add elements (MAP_UNION).
    for (Map.Entry<String, Object> newEntry: newEntries.entrySet()) {
      final String newKey = newEntry.getKey();
      final Long deletedTimestamp = deletedKeyToTsMap.get(newKey);
      if (deletedTimestamp != null) {
        // Key was deleted before.
        if (deletedTimestamp < modifyTimestamp) {
          // k-v entry will be added back.
          deletedKeyToTsMap.remove(newKey);
          activeEntriesToTsMap.put(new KeyValPair(newKey, newEntry.getValue()), modifyTimestamp);
          updated = true;
        } // Else: Key remains "deleted".

      } else {
        // Key was not deleted before.
        KeyValPair newKeyValue = new KeyValPair(newKey, newEntry.getValue());
        final Long activeTimestamp = activeEntriesToTsMap.get(newKeyValue);
        if (activeTimestamp == null) {
          // The key does not exist before.
          activeEntriesToTsMap.put(newKeyValue, modifyTimestamp);
          updated = true;
        } else {
          // The key exist.
          if (activeTimestamp == topLevelTimestamp) {
            newPutOnlyPartLength--;
          }
          if (activeTimestamp < modifyTimestamp) {
            activeEntriesToTsMap.remove(newKeyValue);
            activeEntriesToTsMap.put(newKeyValue, modifyTimestamp);
            updated = true;

          } else if (activeTimestamp == modifyTimestamp) {
            // Note that if the current active timestamp is equal to the modify timestamp, we compare value.
            Object currentValue = currMap.get(newKey);
            Object newValue = newKeyValue.getVal();
            Schema fieldSchema = currValueRecord.getSchema().getField(fieldName).schema();
            if (shouldUpdateMapFieldItemValueWithSameTs(currentValue, newValue, fieldSchema)) {
              activeEntriesToTsMap.remove(newKeyValue);
              activeEntriesToTsMap.put(newKeyValue, modifyTimestamp);
              updated = true;
            }
          }
        }
      }
    }

    // Step 2: Remove elements (MAP_DIFF).
    for (String toRemoveKey: toRemoveKeys) {
      final Long deletedTimestamp = deletedKeyToTsMap.get(toRemoveKey);
      if (deletedTimestamp != null) {
        // This key was deleted before.
        if (deletedTimestamp < modifyTimestamp) {
          // Update the deleted timestamp of this key.
          deletedKeyToTsMap.put(toRemoveKey, modifyTimestamp);
          updated = true;
        }
      } else {
        // This key was not deleted before and now it is deleted.
        final KeyValPair toRemove = new KeyValPair(toRemoveKey);
        final Long activeTimestamp = activeEntriesToTsMap.get(toRemove);
        if (activeTimestamp != null) {
          if (activeTimestamp <= modifyTimestamp) {
            // Delete an existing k-v entry.
            activeEntriesToTsMap.remove(toRemove);
            if (activeTimestamp == topLevelTimestamp) {
              // Delete a k-v pair from the put-only part.
              newPutOnlyPartLength--;
            }
            deletedKeyToTsMap.put(toRemoveKey, modifyTimestamp);
            updated = true;
          } // Else: existing k-v entry does not get deleted.
        } else {
          // Key never existed and it should be marked as deleted now.
          deletedKeyToTsMap.put(toRemoveKey, modifyTimestamp);
          updated = true;
        }
      }
    }

    if (!updated) {
      return UpdateResultStatus.NOT_UPDATED_AT_ALL;
    }

    // Step 3: Set new active map entries and their active timestamps.
    final List<ElementAndTimestamp> newActiveEntriesAndTsList = new ArrayList<>(activeEntriesToTsMap.size());
    activeEntriesToTsMap.forEach((activeEntry, activeTs) -> {
      newActiveEntriesAndTsList.add(new ElementAndTimestamp(activeEntry, activeTs));
    });
    sortElementAndTimestampListInMap(
        newActiveEntriesAndTsList.subList(newPutOnlyPartLength, newActiveEntriesAndTsList.size()),
        x -> ((KeyValPair) x).getKey());
    setNewMapActiveElementAndTs(
        newActiveEntriesAndTsList,
        newPutOnlyPartLength,
        currValueRecord,
        fieldName,
        collectionFieldRmd);

    // Step 4: Set new deleted keys and their deleted timestamps.
    final List<ElementAndTimestamp> newDeletedKeyAndTsList = new ArrayList<>(deletedKeyToTsMap.size());

    deletedKeyToTsMap.forEach((k, v) -> newDeletedKeyAndTsList.add(new ElementAndTimestamp(k, v)));

    // The element here is String (as deleted key). So, we can use a String comparator.
    sortElementAndTimestampListInMap(newDeletedKeyAndTsList, x -> (String) x);
    setDeletedDeletedKeyAndTsList(newDeletedKeyAndTsList, collectionFieldRmd);
    return UpdateResultStatus.PARTIALLY_UPDATED;
  }

  private Schema getArraySchema(GenericRecord currValueRecord, String arrayFieldName) {
    Schema arrayFieldSchema = currValueRecord.getSchema().getField(arrayFieldName).schema();
    switch (arrayFieldSchema.getType()) {
      case ARRAY:
        return arrayFieldSchema;
      case UNION:
        return getSchemaFromNullableCollectionSchema(arrayFieldSchema, Schema.Type.ARRAY);

      default:
        throw new IllegalStateException("Expect an array or a union schema. Got: " + arrayFieldSchema);
    }
  }

  private Schema getSchemaFromNullableCollectionSchema(Schema nullableSchema, Schema.Type expectedNullableType) {
    if (nullableSchema.getType() != Schema.Type.UNION) {
      throw new IllegalStateException(
          "Expect nullable " + expectedNullableType + " schema. But got: " + nullableSchema);
    }
    List<Schema> unionedSchemas = nullableSchema.getTypes();
    if (unionedSchemas.size() != 2) {
      throw new IllegalStateException(
          "Expect nullable " + expectedNullableType + " schema. But got: " + nullableSchema);
    }
    Schema schema = unionedSchemas.get(1);
    if (unionedSchemas.get(0).getType() != Schema.Type.NULL && schema.getType() != expectedNullableType) {
      throw new IllegalStateException(
          "Expect nullable " + expectedNullableType + " schema. But got: " + nullableSchema);
    }
    return schema;
  }

  private void setNewMapActiveElementAndTs(
      List<ElementAndTimestamp> activeElementAndTsList,
      final int newPutOnlyPartLength,
      GenericRecord currValueRecord,
      String fieldName,
      CollectionRmdTimestamp<String> collectionFieldRmd) {
    Map<String, Object> newMap = new IndexedHashMap<>();
    PrimitiveLongList newActiveTimestamps =
        new PrimitiveLongArrayList(activeElementAndTsList.size() - newPutOnlyPartLength);
    int idx = 0;
    for (ElementAndTimestamp activeEntryAndTs: activeElementAndTsList) {
      KeyValPair activeEntry = (KeyValPair) activeEntryAndTs.getElement();
      newMap.put(activeEntry.getKey(), activeEntry.getVal());
      if (idx >= newPutOnlyPartLength) {
        newActiveTimestamps.add(activeEntryAndTs.getTimestamp());
      }
      idx++;
    }
    collectionFieldRmd.setActiveElementTimestamps(newActiveTimestamps);
    currValueRecord.put(fieldName, newMap);
    collectionFieldRmd.setPutOnlyPartLength(newPutOnlyPartLength);
  }

  private void setDeletedDeletedKeyAndTsList(
      List<ElementAndTimestamp> deletedElementAndTsList,
      CollectionRmdTimestamp<String> collectionFieldRmd) {
    List<String> deletedKeys = new ArrayList<>(deletedElementAndTsList.size());
    PrimitiveLongList deletedTimestamps = new PrimitiveLongArrayList(deletedElementAndTsList.size());
    for (ElementAndTimestamp deletedKeyAndTs: deletedElementAndTsList) {
      deletedKeys.add((String) deletedKeyAndTs.getElement());
      deletedTimestamps.add(deletedKeyAndTs.getTimestamp());
    }
    collectionFieldRmd.setDeletedElementsAndTimestamps(deletedKeys, deletedTimestamps);
  }

  private boolean ignoreIncomingRequest(
      final long incomingRequestTimestamp,
      final int incomingRequestColoID,
      CollectionRmdTimestamp<?> currCollectionFieldRmd) {
    if (currCollectionFieldRmd.getTopLevelFieldTimestamp() > incomingRequestTimestamp) {
      return true;

    } else if (currCollectionFieldRmd.getTopLevelFieldTimestamp() == incomingRequestTimestamp) {
      // When both top-level timestamp and colo ID are the same, always let the incoming request win assuming that the
      // incoming request has a higher offset.
      return currCollectionFieldRmd.getTopLevelColoID() > incomingRequestColoID;
    }
    return false;
  }

  private boolean shouldUpdateMapFieldItemValueWithSameTs(Object currentValue, Object newValue, Schema fieldSchema) {
    /**
     * For complex map item value type, for example union type [null, item value type], it is possible that the item
     * value can be null. This is the safeguard to not compare with the null value and always let the not-null value win
     * in the given case.
     */
    if (currentValue == null) {
      return true;
    }
    if (newValue == null) {
      return false;
    }
    Schema mapValueSchema = null;
    if (fieldSchema.isUnion()) {
      for (Schema schema: fieldSchema.getTypes()) {
        if (schema.getType().equals(Schema.Type.MAP)) {
          mapValueSchema = schema.getValueType();
          break;
        }
      }
    } else {
      mapValueSchema = fieldSchema.getValueType();
    }
    if (mapValueSchema == null) {
      throw new VeniceException("Could not find map schema in map field: " + fieldSchema.toString(true));
    }
    return AvroCollectionElementComparator.INSTANCE.compare(newValue, currentValue, mapValueSchema) > 0;
  }
}
