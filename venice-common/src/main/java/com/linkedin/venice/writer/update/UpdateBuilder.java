package com.linkedin.venice.writer.update;

import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.Validate;

/**
 *  This class applies the builder pattern to build a partial update record. Note that to-be-updated value
 *  must be of type {@link Schema.Type#RECORD}.
 *
 *  It provides below ways to update a value record.
 *      1. Set a new value to a field.
 *      2. Add elements to a List/Array field.
 *      3. Remove elements from a List/Array field.
 *      4. Add entries to a Map field.
 *      5. Remove entries by keys from a map field.
 */
public abstract class UpdateBuilder {

  protected final Schema updateSchema;

  /**
   * @param updateSchema Update schema that is derived from the value Record schema.
   */
  UpdateBuilder(Schema updateSchema) {
    Validate.notNull(updateSchema);
    if (updateSchema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException();
    }
    this.updateSchema = updateSchema;
  }

  /**
   * Set a new value to a field. Note that setting value on the same field multiple times throws an {@link IllegalStateException}.
   * If this field is a List/Map field and other methods are used to add to or remove from this field, calling this method
   * throws an {@link IllegalStateException} as well.
   *
   * @param fieldName field name
   * @param newFieldValue new value that is going to be set to this field.
   */
  public abstract UpdateBuilder setNewFieldValue(String fieldName, Object newFieldValue);

  /**
   * Set elements to be added to a List/Array field.
   * Note that if a list field already has {@link this#setNewFieldValue} or this method called on it, calling this
   * method (again) throws an {@link IllegalStateException}.
   *
   * @param listFieldName name of a list field.
   * @param elementsToAdd Elements that are going to be added to this list field.
   */
  public abstract UpdateBuilder setElementsToAddToListField(String listFieldName, List<?> elementsToAdd);

  /**
   * Set elements to be removed from a List/Array field.
   * Note that if a list field already has {@link this#setNewFieldValue} or this method called on it, calling this
   * method (again) throws an {@link IllegalStateException}.
   *
   * @param listFieldName Name of a list field.
   * @param elementsToRemove Elements that are going to be removed from this list field.
   */
  public abstract UpdateBuilder setElementsToRemoveFromListField(String listFieldName, List<?> elementsToRemove);

  /**
   * Set k-v entries to be added to a Map field.
   * Note that if a map field already has {@link this#setNewFieldValue} or this method called on it, calling this
   * method (again) throws an {@link IllegalStateException}.
   *
   * @param mapFieldName Name of a map field.
   * @param entriesToAdd Entries that are going to be added to this map field.
   */
  public abstract UpdateBuilder setEntriesToAddToMapField(String mapFieldName, Map<String, ?> entriesToAdd);

  /**
   * Set keys to be added to a Map field.
   * Note that if a map field already has {@link this#setNewFieldValue} or this method called on it, calling this
   * method (again) throws an {@link IllegalStateException}.
   *
   * @param mapFieldName Name of a map field.
   * @param keysToRemove Keys of k-v entries that are going to be removed from this map field.
   */
  public abstract UpdateBuilder setKeysToRemoveFromMapField(String mapFieldName, List<String> keysToRemove);

  /**
   * Build a {@link GenericRecord} that is ready to be sent as an Update request.
   * @return A {@link GenericRecord} that is ready to be sent to Venice.
   */
  public abstract GenericRecord build();
}
