package com.linkedin.venice.schema.merge;

import com.linkedin.venice.utils.lazy.Lazy;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.Validate;


/**
 * This class holds a value of type {@param T} and its corresponding replication metadata.
 */
public class ValueAndRmd<T> {
  private Lazy<T> value;
  private GenericRecord rmd;
  private boolean updateIgnored; // Whether we should skip the incoming message since it could be a stale message.
  private int valueSchemaID;

  public ValueAndRmd(Lazy<T> value, @Nonnull GenericRecord rmd) {
    Validate.notNull(rmd);
    this.value = value;
    this.rmd = rmd;
    this.valueSchemaID = -1;
  }

  public T getValue() {
    return value.get();
  }

  public void setValue(T value) {
    this.value = Lazy.of(() -> value);
  }

  public GenericRecord getRmd() {
    return rmd;
  }

  public void setRmd(GenericRecord rmd) {
    this.rmd = rmd;
  }

  public void setUpdateIgnored(boolean updateIgnored) {
    this.updateIgnored = updateIgnored;
  }

  public boolean isUpdateIgnored() {
    return updateIgnored;
  }

  public void setValueSchemaID(int valueSchemaID) {
    this.valueSchemaID = valueSchemaID;
  }

  public int getValueSchemaID() {
    return this.valueSchemaID;
  }
}
