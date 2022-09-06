package com.linkedin.venice.meta;

import org.apache.avro.specific.SpecificRecord;


public interface DataModelBackedStructure<T extends SpecificRecord> {
  /**
   * Return the backed data model.
   * @return
   */
  T dataModel();
}
