package com.linkedin.venice.client.store.predicate;

import com.linkedin.venice.annotation.Experimental;
import org.apache.avro.generic.GenericRecord;


/**
 * Functor interface for performing a predicate test on {@link GenericRecord}
 */
@Experimental
public interface Predicate {
  @Experimental
  boolean evaluate(GenericRecord genericRecord);

}
