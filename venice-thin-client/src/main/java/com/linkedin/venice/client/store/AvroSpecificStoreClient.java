package com.linkedin.venice.client.store;

import org.apache.avro.specific.SpecificRecord;

/**
 * AvroSpecificStoreClient for type safety purpose.
 * @param <V>
 */
public interface AvroSpecificStoreClient<V extends SpecificRecord> extends AvroGenericStoreClient<V> {
}
