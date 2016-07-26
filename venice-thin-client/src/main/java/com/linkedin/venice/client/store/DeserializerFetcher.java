package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.serializer.RecordDeserializer;

/**
 * Define the interface for retrieving value schema by the given schema id.
 * The expected customer is {@link com.linkedin.venice.client.store.transport.TransportClient},
 * which will use this class to retrieve {@link RecordDeserializer} to do response deserialization.
 * @param <T>
 */
public interface DeserializerFetcher<T> {

  RecordDeserializer<T> fetch(int schemaId) throws VeniceClientException;
}
