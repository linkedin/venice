package com.linkedin.venice.client.serializer;

import com.linkedin.venice.client.exceptions.VeniceClientException;

public interface RecordDeserializer<T> {

  T deserialize(byte[] bytes) throws VeniceClientException;
}
