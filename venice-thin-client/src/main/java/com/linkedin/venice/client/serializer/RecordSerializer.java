package com.linkedin.venice.client.serializer;

import com.linkedin.venice.client.exceptions.VeniceClientException;

public interface RecordSerializer<T> {

  byte[] serialize(T object) throws VeniceClientException;
}
