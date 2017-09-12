package com.linkedin.venice.listener.response;

import com.linkedin.venice.common.PartitionOffsetMapUtils;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MultiGetResponseWrapper extends ReadResponse {
  private final Map<Integer, Long> partitionOffsetMap;
  private final List<MultiGetResponseRecordV1> records;

  public MultiGetResponseWrapper() {
    this.partitionOffsetMap = new HashMap<>();
    this.records = new ArrayList<>();
  }

  public void addRecord(MultiGetResponseRecordV1 record) {
    records.add(record);
  }

  public void addPartitionOffsetMapping(int partition, long offset) {
    partitionOffsetMap.put(partition, offset);
  }

  public String serializedPartitionOffsetMap() throws IOException {
    return PartitionOffsetMapUtils.serializedPartitionOffsetMap(partitionOffsetMap);
  }

  public byte[] serializedMultiGetResponse() {
    RecordSerializer<MultiGetResponseRecordV1> serializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);

    return serializer.serializeObjects(records);
  }

  public int getResponseSchemaId() {
    return ReadAvroProtocolDefinition.MULTI_GET_RESPONSE_V1.getProtocolVersion();
  }

  public int getRecordCount() {
    return records.size();
  }
}
