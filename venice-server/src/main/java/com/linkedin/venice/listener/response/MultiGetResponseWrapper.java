package com.linkedin.venice.listener.response;

import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.AvroSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


public class MultiGetResponseWrapper extends ReadResponse {
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

  /**
   * Since we are planning to put this serialized bytes a header, so we need to do base64 encoding.
   * @return
   */
  public String serializedPartitionOffsetMap() throws IOException {
    return OBJECT_MAPPER.writeValueAsString(partitionOffsetMap);
  }

  public static Map<Integer, Long> deserializePartitionOffsetMap(String content) throws IOException {
    return OBJECT_MAPPER.readValue(content, new TypeReference<Map<Integer, Long>>(){});
  }

  public byte[] serializedMultiGetResponse() {
    RecordSerializer<MultiGetResponseRecordV1> serializer =
        AvroSerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);

    return serializer.serializeObjects(records);
  }

  public int getResponseSchemaId() {
    return ReadAvroProtocolDefinition.MULTI_GET_RESPONSE_V1.getProtocolVersion();
  }

  public int getRecordCount() {
    return records.size();
  }
}
