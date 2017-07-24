package com.linkedin.venice.schema;

import com.linkedin.venice.serializer.AvroSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.HashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.testng.annotations.Test;


public class TestAvroSchema {
  @Test
  public void testCompatibilityForPartitionState() throws IOException {
    Schema v1Schema = Utils.getSchemaFromResource("avro/PartitionState/v1/PartitionState.avsc");
    Schema v3Schema = Utils.getSchemaFromResource("avro/PartitionState/v3/PartitionState.avsc");

    GenericData.Record v1Record = new GenericData.Record(v1Schema);
    v1Record.put("offset", new Long(123));
    v1Record.put("endOfPush", new Boolean(true));
    v1Record.put("lastUpdate", new Long(-1));
    v1Record.put("producerStates", new HashMap<>());

    RecordSerializer<Object> serializer = AvroSerializerDeserializerFactory.getAvroGenericSerializer(v1Schema);
    byte[] bytes = serializer.serialize(v1Record);

    // No exception should be thrown here
    RecordDeserializer<Object> deserializer = AvroSerializerDeserializerFactory.getAvroGenericDeserializer(v1Schema, v3Schema);
    deserializer.deserialize(bytes);
  }
}
