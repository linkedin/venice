package com.linkedin.venice.etl.extractor;

import com.linkedin.venice.etl.VeniceKafkaDecodedRecord;
import com.linkedin.venice.etl.client.VeniceKafkaConsumerClient;
import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaExtractor;

import static com.linkedin.venice.etl.source.VeniceKafkaSource.*;


public class VeniceKafkaExtractor extends KafkaExtractor<Schema, GenericRecord> {
  private final Schema decodedSchema;

  public VeniceKafkaExtractor(WorkUnitState state) {
    super(state);

    // change the name of the schema to the topic name
    List<Schema.Field> outputSchemaFields = new ArrayList<>();
    for (Schema.Field field : VeniceKafkaDecodedRecord.SCHEMA$.getFields()) {
      outputSchemaFields.add(new Schema.Field(field.name(), field.schema(), field.doc(),
          field.defaultValue(), field.order()));
    }
    decodedSchema = Schema
        .createRecord(topicName, VeniceKafkaDecodedRecord.SCHEMA$.getDoc(),
            VeniceKafkaDecodedRecord.SCHEMA$.getNamespace(), VeniceKafkaDecodedRecord.SCHEMA$.isError());
    decodedSchema.setFields(outputSchemaFields);
  }

  @Override
  public Schema getSchema() {
    return decodedSchema;
  }

  @Override
  protected GenericRecord decodeRecord(ByteArrayBasedKafkaRecord kafkaConsumerRecord) throws IOException {
    GenericRecord decodedRecord = new GenericData.Record(decodedSchema);
    Map<String, String> metadata = new HashMap<>();

    decodedRecord.put(VENICE_ETL_KEY_FIELD, ByteBuffer.wrap(kafkaConsumerRecord.getKeyBytes()));
    /**
     * lumos use this offset field to do compaction;
     * the value with the biggest offset for the same key will remain in the snapshot
     **/
    decodedRecord.put(VENICE_ETL_OFFSET_FIELD, kafkaConsumerRecord.getOffset());
    byte[] valueBytes = kafkaConsumerRecord.getMessageBytes();
    if (null == valueBytes) {
      // delete message
      decodedRecord.put(VENICE_ETL_VALUE_FIELD, null);
      decodedRecord.put(VENICE_ETL_DELETED_TS_FIELD, kafkaConsumerRecord.getOffset());
      metadata.put(VENICE_ETL_SCHEMAID_FIELD, String.valueOf(0));
    } else {
      // put message
      // leave DELETED_TS field as null so that converter knows it's a delete message
      decodedRecord.put(VENICE_ETL_VALUE_FIELD, ByteBuffer.wrap(kafkaConsumerRecord.getMessageBytes()));
      if (kafkaConsumerRecord instanceof VeniceKafkaConsumerClient.VeniceKafkaRecord) {
        VeniceKafkaConsumerClient.VeniceKafkaRecord veniceRecord =
          (VeniceKafkaConsumerClient.VeniceKafkaRecord) kafkaConsumerRecord;
        metadata.put(VENICE_ETL_SCHEMAID_FIELD, String.valueOf(veniceRecord.getSchemaId()));
      } else {
        // this should never happen
        throw new VeniceException("The extractor expects the client returns a VeniceKafkaRecord");
      }
    }
    decodedRecord.put(VENICE_ETL_METADATA_FIELD, metadata);

    return decodedRecord;
  }
}
