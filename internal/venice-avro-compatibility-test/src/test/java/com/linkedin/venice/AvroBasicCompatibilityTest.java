package com.linkedin.venice;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelperCommon;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.VeniceSerializationException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroBasicCompatibilityTest {
  private static final Logger LOGGER = LogManager.getLogger(AvroBasicCompatibilityTest.class);

  @Test
  public void testFullCompatibilityWithEnumEvolution() {
    // Initial schema has an enum field with a default
    Schema schema1 = AvroCompatibilityHelper.parse(
        "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
            + "           \"fields\" : [\n" + "               {\n" + "                 \"name\": \"Suit\", \n"
            + "                 \"type\": {\n"
            + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEARTS\", \"CLUBS\"],\n"
            + "                        \"default\": \"SPADES\"\n" + "                } \n" + "              }\n"
            + "           ]\n" + "        }");
    GenericRecord schema1SpadesRecord = getRecordWithSuitEnum(schema1, "SPADES");
    GenericRecord schema1HeartRecord = getRecordWithSuitEnum(schema1, "HEARTS");

    // New schema removes some symbols from enum field that has a default
    Schema schema2 = AvroCompatibilityHelper.parse(
        "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
            + "           \"fields\" : [\n" + "               {\n" + "                 \"name\": \"Suit\", \n"
            + "                 \"type\": {\n"
            + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"CLUBS\"],\n"
            + "                        \"default\": \"SPADES\"\n" + "                } \n" + "              }\n"
            + "           ]\n" + "        }");
    GenericRecord schema2SpadesRecord = getRecordWithSuitEnum(schema2, "SPADES");

    // New schema adds previously unknown symbols to enum field that has a default
    Schema schema3 = AvroCompatibilityHelper.parse(
        "{\n" + "           \"type\": \"record\",\n" + "           \"name\": \"KeyRecord\",\n"
            + "           \"fields\" : [\n" + "               {\n" + "                 \"name\": \"Suit\", \n"
            + "                 \"type\": {\n"
            + "                        \"name\": \"SuitType\", \"type\": \"enum\", \"symbols\": [\"SPADES\", \"DIAMONDS\", \"HEARTS\", \"CLUBS\", \"UNKNOWN\"],\n"
            + "                        \"default\": \"SPADES\"\n" + "                } \n" + "              }\n"
            + "           ]\n" + "        }");
    GenericRecord schema3SpadesRecord = getRecordWithSuitEnum(schema3, "SPADES");
    GenericRecord schema3UnknownRecord = getRecordWithSuitEnum(schema3, "UNKNOWN");

    AvroVersion currentAvroVersion = AvroCompatibilityHelperCommon.getRuntimeAvroVersion();
    LOGGER.info("Current Avro version: {}", currentAvroVersion);

    boolean enumEvolutionSupported = currentAvroVersion.laterThan(AvroVersion.AVRO_1_8);

    // SPADES is present in all schemas, so it should be deserializable either way
    testCompatibility(schema1SpadesRecord, schema2, "SPADES");
    testCompatibility(schema2SpadesRecord, schema1, "SPADES");
    testCompatibility(schema1SpadesRecord, schema3, "SPADES");
    testCompatibility(schema3SpadesRecord, schema1, "SPADES");
    testCompatibility(schema2SpadesRecord, schema3, "SPADES");
    testCompatibility(schema3SpadesRecord, schema2, "SPADES");

    // HEARTS is removed in schema2, so it should be deserializable only if enum evolution is supported
    testCompatibility(schema1HeartRecord, schema2, enumEvolutionSupported ? "SPADES" : null);

    // UNKNOWN is added in schema3, so it should be deserializable only if enum evolution is supported
    testCompatibility(schema3UnknownRecord, schema1, enumEvolutionSupported ? "SPADES" : null);
    testCompatibility(schema3UnknownRecord, schema2, enumEvolutionSupported ? "SPADES" : null);
  }

  private void testCompatibility(GenericRecord writer, Schema readerSchema, String expectedValue) {
    Schema writerSchema = writer.getSchema();
    RecordSerializer serializer = FastSerializerDeserializerFactory.getFastAvroGenericSerializer(writerSchema);
    byte[] serialized = serializer.serialize(writer);

    boolean compatibilityExpected = expectedValue != null;
    try {
      RecordDeserializer deserializer =
          FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(writerSchema, readerSchema);
      GenericRecord deserializedRecord = (GenericRecord) deserializer.deserialize(serialized);
      // If we reach here, it means deserialization was successful
      Assert.assertTrue(compatibilityExpected, "Expected incompatibility but got no exception");
      Assert.assertEquals(deserializedRecord.get("Suit").toString(), expectedValue);
    } catch (VeniceSerializationException e) {
      // If we reach here, it means deserialization failed
      Assert.assertFalse(compatibilityExpected, "Expected compatibility but got exception: " + e);
    }
  }

  private GenericRecord getRecordWithSuitEnum(Schema schema, String symbol) {
    GenericRecord record = new GenericData.Record(schema);
    GenericData.EnumSymbol enumSymbol = AvroCompatibilityHelper.newEnumSymbol(schema.getField("Suit").schema(), symbol);
    record.put("Suit", enumSymbol);
    return record;
  }
}
