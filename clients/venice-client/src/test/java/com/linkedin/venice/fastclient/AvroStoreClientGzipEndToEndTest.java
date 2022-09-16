package com.linkedin.venice.fastclient;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.fastclient.transport.HttpClient5BasedR2Client;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.SslUtils;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.DataProvider;


public class AvroStoreClientGzipEndToEndTest extends AvroStoreClientEndToEndTest {
  @DataProvider(name = "useDaVinciClientBasedMetadata")
  public static Object[][] useDaVinciClientBasedMetadata() {
    return new Object[][] { { true } };
  }

  protected Client constructR2Client() throws Exception {
    return HttpClient5BasedR2Client.getR2Client(SslUtils.getVeniceLocalSslFactory().getSSLContext(), 8);
  }

  protected void prepareData() throws Exception {
    keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_STR);
    valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_STR);

    for (int i = 0; i < recordCnt; ++i) {
      GenericRecord record = new GenericData.Record(VALUE_SCHEMA);
      record.put(VALUE_FIELD_NAME, i);
    }
    Stream<Map.Entry> genericRecordStream = IntStream.range(0, recordCnt).mapToObj(i -> {
      GenericRecord record = new GenericData.Record(VALUE_SCHEMA);
      record.put(VALUE_FIELD_NAME, i);
      return new AbstractMap.SimpleEntry<>(keyPrefix + i, record);
    });

    storeName = veniceCluster
        .createStore(KEY_SCHEMA_STR, VALUE_SCHEMA_STR, genericRecordStream, CompressionStrategy.GZIP, topic -> {
          storeVersionName = topic;
          return null;
        });
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;
  }

}
