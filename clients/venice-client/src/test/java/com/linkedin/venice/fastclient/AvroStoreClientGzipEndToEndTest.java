package com.linkedin.venice.fastclient;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.fastclient.transport.HttpClient5BasedR2Client;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.SslUtils;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.DataProvider;


public class AvroStoreClientGzipEndToEndTest extends AvroStoreClientEndToEndTest {

  // useDaVinciClientBasedMetadata is always true as router based metadata store is considered legacy
  @Override
  @DataProvider(name = "FastClient-Four-Boolean-And-A-Number")
  public Object[][] fourBooleanAndANumber() {
    return DataProviderUtils.allPermutationGenerator(
        DataProviderUtils.BOOLEAN_TRUE,
        DataProviderUtils.BOOLEAN,
        DataProviderUtils.BOOLEAN,
        DataProviderUtils.BOOLEAN,
        BATCH_GET_KEY_SIZE);
  }

  // TODO why a different implementation for gzip only?
  @Override
  protected Client constructR2Client() throws Exception {
    return HttpClient5BasedR2Client.getR2Client(SslUtils.getVeniceLocalSslFactory().getSSLContext(), 8);
  }

  @Override
  protected void prepareData() throws Exception {
    keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_STR);
    valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_STR);

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
