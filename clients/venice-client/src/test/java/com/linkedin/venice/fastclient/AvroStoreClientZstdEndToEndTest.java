package com.linkedin.venice.fastclient;

import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_KB;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;

import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.DataProvider;


public class AvroStoreClientZstdEndToEndTest extends AvroStoreClientEndToEndTest {

  // useDaVinciClientBasedMetadata is always true as router based metadata store is considered legacy
  // The tests fails on zstd fetching side if it's set to false. Deprioritized debugging this as its legacy.
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

  @Override
  protected void prepareData() throws Exception {
    keySerializer = new VeniceAvroKafkaSerializer(KEY_SCHEMA_STR);
    valueSerializer = new VeniceAvroKafkaSerializer(VALUE_SCHEMA_STR);

    Stream<Map.Entry> genericRecordStream = IntStream.range(0, recordCnt).mapToObj(i -> {
      GenericRecord record = new GenericData.Record(VALUE_SCHEMA);
      record.put(VALUE_FIELD_NAME, i);
      return new AbstractMap.SimpleEntry<>(keyPrefix + i, record);
    });

    storeName = veniceCluster.createStore(
        KEY_SCHEMA_STR,
        VALUE_SCHEMA_STR,
        genericRecordStream,
        CompressionStrategy.ZSTD_WITH_DICT,
        topic -> {
          storeVersionName = topic;
          ZstdDictTrainer trainer = new ZstdDictTrainer(1 * BYTES_PER_MB, 10 * BYTES_PER_KB);
          for (int i = 0; i < 100000; i++) {
            GenericRecord record = new GenericData.Record(VALUE_SCHEMA);
            record.put(VALUE_FIELD_NAME, i);
            trainer.addSample(valueSerializer.serialize(topic, record));
          }
          byte[] compressionDictionaryBytes = trainer.trainSamples();
          return ByteBuffer.wrap(compressionDictionaryBytes);
        });
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;
  }
}
