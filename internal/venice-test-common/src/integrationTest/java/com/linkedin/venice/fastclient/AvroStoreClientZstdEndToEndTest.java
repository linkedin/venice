package com.linkedin.venice.fastclient;

import static com.linkedin.venice.fastclient.utils.ClientTestUtils.STORE_METADATA_FETCH_MODES;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_KB;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;

import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
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
  @Override
  @DataProvider(name = "FastClient-Five-Boolean-A-Number-Store-Metadata-Fetch-Mode")
  public Object[][] fiveBooleanANumberStoreMetadataFetchMode() {
    return DataProviderUtils.allPermutationGenerator((permutation) -> {
      boolean batchGet = (boolean) permutation[2];
      boolean useStreamingBatchGetAsDefault = (boolean) permutation[3];
      int batchGetKeySize = (int) permutation[5];
      StoreMetadataFetchMode storeMetadataFetchMode = (StoreMetadataFetchMode) permutation[6];
      if (!batchGet) {
        if (useStreamingBatchGetAsDefault || batchGetKeySize != (int) BATCH_GET_KEY_SIZE.get(0)) {
          // these parameters are related only to batchGet, so just allowing 1 set
          // to avoid duplicate tests
          return false;
        }
      }
      if (storeMetadataFetchMode == StoreMetadataFetchMode.DA_VINCI_CLIENT_BASED_METADATA) {
        return false;
      }
      return true;
    },
        DataProviderUtils.BOOLEAN_FALSE, // dualRead
        DataProviderUtils.BOOLEAN_FALSE, // speculativeQueryEnabled
        DataProviderUtils.BOOLEAN, // batchGet
        DataProviderUtils.BOOLEAN_TRUE, // useStreamingBatchGetAsDefault
        DataProviderUtils.BOOLEAN, // enableGrpc
        BATCH_GET_KEY_SIZE.toArray(), // batchGetKeySize
        STORE_METADATA_FETCH_MODES); // storeMetadataFetchMode
  }

  @Override
  @DataProvider(name = "FastClient-Three-Boolean-And-A-Number")
  public Object[][] threeBooleanAndANumber() {
    return DataProviderUtils.allPermutationGenerator((permutation) -> {
      boolean batchGet = (boolean) permutation[0];
      int batchGetKeySize = (int) permutation[3];
      if (!batchGet) {
        if (batchGetKeySize != (int) BATCH_GET_KEY_SIZE.get(0)) {
          // these parameters are related only to batchGet, so just allowing 1 set
          // to avoid duplicate tests
          return false;
        }
      }
      return true;
    },
        DataProviderUtils.BOOLEAN, // batchGet
        DataProviderUtils.BOOLEAN_FALSE, // dualRead
        DataProviderUtils.BOOLEAN_FALSE, // speculativeQueryEnabled
        BATCH_GET_KEY_SIZE.toArray());
  }

  /** the below tests are used for long tail retry cases which are flaky,
   * so disabling them for this class */
  @Override
  @DataProvider(name = "FastClient-One-Boolean")
  public Object[][] oneBoolean() {
    return DataProviderUtils.allPermutationGenerator((permutation) -> false, DataProviderUtils.BOOLEAN);
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
