package com.linkedin.venice.fastclient;

import static com.linkedin.venice.fastclient.utils.ClientTestUtils.REQUEST_TYPES_SMALL;
import static com.linkedin.venice.fastclient.utils.ClientTestUtils.STORE_METADATA_FETCH_MODES;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_KB;
import static com.linkedin.venice.utils.ByteUtils.BYTES_PER_MB;
import static org.testng.Assert.assertFalse;

import com.github.luben.zstd.ZstdDictTrainer;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.read.RequestType;
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
  /**
   * We override this data provider since we don't need to test the full suite of permutations.
   */
  @Override
  @DataProvider(name = "FastClient-Test-Permutations")
  public Object[][] fastClientTestPermutations() {
    return DataProviderUtils.allPermutationGenerator((permutation) -> {
      boolean retryEnabled = (boolean) permutation[2];
      if (retryEnabled) {
        return false;
      }
      int batchGetKeySize = (int) permutation[3];
      RequestType requestType = (RequestType) permutation[4];
      StoreMetadataFetchMode storeMetadataFetchMode = (StoreMetadataFetchMode) permutation[5];
      if (requestType != RequestType.MULTI_GET && requestType != RequestType.MULTI_GET_STREAMING) {
        if (batchGetKeySize != (int) BATCH_GET_KEY_SIZE.get(0)) {
          // these parameters are related only to batchGet, so just allowing 1 set
          // to avoid duplicate tests
          return false;
        }
      }

      if (storeMetadataFetchMode != StoreMetadataFetchMode.SERVER_BASED_METADATA) {
        return false;
      }
      return true;
    },
        DataProviderUtils.BOOLEAN_FALSE, // dualRead
        DataProviderUtils.BOOLEAN, // enableGrpc
        DataProviderUtils.BOOLEAN, // retryEnabled
        BATCH_GET_KEY_SIZE.toArray(), // batchGetKeySize
        REQUEST_TYPES_SMALL, // requestType
        STORE_METADATA_FETCH_MODES); // storeMetadataFetchMode
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
    veniceCluster
        .useControllerClient(
            client -> assertFalse(
                client
                    .updateStore(
                        storeName,
                        new UpdateStoreQueryParams().setStorageNodeReadQuotaEnabled(true)
                            .setReadComputationEnabled(true))
                    .isError()));
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;
  }
}
