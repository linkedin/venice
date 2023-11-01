package com.linkedin.venice.fastclient;

import static com.linkedin.venice.fastclient.utils.ClientTestUtils.REQUEST_TYPES_SMALL;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.DataProviderUtils;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.DataProvider;


public class AvroStoreClientGzipEndToEndTest extends AvroStoreClientEndToEndTest {
  /**
   * We override this data provider since we don't need to test the full suite of permutations.
   */
  @Override
  @DataProvider(name = "FastClient-Test-Permutations")
  public Object[][] fastClientTestPermutations() {
    return DataProviderUtils.allPermutationGenerator((permutation) -> {
      boolean speculativeQueryEnabled = (boolean) permutation[1];
      if (speculativeQueryEnabled) {
        return false;
      }
      boolean useStreamingBatchGetAsDefault = (boolean) permutation[2];
      boolean retryEnabled = (boolean) permutation[4];
      if (retryEnabled) {
        return false;
      }
      int batchGetKeySize = (int) permutation[5];
      RequestType requestType = (RequestType) permutation[6];
      StoreMetadataFetchMode storeMetadataFetchMode = (StoreMetadataFetchMode) permutation[7];
      if (requestType != RequestType.MULTI_GET && requestType != RequestType.MULTI_GET_STREAMING) {
        if (useStreamingBatchGetAsDefault || batchGetKeySize != (int) BATCH_GET_KEY_SIZE.get(0)) {
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
        DataProviderUtils.BOOLEAN_FALSE, // speculativeQueryEnabled
        DataProviderUtils.BOOLEAN_TRUE, // useStreamingBatchGetAsDefault
        DataProviderUtils.BOOLEAN, // enableGrpc
        DataProviderUtils.BOOLEAN, // retryEnabled
        BATCH_GET_KEY_SIZE.toArray(), // batchGetKeySize
        REQUEST_TYPES_SMALL); // requestType
  }

  @Override
  protected void prepareData() {
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
    veniceCluster.updateStore(storeName, new UpdateStoreQueryParams().setReadComputationEnabled(true));
    valueSchemaId = HelixReadOnlySchemaRepository.VALUE_SCHEMA_STARTING_ID;
  }

}
