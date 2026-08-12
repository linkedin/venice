package com.linkedin.venice.meta;

import static org.testng.Assert.assertEquals;

import com.linkedin.venice.ConfigConstants;
import com.linkedin.venice.helix.StoreJSONSerializer;
import java.io.IOException;
import org.testng.annotations.Test;


public class TestStoreJsonSerde {
  /** Need to hardcode this since it defaults to the current timestamp, and would mess up comparison. */
  private static final long LATEST_VERSION_PROMOTE_TO_CURRENT_TIMESTAMP = 1679322804317L;

  /** Legacy way to encode the {@link Store} in JSON format. */
  private static final String SERIALIZED_WITH_EMPTY_OPTIONAL = "{\n" + "  \"name\" : \"storeName\",\n"
      + "  \"owner\" : \"owner\",\n" + "  \"createdTime\" : 1,\n" + "  \"persistenceType\" : \"ROCKS_DB\",\n"
      + "  \"routingStrategy\" : \"CONSISTENT_HASH\",\n" + "  \"readStrategy\" : \"ANY_OF_ONLINE\",\n"
      + "  \"offLinePushStrategy\" : \"WAIT_ALL_REPLICAS\",\n" + "  \"currentVersion\" : 0,\n"
      + "  \"storageQuotaInByte\" : 21474836480,\n" + "  \"readQuotaInCU\" : 1800,\n"
      + "  \"hybridStoreConfig\" : null,\n" + "  \"partitionerConfig\" : {\n"
      + "    \"partitionerClass\" : \"com.linkedin.venice.partitioner.DefaultVenicePartitioner\",\n"
      + "    \"partitionerParams\" : { },\n" + "    \"amplificationFactor\" : 1\n" + "  },\n"
      + "  \"replicationFactor\" : 1,\n" + "  \"largestUsedVersionNumber\" : 0,\n"
      + "  \"clientDecompressionEnabled\" : true,\n" + "  \"bootstrapToOnlineTimeoutInHours\" : 24,\n"
      + "  \"nativeReplicationEnabled\" : false,\n" + "  \"schemaAutoRegisterFromPushJobEnabled\" : false,\n"
      + "  \"latestSuperSetValueSchemaId\" : -1,\n" + "  \"hybridStoreDiskQuotaEnabled\" : false,\n"
      + "  \"storeMetadataSystemStoreEnabled\" : false,\n" + "  \"storeMetaSystemStoreEnabled\" : false,\n"
      + "  \"latestVersionPromoteToCurrentTimestamp\" : " + LATEST_VERSION_PROMOTE_TO_CURRENT_TIMESTAMP + ",\n"
      + "  \"backupVersionRetentionMs\" : -1,\n" + "  \"nativeReplicationSourceFabric\" : \"\",\n"
      + "  \"activeActiveReplicationEnabled\" : false,\n" + "  \"daVinciPushStatusStoreEnabled\" : false,\n"
      + "  \"lowWatermark\" : 0,\n" + "  \"partitionCount\" : 0,\n" + "  \"enableWrites\" : true,\n"
      + "  \"enableReads\" : true,\n" + "  \"compressionStrategy\" : \"NO_OP\",\n" + "  \"chunkingEnabled\" : false,\n"
      + "  \"rmdChunkingEnabled\" : false,\n" + "  \"batchGetLimit\" : -1,\n"
      + "  \"incrementalPushEnabled\" : false,\n" + "  \"accessControlled\" : true,\n" + "  \"migrating\" : false,\n"
      + "  \"numVersionsToPreserve\" : 0,\n" + "  \"writeComputationEnabled\" : false,\n"
      + "  \"readComputationEnabled\" : false,\n" + "  \"pushStreamSourceAddress\" : \"\",\n"

      /**
       * The {@link Store#getRmdVersion()} used to be an improperly-encoded {@link java.util.Optional<Integer>}
       * We have since then renamed the field so that past occurrences of the badly-encoded field just get ignored.
       */
      + "  \"rmdVersionID\" : {\n" + "    \"empty\" : true,\n" + "    \"present\" : false\n" + "  },\n"
      + "  \"backupStrategy\" : \"DELETE_ON_NEW_PUSH_START\",\n" + "  \"etlStoreConfig\" : {\n"
      + "    \"etledUserProxyAccount\" : \"\",\n" + "    \"regularVersionETLEnabled\" : false,\n"
      + "    \"futureVersionETLEnabled\" : false\n" + "  },\n" + "  \"retentionTime\" : 432000000,\n"
      + "  \"migrationDuplicateStore\" : false,\n" + "  \"systemStores\" : { },\n" + "  \"versions\" : [ ],\n"
      + "  \"hybrid\" : false,\n" + "  \"systemStore\" : false,\n" + "  \"leaderFollowerModelEnabled\" : true,\n"
      + "  \"views\" : { }\n" + "}";

  @Test
  public void test() throws IOException {
    StoreJSONSerializer storeJSONSerializer = new StoreJSONSerializer();
    byte[] serializedStore = SERIALIZED_WITH_EMPTY_OPTIONAL.getBytes();
    Store deserializedStore = storeJSONSerializer.deserialize(serializedStore, "/");
    assertEquals(deserializedStore.getRmdVersion(), ConfigConstants.UNSPECIFIED_REPLICATION_METADATA_VERSION);
    byte[] reserializedStore = storeJSONSerializer.serialize(deserializedStore, "/");
    Store redeserializedStore = storeJSONSerializer.deserialize(reserializedStore, "/");
    assertEquals(redeserializedStore, deserializedStore);
    assertEquals(redeserializedStore.getRmdVersion(), ConfigConstants.UNSPECIFIED_REPLICATION_METADATA_VERSION);

    Store store = new ZKStore(
        "storeName",
        "owner",
        1L,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
    store.setLatestVersionPromoteToCurrentTimestamp(LATEST_VERSION_PROMOTE_TO_CURRENT_TIMESTAMP);
    assertEquals(store, deserializedStore);
    assertEquals(redeserializedStore, store);
  }
}
