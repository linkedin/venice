package com.linkedin.davinci.store.rocksdb;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_RMD_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_SEPARATE_RMD_CACHE_ENABLED;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.AggVersionedStorageEngineStats;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStorageEngineTest;
import com.linkedin.davinci.store.StorageEngineAccessor;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.avro.Schema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


public class ReplicationMeadataRocksDBStoragePartitionCFTest extends ReplicationMetadataRocksDBStoragePartitionTest {
  private static final int PARTITION_ID = 0;

  private static final String storeName = Version.composeKafkaTopic(Utils.getUniqueString("rocksdb_store_test"), 1);
  private final ReadOnlyStoreRepository mockReadOnlyStoreRepository = mock(ReadOnlyStoreRepository.class);
  private static final int versionNumber = 0;
  private static final String topicName = Version.composeKafkaTopic(storeName, versionNumber);

  private StorageService storageService;
  private VeniceStoreVersionConfig storeConfig;

  @Override
  public void createStorageEngineForTest() {
    Version mockVersion = mock(Version.class);
    when(mockVersion.isActiveActiveReplicationEnabled()).thenReturn(true);
    Store mockStore = mock(Store.class);
    when(mockStore.getVersion(versionNumber)).thenReturn(mockVersion);
    when(mockReadOnlyStoreRepository.getStoreOrThrow(storeName)).thenReturn(mockStore);

    Properties properties = new Properties();
    properties.put(ROCKSDB_SEPARATE_RMD_CACHE_ENABLED, "true");
    properties.put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 1024 * 1024 * 1024L);
    properties.put(ROCKSDB_RMD_BLOCK_CACHE_SIZE_IN_BYTES, 1024 * 1024L);
    VeniceProperties serverProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.ROCKS_DB, properties);
    storageService = new StorageService(
        AbstractStorageEngineTest.getVeniceConfigLoader(serverProps),
        mock(AggVersionedStorageEngineStats.class),
        null,
        AvroProtocolDefinition.STORE_VERSION_STATE.getSerializer(),
        AvroProtocolDefinition.PARTITION_STATE.getSerializer(),
        mockReadOnlyStoreRepository);
    storeConfig = new VeniceStoreVersionConfig(topicName, serverProps, PersistenceType.ROCKS_DB);
    testStoreEngine = StorageEngineAccessor
        .getInnerStorageEngine(storageService.openStoreForNewPartition(storeConfig, PARTITION_ID, () -> null));
    createStoreForTest();
    String stringSchema = "\"string\"";
    Schema aaSchema = RmdSchemaGenerator.generateMetadataSchema(stringSchema, 1);
    ReadOnlySchemaRepository schemaRepository = mock(ReadOnlySchemaRepository.class);

    RmdSchemaEntry rmdSchemaEntry = new RmdSchemaEntry(1, 1, aaSchema);
    doReturn(rmdSchemaEntry).when(schemaRepository).getReplicationMetadataSchema(anyString(), anyInt(), anyInt());

    SchemaEntry valueSchemaEntry = new SchemaEntry(1, stringSchema);
    RmdSchemaEntry rmdSchemaEnry = new RmdSchemaEntry(1, 1, aaSchema);
    doReturn(valueSchemaEntry).when(schemaRepository).getSupersetOrLatestValueSchema(anyString());
    doReturn(rmdSchemaEnry).when(schemaRepository).getReplicationMetadataSchema(anyString(), anyInt(), anyInt());
  }

  @BeforeClass
  public void setUp() {
    createStorageEngineForTest();
  }

  @AfterClass
  public void cleanUp() throws Exception {
    storageService.dropStorePartition(storeConfig, PARTITION_ID);
    storageService.stop();
  }
}
