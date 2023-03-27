package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.ROUTER_CLIENT_DECOMPRESSION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SIZE_LIMIT;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SIZE_MEASURE_INTERVAL;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE;
import static com.linkedin.venice.ConfigKeys.SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.runVPJ;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithCustomSize;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestDiskDatabaseLimiter {
  protected VeniceClusterWrapper veniceCluster;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    veniceCluster = ServiceFactory.getVeniceCluster(1, 0, 0);
    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.put(SERVER_PROMOTION_TO_LEADER_REPLICA_DELAY_SECONDS, Long.toString(1L));
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(SERVER_DATABASE_SYNC_BYTES_INTERNAL_FOR_DEFERRED_WRITE_MODE, "300");
    serverProperties.setProperty(SERVER_DATABASE_SIZE_LIMIT, Long.toString(10));
    serverProperties.setProperty(SERVER_DATABASE_SIZE_MEASURE_INTERVAL, Long.toString(10));
    veniceCluster.addVeniceServer(new Properties(), serverProperties);
    veniceCluster.addVeniceServer(new Properties(), serverProperties);

    Properties routerProperties = new Properties();
    routerProperties.put(ROUTER_CLIENT_DECOMPRESSION_ENABLED, "true");
    veniceCluster.addVeniceRouter(routerProperties);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Push job error.*")
  public void testPushJobFailureWhenHittingDatabaseLimit() {
    String storeName = Utils.getUniqueString("test_empty_push_store");
    try (ControllerClient controllerClient =
        new ControllerClient(veniceCluster.getClusterName(), veniceCluster.getAllControllersURLs())) {
      controllerClient.createNewStore(storeName, "owner", STRING_SCHEMA, STRING_SCHEMA);
      controllerClient
          .updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));
      File inputDir = getTempDataDirectory();
      String inputDirPath = "file://" + inputDir.getAbsolutePath();
      writeSimpleAvroFileWithCustomSize(inputDir, 1000, 1000, 5000);

      Properties vpjProperties = defaultVPJProps(veniceCluster, inputDirPath, storeName);
      runVPJ(vpjProperties, 1, controllerClient);
    } catch (IOException e) {
      throw new VeniceException(e);
    }
  }
}
