package com.linkedin.venice.endToEnd;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.metadata.response.StorePropertiesResponseRecord;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import java.util.Optional;
import org.apache.avro.Schema;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestServerStorePropertiesEndpoint extends AbstractClientEndToEndSetup {

  /*
   * @{link AbstractClientEndToEndSetup} enables the read quota for stores as part of prepareData and stores are
   * typically reused at a class level for tests. However, some tests require flows to disable read quota, and
   * it is important to reset so that these tests don't step on other tests. For now, we choose to add a before
   * method to class that validates disabled read quota behavior to reset the state to ensure other tests run
   * successfully.
   */
  @BeforeMethod
  public void enableStoreReadQuota() {
    veniceCluster.useControllerClient(controllerClient -> {
      TestUtils.assertCommand(
          controllerClient.updateStore(storeName, new UpdateStoreQueryParams().setStorageNodeReadQuotaEnabled(true)));
    });
  }

  @Test(timeOut = TIME_OUT)
  public void testRequestBasedStoreProperties() throws Exception {

    // SSL
    Optional<SSLFactory> sslFactory = Optional.of(SslUtils.getVeniceLocalSslFactory());

    // Server
    VeniceServerWrapper veniceServerWrapper = veniceCluster.getVeniceServers().stream().findAny().get();
    String serverUrl = "https://" + veniceServerWrapper.getHost() + ":" + veniceServerWrapper.getPort();

    // Request
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(serverUrl);
    clientConfig.setSslFactory(sslFactory.get());
    TransportClient transportClient = ClientFactory.getTransportClient(clientConfig);
    String requestUrl = QueryAction.STORE_PROPERTIES.toString().toLowerCase() + "/" + storeName;
    TransportClientResponse response = transportClient.get(requestUrl).get();

    // Deserialize
    Schema writerSchema = StorePropertiesResponseRecord.SCHEMA$;
    RecordDeserializer<StorePropertiesResponseRecord> recordDeserializer = FastSerializerDeserializerFactory
        .getFastAvroSpecificDeserializer(writerSchema, StorePropertiesResponseRecord.class);
    StorePropertiesResponseRecord record = recordDeserializer.deserialize(response.getBody());

    ObjectWriter jsonWriter = ObjectMapperFactory.getInstance().writerWithDefaultPrettyPrinter();
    Object printObject = ObjectMapperFactory.getInstance().readValue(record.toString(), Object.class);
    System.out.println(jsonWriter.writeValueAsString(printObject));

    // Assert
    Store expectedStore =
        veniceCluster.getLeaderVeniceController().getVeniceAdmin().getStore(veniceCluster.getClusterName(), storeName);

    assertNotNull(record);
    assertNotNull(record.storeMetaValue);
    assertNotNull(record.storeMetaValue.storeProperties);
    assertEquals(record.storeMetaValue.storeProperties.name.toString(), expectedStore.getName());
    assertEquals(record.storeMetaValue.storeProperties.owner.toString(), expectedStore.getOwner());
    assertEquals(record.storeMetaValue.storeProperties.createdTime, expectedStore.getCreatedTime());
    assertEquals(record.storeMetaValue.storeProperties.currentVersion, expectedStore.getCurrentVersion());
    assertEquals(record.storeMetaValue.storeProperties.storageQuotaInByte, expectedStore.getStorageQuotaInByte());
    assertEquals(record.storeMetaValue.storeProperties.readQuotaInCU, expectedStore.getReadQuotaInCU());
    assertEquals(
        record.storeMetaValue.storeProperties.largestUsedVersionNumber,
        expectedStore.getLargestUsedVersionNumber());
    assertEquals(
        record.storeMetaValue.storeProperties.latestVersionPromoteToCurrentTimestamp,
        expectedStore.getLatestVersionPromoteToCurrentTimestamp());
    assertEquals(record.storeMetaValue.storeProperties.versions.size(), expectedStore.getVersions().size());
    assertEquals(record.storeMetaValue.storeProperties.systemStores.size(), expectedStore.getSystemStores().size());
    assertNotNull(record.storeMetaValue.storeKeySchemas);
    assertNotNull(record.storeMetaValue.storeValueSchemas);
    assertNotNull(record.helixGroupInfo);
    assertNotNull(record.routingInfo);

    // Close
    veniceServerWrapper.close();
    veniceCluster.close();
  }
}
