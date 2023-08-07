package com.linkedin.venice;

import static com.linkedin.venice.Arg.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiReplicaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateClusterConfigQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.metadata.response.VersionProperties;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAdminTool {
  @Test
  public void testPrintObject() {
    List<String> output = new ArrayList<>();
    Consumer<String> printCapture = (String s) -> output.add(s);

    MultiReplicaResponse multiReplicaResponse = new MultiReplicaResponse();
    AdminTool.printObject(multiReplicaResponse, printCapture);

    Assert.assertFalse(
        output.get(output.size() - 1).contains("topic"),
        "Printed multi-replica response should not contain a topic field");
  }

  @Test(enabled = false) // disable until SSL config file becomes a mandatory config
  public void testAdminToolRequiresSSLConfigFile() {
    String[] args =
        { "--delete-store", "--url", "https://localhost:7036", "--cluster", "test-cluster", "--store", "testStore" };
    try {
      AdminTool.main(args);
    } catch (Exception e) {
      // AdminTool should enforce the rule that SSL config file must be included
      Assert.assertTrue(e.getMessage().contains("SSL config file path must be specified"));
    }
  }

  @Test
  public void testAdminUpdateStoreArg() throws ParseException, IOException {
    final String K1 = "k1", V1 = "v1", K2 = "k2", V2 = "v2", K3 = "k3", V3 = "v3";
    String[] args = { "--update-store", "--url", "http://localhost:7036", "--cluster", "test-cluster", "--store",
        "testStore", "--rmd-chunking-enabled", "true", "--partitioner-params",
        K1 + "=" + V1 + "," + K2 + "=" + V2 + "," + K3 + "=" + V3 };

    CommandLine commandLine = AdminTool.getCommandLine(args);
    UpdateStoreQueryParams params = AdminTool.getUpdateStoreQueryParams(commandLine);
    Assert.assertTrue(params.getRmdChunkingEnabled().isPresent());
    Assert.assertTrue(params.getRmdChunkingEnabled().get());
    Optional<Map<String, String>> partitionerParams = params.getPartitionerParams();
    Assert.assertTrue(partitionerParams.isPresent());
    Map<String, String> partitionerParamsMap = partitionerParams.get();
    Assert.assertEquals(partitionerParamsMap.get(K1), V1);
    Assert.assertEquals(partitionerParamsMap.get(K2), V2);
    Assert.assertEquals(partitionerParamsMap.get(K3), V3);
  }

  @Test
  public void testAdminUpdateClusterConfigArg() throws ParseException, IOException {
    String controllerUrl = "controllerUrl";
    String clusterName = "clusterName";
    String regionName = "region0";
    int kafkaFetchQuota = 1000;

    String[] args = { "--update-cluster-config", "--url", controllerUrl, "--cluster", clusterName, "--fabric",
        regionName, "--" + SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND.getArgName(), String.valueOf(kafkaFetchQuota) };

    CommandLine commandLine = AdminTool.getCommandLine(args);
    UpdateClusterConfigQueryParams params = AdminTool.getUpdateClusterConfigQueryParams(commandLine);
    Optional<Map<String, Integer>> serverKafkaFetchQuotaRecordsPerSecond =
        params.getServerKafkaFetchQuotaRecordsPerSecond();
    Assert.assertTrue(serverKafkaFetchQuotaRecordsPerSecond.isPresent(), "Kafka fetch quota not parsed from args");
    Assert.assertTrue(
        serverKafkaFetchQuotaRecordsPerSecond.get().containsKey(regionName),
        "Kafka fetch quota does not have info for region");
    Assert.assertEquals(
        (int) serverKafkaFetchQuotaRecordsPerSecond.get().get(regionName),
        kafkaFetchQuota,
        "Kafka fetch quota has incorrect info for region");
  }

  @Test
  public void testIsClonedStoreOnline() {
    String storeName = "testStore";
    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    ControllerClient srcControllerClient = mock(ControllerClient.class);
    ControllerClient destControllerClient = mock(ControllerClient.class);

    StoreResponse srcStoreResponse = new StoreResponse();
    StoreInfo srcStore = createStore(storeName, true);
    srcStore.setStoreMetaSystemStoreEnabled(true);
    srcStoreResponse.setStore(srcStore);
    doReturn(srcStoreResponse).when(srcControllerClient).getStore(storeName);
    StoreResponse srcMetaSystemStoreResponse = new StoreResponse();
    StoreInfo srcMetaSystemStore = createStore(metaSystemStoreName, true);
    srcMetaSystemStoreResponse.setStore(srcMetaSystemStore);
    doReturn(srcMetaSystemStoreResponse).when(srcControllerClient).getStore(metaSystemStoreName);

    StoreResponse destStoreResponse = new StoreResponse();
    StoreInfo destStore = createStore(storeName, false);
    destStoreResponse.setStore(destStore);
    doReturn(destStoreResponse).when(destControllerClient).getStore(storeName);
    StoreResponse destMetaSystemStoreResponse = new StoreResponse();
    StoreInfo destMetaSystemStore = createStore(metaSystemStoreName, false);
    destMetaSystemStoreResponse.setStore(destMetaSystemStore);
    doReturn(destMetaSystemStoreResponse).when(destControllerClient).getStore(metaSystemStoreName);

    Assert.assertFalse(AdminTool.isClonedStoreOnline(srcControllerClient, destControllerClient, storeName));
  }

  private StoreInfo createStore(String storeName, boolean hasOnlineVersion) {
    StoreInfo storeInfo = new StoreInfo();
    if (hasOnlineVersion) {
      Version version = new VersionImpl(storeName, 1, "pushJobId");
      version.setStatus(VersionStatus.ONLINE);
      storeInfo.setVersions(Collections.singletonList(version));
    } else {
      storeInfo.setVersions(Collections.emptyList());
    }
    return storeInfo;
  }

  @Test
  public void testGetPubSubTopicConfigsRequiresValidTopicName() {
    String badTopicName = "badTopicName_v_rt_0";
    String[] args =
        { "--get-kafka-topic-configs", "--url", "http://localhost:7036", "--kafka-topic-name", badTopicName };
    Assert.assertThrows(VeniceException.class, () -> AdminTool.main(args));
  }

  @Test
  public void testAdminTopicIsAllowedByTopicConfigsRelatedApi() {
    String topicName = "venice_admin_testCluster";
    String[] args = { "--update-kafka-topic-retention", "--url", "http://localhost:7036", "--kafka-topic-name",
        topicName, "--kafka-topic-retention-in-ms", "1000" };
    try {
      AdminTool.main(args);
    } catch (Exception e) {
      Assert.fail("AdminTool should allow admin topic to be updated by config update API", e);
    }

    String[] args2 = { "--get-kafka-topic-configs", "--url", "http://localhost:7036", "--kafka-topic-name", topicName };
    try {
      AdminTool.main(args2);
    } catch (Exception e) {
      Assert.fail("AdminTool should allow admin topic to be queried by config query API", e);
    }
  }

  @Test
  public void testAdminToolDataRecoveryApi() {
    String storeNames = "test1,test2,test3";

    String[] estimateArgs = { "--estimate-data-recovery-time", "--url", "http://localhost:7036", "--stores", storeNames,
        "--dest-fabric", "ei-ltx1" };

    String[] executeArgs = { "--execute-data-recovery", "--recovery-command", "venice-tools", "--extra-command-args",
        "repush kafka --force --format", "--url", "http://localhost:7036", "--stores", storeNames, "--source-fabric",
        "ei4", "--dest-fabric", "ei-ltx1", "--non-interactive", "--datetime", "2023-06-27T14:19:25" };

    String[] monitorArgs = { "--monitor-data-recovery", "--url", "http://localhost:7036", "--stores", storeNames,
        "--dest-fabric", "ei-ltx1", "--datetime", "2023-06-27T14:19:25", "--interval", "300" };

    String[] estimateArgs2 = { "--estimate-data-recovery-time", "--url", "http://localhost:7036", "--cluster",
        "venice-1", "--dest-fabric", "ei-ltx1" };

    String[][] commands = { estimateArgs, estimateArgs2, executeArgs, monitorArgs };
    try {
      for (String[] command: commands) {
        AdminTool.main(command);
      }
    } catch (VeniceClientException e) {
      // Expected exception.
    } catch (Exception err) {
      Assert.fail("Unexpected exception happens in data recovery APIs: ", err);
    }
  }

  @Test
  public void testAdminToolRequestBasedMetadata()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    String storeName = "test-store1";
    String[] getMetadataArgs = { "--request-based-metadata", "--url", "http://localhost:7036", "--server-url",
        "http://localhost:7036", "--store", storeName };
    VeniceException requestException =
        Assert.expectThrows(VeniceException.class, () -> AdminTool.main(getMetadataArgs));
    Assert.assertTrue(requestException.getMessage().contains("Unable to discover cluster for store"));
    String[] getMetadataArgsSSL = { "--request-based-metadata", "--url", "https://localhost:7036", "--server-url",
        "https://localhost:7036", "--store", storeName };
    VeniceException sslException = Assert.expectThrows(VeniceException.class, () -> AdminTool.main(getMetadataArgsSSL));
    Assert.assertTrue(sslException.getMessage().contains("requires admin tool to be executed with cert"));

    TransportClient transportClient = mock(TransportClient.class);
    CompletableFuture<TransportClientResponse> completableFuture = mock(CompletableFuture.class);
    TransportClientResponse response = mock(TransportClientResponse.class);
    RecordSerializer<MetadataResponseRecord> metadataResponseSerializer =
        FastSerializerDeserializerFactory.getFastAvroGenericSerializer(MetadataResponseRecord.SCHEMA$);
    MetadataResponseRecord record = new MetadataResponseRecord();
    record.setRoutingInfo(Collections.singletonMap("0", Collections.singletonList("host1")));
    record.setVersions(Collections.singletonList(1));
    record.setHelixGroupInfo(Collections.emptyMap());
    VersionProperties versionProperties = new VersionProperties();
    versionProperties.setCurrentVersion(1);
    versionProperties.setAmplificationFactor(1);
    versionProperties.setCompressionStrategy(1);
    versionProperties.setPartitionCount(1);
    versionProperties.setPartitionerClass("com.linkedin.venice.partitioner.DefaultVenicePartitioner");
    versionProperties.setPartitionerParams(Collections.emptyMap());
    record.setVersionMetadata(versionProperties);
    record.setKeySchema(Collections.singletonMap("1", "\"string\""));
    record.setValueSchemas(Collections.singletonMap("1", "\"string\""));
    record.setLatestSuperSetValueSchemaId(1);
    byte[] responseByte = metadataResponseSerializer.serialize(record);
    doReturn(responseByte).when(response).getBody();
    doReturn(AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getCurrentProtocolVersion()).when(response).getSchemaId();
    doReturn(response).when(completableFuture).get();
    String requestBasedMetadataURL = QueryAction.METADATA.toString().toLowerCase() + "/" + storeName;
    doReturn(completableFuture).when(transportClient).get(requestBasedMetadataURL);
    ControllerClient controllerClient = mock(ControllerClient.class);
    SchemaResponse schemaResponse = mock(SchemaResponse.class);
    doReturn(false).when(schemaResponse).isError();
    doReturn(MetadataResponseRecord.SCHEMA$.toString()).when(schemaResponse).getSchemaStr();
    doReturn(schemaResponse).when(controllerClient)
        .getValueSchema(
            AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getSystemStoreName(),
            AvroProtocolDefinition.SERVER_METADATA_RESPONSE.getCurrentProtocolVersion());
    AdminTool.getAndPrintRequestBasedMetadata(transportClient, controllerClient, "http://localhost:7036", storeName);
  }
}
