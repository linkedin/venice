package com.linkedin.venice;

import static com.linkedin.venice.Arg.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND;

import com.linkedin.venice.controllerapi.MultiReplicaResponse;
import com.linkedin.venice.controllerapi.UpdateClusterConfigQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
}
