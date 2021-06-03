package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.ingestion.isolated.IsolatedIngestionServer;
import com.linkedin.davinci.ingestion.main.MainIngestionRequestClient;
import com.linkedin.venice.ingestion.protocol.InitializationConfigs;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils.*;
import static com.linkedin.venice.ConfigKeys.*;


public class IsolatedIngestionServerTest {
  @Test
  public void testShutdownAfterHeartbeatTimeout() throws IOException {
    int servicePort = Utils.getFreePort();
    Process isolatedIngestionService = ForkedJavaProcess.exec(
        IsolatedIngestionServer.class,
        Arrays.asList(String.valueOf(servicePort), String.valueOf(TimeUnit.SECONDS.toMillis(10))),
        new ArrayList<>(),
        Optional.empty()
    );

    MainIngestionRequestClient client = new MainIngestionRequestClient(servicePort);
    TestUtils.waitForNonDeterministicAssertion(3, TimeUnit.SECONDS, () -> {
      Assert.assertTrue(client.sendHeartbeatRequest());
    });
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertFalse(isolatedIngestionService.isAlive());
    });
  }

  @Test
  public void testCustomizedConfigs() throws Exception {
    VeniceProperties properties = new PropertyBuilder()
        .put(CLUSTER_NAME, TestUtils.getUniqueString())
        .put(ZOOKEEPER_ADDRESS, TestUtils.getUniqueString())
        .put(KAFKA_BOOTSTRAP_SERVERS, TestUtils.getUniqueString())
        .put(KAFKA_ZK_ADDRESS, TestUtils.getUniqueString())
        .put(D2_CLIENT_ZK_HOSTS_ADDRESS, TestUtils.getUniqueString())
        .put(SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS, 100)
        .put(INGESTION_ISOLATION_CONFIG_PREFIX + "." + SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS, 10)
        .build();
    VeniceConfigLoader configLoader = new VeniceConfigLoader(properties, properties);
    MainIngestionRequestClient client = new MainIngestionRequestClient(12345);
    InitializationConfigs initializationConfigs = client.buildInitializationConfig(configLoader);

    PropertyBuilder propertyBuilder = new PropertyBuilder();
    initializationConfigs.aggregatedConfigs.forEach((key, value) -> propertyBuilder.put(key.toString(), value));
    Assert.assertEquals(propertyBuilder.build().getInt(SERVER_PARTITION_GRACEFUL_DROP_DELAY_IN_SECONDS), 10);
  }
}
