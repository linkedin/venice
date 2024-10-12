package com.linkedin.davinci.consumer;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.HostResolver;
import org.apache.kafka.clients.LeastLoadedNodeAlgorithm;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MetadataUpdater;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.testng.annotations.Test;


public class KafkaNetworkClientTest {
  @Test
  public void test() {
    MetadataUpdater metadataUpdater = mock(MetadataUpdater.class);
    List<Node> nodes = Collections.singletonList(mock(Node.class));
    when(metadataUpdater.fetchNodes()).thenReturn(nodes);
    when(metadataUpdater.isUpdateClusterMetadataDue(anyLong())).thenReturn(true);
    NetworkClient networkClient = new NetworkClient(
        metadataUpdater,
        mock(Metadata.class),
        mock(Selectable.class),
        "clientId",
        1,
        10,
        100,
        1024,
        1024,
        1000,
        1000,
        1000,
        Time.SYSTEM,
        false,
        new ApiVersions(),
        mock(Sensor.class),
        new LogContext(),
        mock(HostResolver.class),
        "",
        LeastLoadedNodeAlgorithm.VANILLA);

    assertThrows(ConfigException.class, () -> networkClient.leastLoadedNode(1));

    /**
     * In previous versions of Kafka, after the first exception above, there would be a flood of
     * {@link IllegalArgumentException} during subsequent calls to {@link NetworkClient#leastLoadedNode(long)}.
     *
     * This could happen during transient network issue (e.g. NIC temporarily going down) and flood the logs.
     *
     * The new version of Kafka returns null instead of throwing.
     */
    assertNull(networkClient.leastLoadedNode(1));
  }
}
