package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import io.opentelemetry.api.common.Attributes;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.util.Collections;


/**
 * Shared test helpers for blob transfer stats tests.
 */
class BlobTransferStatsTestUtils {
  static final String CLUSTER_NAME = "test-cluster";
  static final String METRIC_PREFIX = "server";

  static Store createStore(String storeName) {
    return new ZKStore(
        storeName,
        "",
        10,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
  }

  static VeniceServerConfig createMockServerConfig() {
    VeniceServerConfig mockConfig = mock(VeniceServerConfig.class);
    doReturn(Int2ObjectMaps.emptyMap()).when(mockConfig).getKafkaClusterIdToAliasMap();
    doReturn(true).when(mockConfig).isUnregisterMetricForDeletedStoreEnabled();
    doReturn(CLUSTER_NAME).when(mockConfig).getClusterName();
    return mockConfig;
  }

  static ReadOnlyStoreRepository createMockMetaRepository(Store store) {
    ReadOnlyStoreRepository mockMetaRepo = mock(ReadOnlyStoreRepository.class);
    doReturn(store).when(mockMetaRepo).getStoreOrThrow(any());
    doReturn(Collections.singletonList(store)).when(mockMetaRepo).getAllStores();
    return mockMetaRepo;
  }

  static Attributes buildVersionRoleAttributes(String storeName, String clusterName, VersionRole role) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), role.getDimensionValue())
        .build();
  }

  static Attributes buildResponseCountAttributes(
      String storeName,
      String clusterName,
      VersionRole role,
      VeniceResponseStatusCategory status) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), role.getDimensionValue())
        .put(VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(), status.getDimensionValue())
        .build();
  }

  private BlobTransferStatsTestUtils() {
  }
}
