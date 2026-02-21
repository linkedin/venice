package com.linkedin.davinci.store.view;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.meta.MaterializedViewParameters;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.views.ViewUtils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ViewWriterUtilsTest {
  @Test
  public void testGetVeniceViewWriter() {
    Store mockStore = Mockito.mock(Store.class);
    Version version = new VersionImpl("test-store", 1, "dummyPushId");
    Mockito.when(mockStore.getVersionOrThrow(1)).thenReturn(version);
    VeniceProperties props = VeniceProperties.empty();
    Object2IntMap<String> urlMappingMap = new Object2IntOpenHashMap<>();
    CompletableFuture<PubSubProduceResult> mockFuture = Mockito.mock(CompletableFuture.class);

    VeniceWriter mockVeniceWriter = Mockito.mock(VeniceWriter.class);
    Mockito.when(mockVeniceWriter.put(Mockito.any(), Mockito.any(), Mockito.anyInt())).thenReturn(mockFuture);

    VeniceServerConfig mockVeniceServerConfig = Mockito.mock(VeniceServerConfig.class);
    Mockito.when(mockVeniceServerConfig.getKafkaClusterUrlToIdMap()).thenReturn(urlMappingMap);
    VeniceWriterFactory mockVeniceWriterFactory = Mockito.mock(VeniceWriterFactory.class);

    VeniceConfigLoader mockVeniceConfigLoader = Mockito.mock(VeniceConfigLoader.class);
    Mockito.when(mockVeniceConfigLoader.getCombinedProperties()).thenReturn(props);
    Mockito.when(mockVeniceConfigLoader.getVeniceServerConfig()).thenReturn(mockVeniceServerConfig);

    Map<String, String> viewParams = new MaterializedViewParameters.Builder("test-view").setPartitionCount(12)
        .setPartitioner(DefaultVenicePartitioner.class.getCanonicalName())
        .build();
    VeniceView veniceView = ViewUtils.getVeniceView(
        MaterializedView.class.getCanonicalName(),
        mockVeniceConfigLoader.getCombinedProperties().toProperties(),
        "test-store",
        viewParams);
    VeniceViewWriter viewWriter = ViewWriterUtils.getVeniceViewWriter(
        MaterializedView.class.getCanonicalName(),
        mockVeniceConfigLoader,
        mockStore,
        1,
        viewParams,
        mockVeniceWriterFactory);

    Assert.assertTrue(viewWriter instanceof MaterializedViewWriter);
    Assert.assertTrue(veniceView instanceof MaterializedView);
  }

}
