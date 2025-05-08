package com.linkedin.davinci.store.view;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.ChangeCaptureView;
import com.linkedin.venice.writer.VeniceWriterFactory;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceViewWriterFactoryTest {
  public static final String TEST_STORE = "testStore";
  private static final Schema SCHEMA = AvroCompatibilityHelper.parse("\"string\"");

  @Test
  public void testConstructStoreViewWriters() {
    VeniceProperties props = VeniceProperties.empty();
    Object2IntMap<String> urlMappingMap = new Object2IntOpenHashMap<>();
    VeniceServerConfig mockVeniceServerConfig = Mockito.mock(VeniceServerConfig.class);
    Mockito.when(mockVeniceServerConfig.getKafkaClusterUrlToIdMap()).thenReturn(urlMappingMap);
    VeniceConfigLoader mockVeniceConfigLoader = Mockito.mock(VeniceConfigLoader.class);
    Mockito.when(mockVeniceConfigLoader.getCombinedProperties()).thenReturn(props);
    PubSubClientsFactory mockPubSubClientsFactory = Mockito.mock(PubSubClientsFactory.class);
    PubSubProducerAdapterFactory mockPubSubProducerAdapterFactory = Mockito.mock(PubSubProducerAdapterFactory.class);
    Mockito.when(mockPubSubClientsFactory.getProducerAdapterFactory()).thenReturn(mockPubSubProducerAdapterFactory);
    Mockito.when(mockVeniceServerConfig.getPubSubClientsFactory()).thenReturn(mockPubSubClientsFactory);
    Mockito.when(mockVeniceConfigLoader.getCombinedProperties()).thenReturn(props);
    Mockito.when(mockVeniceConfigLoader.getVeniceServerConfig()).thenReturn(mockVeniceServerConfig);

    Map<String, ViewConfig> viewConfigMap = new HashMap<>();
    viewConfigMap.put("view1", new ViewConfigImpl(ChangeCaptureView.class.getCanonicalName(), Collections.emptyMap()));
    viewConfigMap.put("view2", new ViewConfigImpl(ChangeCaptureView.class.getCanonicalName(), Collections.emptyMap()));

    Store mockStore = Mockito.mock(Store.class);
    Version version = new VersionImpl(TEST_STORE, 1, "fooid");
    version.setViewConfigs(viewConfigMap);
    Mockito.when(mockStore.getVersionOrThrow(1)).thenReturn(version);
    Mockito.when(mockStore.getName()).thenReturn(TEST_STORE);

    VeniceWriterFactory mockVeniceWriterFactory = Mockito.mock(VeniceWriterFactory.class);
    VeniceViewWriterFactory viewWriterFactory =
        new VeniceViewWriterFactory(mockVeniceConfigLoader, mockVeniceWriterFactory);
    Map<String, VeniceViewWriter> viewWriterMap = viewWriterFactory.buildStoreViewWriters(mockStore, 1, SCHEMA);

    Assert.assertTrue(viewWriterMap.get("view1") instanceof ChangeCaptureViewWriter);
    Assert.assertTrue(viewWriterMap.get("view2") instanceof ChangeCaptureViewWriter);
    Assert.assertEquals(viewWriterMap.size(), 2);
  }
}
