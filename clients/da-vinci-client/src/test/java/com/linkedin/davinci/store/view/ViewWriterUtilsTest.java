package com.linkedin.davinci.store.view;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pubsub.api.PubSubClientsFactory;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterFactory;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.ChangeCaptureView;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.views.ViewUtils;
import com.linkedin.venice.writer.VeniceWriter;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Collections;
import java.util.concurrent.Future;
import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ViewWriterUtilsTest {
  private static final Schema SCHEMA = AvroCompatibilityHelper.parse("\"string\"");

  @Test
  public void testGetVeniceViewWriter() {
    Store mockStore = Mockito.mock(Store.class);
    VeniceProperties props = VeniceProperties.empty();
    Object2IntMap<String> urlMappingMap = new Object2IntOpenHashMap<>();
    Future<PubSubProduceResult> mockFuture = Mockito.mock(Future.class);

    VeniceWriter mockVeniceWriter = Mockito.mock(VeniceWriter.class);
    Mockito.when(mockVeniceWriter.put(Mockito.any(), Mockito.any(), Mockito.anyInt())).thenReturn(mockFuture);

    VeniceServerConfig mockVeniceServerConfig = Mockito.mock(VeniceServerConfig.class);
    Mockito.when(mockVeniceServerConfig.getKafkaClusterUrlToIdMap()).thenReturn(urlMappingMap);
    PubSubProducerAdapterFactory mockPubSubProducerAdapterFactory = Mockito.mock(PubSubProducerAdapterFactory.class);
    PubSubClientsFactory mockPubSubClientsFactory = Mockito.mock(PubSubClientsFactory.class);
    Mockito.when(mockPubSubClientsFactory.getProducerAdapterFactory()).thenReturn(mockPubSubProducerAdapterFactory);
    Mockito.when(mockVeniceServerConfig.getPubSubClientsFactory()).thenReturn(mockPubSubClientsFactory);

    VeniceConfigLoader mockVeniceConfigLoader = Mockito.mock(VeniceConfigLoader.class);
    Mockito.when(mockVeniceConfigLoader.getCombinedProperties()).thenReturn(props);
    Mockito.when(mockVeniceConfigLoader.getVeniceServerConfig()).thenReturn(mockVeniceServerConfig);

    VeniceView veniceView = ViewUtils.getVeniceView(
        ChangeCaptureView.class.getCanonicalName(),
        mockVeniceConfigLoader.getCombinedProperties().toProperties(),
        mockStore,
        Collections.EMPTY_MAP);
    VeniceViewWriter viewWriter = ViewWriterUtils.getVeniceViewWriter(
        ChangeCaptureView.class.getCanonicalName(),
        mockVeniceConfigLoader,
        mockStore,
        SCHEMA,
        Collections.EMPTY_MAP);

    Assert.assertTrue(viewWriter instanceof ChangeCaptureViewWriter);
    Assert.assertTrue(veniceView instanceof ChangeCaptureView);
  }

}
