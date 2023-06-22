package com.linkedin.venice.writer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapterFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;


public class VeniceWriterFactoryTest {
  @Test
  public void testVeniceWriterFactory() throws Exception {
    PubSubProducerAdapterFactory<PubSubProducerAdapter> producerFactoryMack = mock(PubSubProducerAdapterFactory.class);
    PubSubProducerAdapter producerAdapterMock = mock(PubSubProducerAdapter.class);
    ArgumentCaptor<String> brokerAddrCapture = ArgumentCaptor.forClass(String.class);
    when(producerFactoryMack.create(any(VeniceProperties.class), eq("store_v1"), brokerAddrCapture.capture()))
        .thenReturn(producerAdapterMock);
    when(producerAdapterMock.getNumberOfPartitions(any(), anyInt(), any())).thenReturn(1);

    VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(new Properties(), producerFactoryMack, null);
    VeniceWriter veniceWriter = veniceWriterFactory
        .createVeniceWriter(new VeniceWriterOptions.Builder("store_v1").setBrokerAddress("kafka:9898").build());
    when(producerAdapterMock.getBrokerAddress()).thenReturn(brokerAddrCapture.getValue());
    assertNotNull(veniceWriter);

    String capturedBrokerAddr = veniceWriter.getDestination();
    assertNotNull(capturedBrokerAddr);
    assertEquals(capturedBrokerAddr, "store_v1@kafka:9898");
  }
}
