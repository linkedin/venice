package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.ingestion.handler.IngestionServiceTaskHandler;
import com.linkedin.venice.ingestion.protocol.InitializationConfigs;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import com.linkedin.venice.utils.TestUtils;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;

import static com.linkedin.venice.ConfigKeys.*;
import static org.mockito.Mockito.*;


public class IngestionServiceTaskHandlerTest {
  @Test
  public void testInitializationConfig() throws Exception {
    try (ZkServerWrapper zk = ServiceFactory.getZkServer()) {
      String path = "/" + IngestionAction.INIT;
      InitializationConfigs initializationConfigs = new InitializationConfigs();
      initializationConfigs.aggregatedConfigs = new HashMap<>();
      initializationConfigs.aggregatedConfigs.put(CLUSTER_NAME, TestUtils.getUniqueString());
      initializationConfigs.aggregatedConfigs.put(ZOOKEEPER_ADDRESS, zk.getAddress());
      initializationConfigs.aggregatedConfigs.put(KAFKA_BOOTSTRAP_SERVERS, TestUtils.getUniqueString());
      initializationConfigs.aggregatedConfigs.put(KAFKA_ZK_ADDRESS, TestUtils.getUniqueString());
      initializationConfigs.aggregatedConfigs.put(D2_CLIENT_ZK_HOSTS_ADDRESS, zk.getAddress());
      byte[] content = IngestionUtils.initializationConfigSerializer.serialize(null, initializationConfigs);
      ChannelHandlerContext mockContext = mock(ChannelHandlerContext.class);
      FullHttpRequest msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, path, Unpooled.wrappedBuffer(content));
      ArgumentCaptor<FullHttpResponse> argumentCaptor = ArgumentCaptor.forClass(FullHttpResponse.class);

      IngestionService ingestionService = mock(IngestionService.class);
      IngestionServiceTaskHandler ingestionServiceTaskHandler = new IngestionServiceTaskHandler(ingestionService);
      ingestionServiceTaskHandler.channelRead(mockContext, msg);
      verify(mockContext).writeAndFlush(argumentCaptor.capture());
      FullHttpResponse response = argumentCaptor.getValue();
      Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    }
  }
}
