package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.ingestion.isolated.IsolatedIngestionServerHandler;
import com.linkedin.davinci.ingestion.isolated.IsolatedIngestionServer;
import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
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


public class IsolatedIngestionServerTaskHandlerTest {
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
      byte[] content = IsolatedIngestionUtils.serializeIngestionActionRequest(IngestionAction.INIT, initializationConfigs);
      ChannelHandlerContext mockContext = mock(ChannelHandlerContext.class);
      FullHttpRequest msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, path, Unpooled.wrappedBuffer(content));
      ArgumentCaptor<FullHttpResponse> argumentCaptor = ArgumentCaptor.forClass(FullHttpResponse.class);

      IsolatedIngestionServer isolatedIngestionServer = mock(IsolatedIngestionServer.class);
      IsolatedIngestionServerHandler isolatedIngestionServerHandler = new IsolatedIngestionServerHandler(isolatedIngestionServer);
      isolatedIngestionServerHandler.channelRead(mockContext, msg);
      verify(mockContext).writeAndFlush(argumentCaptor.capture());
      FullHttpResponse response = argumentCaptor.getValue();
      Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    }
  }
}
