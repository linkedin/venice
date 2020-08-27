package com.linkedin.venice.ingestion;

import com.linkedin.venice.ingestion.handler.IngestionServiceTaskHandler;
import com.linkedin.venice.ingestion.protocol.InitializationConfigs;
import com.linkedin.venice.meta.IngestionAction;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.HashMap;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class IngestionServiceTaskHandlerTest {

  @Test
  public void testInitializationConfig() throws Exception {
    String path = "/" + IngestionAction.INIT;
    InitializationConfigs initializationConfigs = new InitializationConfigs();
    initializationConfigs.aggregatedConfigs = new HashMap<>();
    byte[] content = IngestionServiceTaskHandler.initializationConfigSerializer.serialize(null, initializationConfigs);
    ChannelHandlerContext mockContext = Mockito.mock(ChannelHandlerContext.class);
    FullHttpRequest msg = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, path, Unpooled.wrappedBuffer(content));
    ArgumentCaptor<FullHttpResponse> argumentCaptor = ArgumentCaptor.forClass(FullHttpResponse.class);

    IngestionServiceTaskHandler ingestionServiceTaskHandler = new IngestionServiceTaskHandler(null);
    ingestionServiceTaskHandler.channelRead(mockContext, msg);
    verify(mockContext).writeAndFlush(argumentCaptor.capture());
    FullHttpResponse response = argumentCaptor.getValue();
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
  }
}
