package com.linkedin.venice.router;

import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.schema.SchemaEntry;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.DownstreamMessageEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class TestMetaDataHandler {
  private static ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testControllerLookup() throws IOException {
    // Mock RoutingDataRepository
    RoutingDataRepository routingRepo = Mockito.mock(RoutingDataRepository.class);
    String masterControllerHost = "myControllerHost";
    int masterControllerPort = 1234;
    Instance masterControllerInstance = new Instance("1", masterControllerHost, masterControllerPort);
    Mockito.doReturn(masterControllerInstance).when(routingRepo).getMasterController();

    // Mock MessageEvent
    MessageEvent event = Mockito.mock(MessageEvent.class);
    HttpRequest httpRequest = Mockito.mock(HttpRequest.class);
    Mockito.doReturn("http://myRouterHost:4567/controller").when(httpRequest).getUri();
    Mockito.doReturn(httpRequest).when(event).getMessage();

    // Mock ChannelHandlerContext
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.doReturn(ch).when(ctx).getChannel();

    MetaDataHandler handler = new MetaDataHandler(routingRepo, null, "test-cluster");
    handler.messageReceived(ctx, event);
    ArgumentCaptor<DownstreamMessageEvent> captor = ArgumentCaptor.forClass(DownstreamMessageEvent.class);
    Mockito.verify(ctx).sendDownstream(captor.capture());
    DownstreamMessageEvent messageEvent = captor.getValue();
    DefaultHttpResponse response = (DefaultHttpResponse)messageEvent.getMessage();
    Assert.assertEquals(response.getStatus().getCode(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "text/plain");
    String actualBody = new String(response.getContent().array());
    Assert.assertEquals(actualBody, "http://" + masterControllerHost + ":" + masterControllerPort);
  }

  @Test
  public void testKeySchemaLookup() throws IOException {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    String clusterName = "test-cluster";
    int keySchemaId = 1;
    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    SchemaEntry keySchemaEntry = new SchemaEntry(keySchemaId, keySchemaStr);
    Mockito.doReturn(keySchemaEntry).when(schemaRepo).getKeySchema(storeName);

    // Mock MessageEvent
    MessageEvent event = Mockito.mock(MessageEvent.class);
    HttpRequest httpRequest = Mockito.mock(HttpRequest.class);
    Mockito.doReturn("http://myRouterHost:4567/key_schema/" + storeName).when(httpRequest).getUri();
    Mockito.doReturn(httpRequest).when(event).getMessage();

    // Mock ChannelHandlerContext
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.doReturn(ch).when(ctx).getChannel();

    MetaDataHandler handler = new MetaDataHandler(null, schemaRepo, clusterName);
    handler.messageReceived(ctx, event);
    ArgumentCaptor<DownstreamMessageEvent> captor = ArgumentCaptor.forClass(DownstreamMessageEvent.class);
    Mockito.verify(ctx).sendDownstream(captor.capture());
    DownstreamMessageEvent messageEvent = captor.getValue();
    DefaultHttpResponse response = (DefaultHttpResponse)messageEvent.getMessage();
    Assert.assertEquals(response.getStatus().getCode(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    SchemaResponse schemaResponse = mapper.readValue(response.getContent().array(), SchemaResponse.class);
    Assert.assertEquals(schemaResponse.getId(), keySchemaId);
    Assert.assertEquals(schemaResponse.getSchemaStr(), keySchemaStr);
    Assert.assertEquals(schemaResponse.getName(), storeName);
    Assert.assertEquals(schemaResponse.getCluster(), clusterName);
    Assert.assertFalse(schemaResponse.isError());
  }

  @Test
  public void testKeySchemaLookupWithKeySchemaDoesntExist() throws IOException {
    String storeName = "test_store";
    String clusterName = "test-cluster";
    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    Mockito.doReturn(null).when(schemaRepo).getKeySchema(storeName);

    // Mock MessageEvent
    MessageEvent event = Mockito.mock(MessageEvent.class);
    HttpRequest httpRequest = Mockito.mock(HttpRequest.class);
    Mockito.doReturn("http://myRouterHost:4567/key_schema/" + storeName).when(httpRequest).getUri();
    Mockito.doReturn(httpRequest).when(event).getMessage();

    // Mock ChannelHandlerContext
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.doReturn(ch).when(ctx).getChannel();

    MetaDataHandler handler = new MetaDataHandler(null, schemaRepo, clusterName);
    handler.messageReceived(ctx, event);
    ArgumentCaptor<DownstreamMessageEvent> captor = ArgumentCaptor.forClass(DownstreamMessageEvent.class);
    Mockito.verify(ctx).sendDownstream(captor.capture());
    DownstreamMessageEvent messageEvent = captor.getValue();
    DefaultHttpResponse response = (DefaultHttpResponse)messageEvent.getMessage();
    Assert.assertEquals(response.getStatus().getCode(), 404);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "text/plain");
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*storeName required.*")
  public void testInvalidKeySchemaPath() throws IOException {
    String storeName = "test_store";
    String clusterName = "test-cluster";
    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    Mockito.doReturn(null).when(schemaRepo).getKeySchema(storeName);

    // Mock MessageEvent
    MessageEvent event = Mockito.mock(MessageEvent.class);
    HttpRequest httpRequest = Mockito.mock(HttpRequest.class);
    Mockito.doReturn("http://myRouterHost:4567/key_schema/").when(httpRequest).getUri();
    Mockito.doReturn(httpRequest).when(event).getMessage();

    // Mock ChannelHandlerContext
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.doReturn(ch).when(ctx).getChannel();

    MetaDataHandler handler = new MetaDataHandler(null, schemaRepo, clusterName);
    handler.messageReceived(ctx, event);
  }

  @Test
  public void testSingleValueSchemaLookup() throws IOException {
    String storeName = "test_store";
    String valueSchemaStr = "\"string\"";
    String clusterName = "test-cluster";
    int valueSchemaId = 1;
    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    SchemaEntry valueSchemaEntry = new SchemaEntry(valueSchemaId, valueSchemaStr);
    Mockito.doReturn(valueSchemaEntry).when(schemaRepo).getValueSchema(storeName, valueSchemaId);

    // Mock MessageEvent
    MessageEvent event = Mockito.mock(MessageEvent.class);
    HttpRequest httpRequest = Mockito.mock(HttpRequest.class);
    Mockito.doReturn("http://myRouterHost:4567/value_schema/" + storeName + "/" + valueSchemaId).when(httpRequest).getUri();
    Mockito.doReturn(httpRequest).when(event).getMessage();

    // Mock ChannelHandlerContext
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.doReturn(ch).when(ctx).getChannel();

    MetaDataHandler handler = new MetaDataHandler(null, schemaRepo, clusterName);
    handler.messageReceived(ctx, event);
    ArgumentCaptor<DownstreamMessageEvent> captor = ArgumentCaptor.forClass(DownstreamMessageEvent.class);
    Mockito.verify(ctx).sendDownstream(captor.capture());
    DownstreamMessageEvent messageEvent = captor.getValue();
    DefaultHttpResponse response = (DefaultHttpResponse)messageEvent.getMessage();
    Assert.assertEquals(response.getStatus().getCode(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    SchemaResponse schemaResponse = mapper.readValue(response.getContent().array(), SchemaResponse.class);
    Assert.assertEquals(schemaResponse.getId(), 1);
    Assert.assertEquals(schemaResponse.getSchemaStr(), valueSchemaStr);
    Assert.assertEquals(schemaResponse.getName(), storeName);
    Assert.assertEquals(schemaResponse.getCluster(), clusterName);
    Assert.assertFalse(schemaResponse.isError());
  }

  @Test
  public void testSingleValueSchemaLookupWithValueSchemaDoesntExist() throws IOException {
    String storeName = "test_store";
    String clusterName = "test-cluster";
    int valueSchemaId = 1;
    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    Mockito.doReturn(null).when(schemaRepo).getValueSchema(storeName, valueSchemaId);

    // Mock MessageEvent
    MessageEvent event = Mockito.mock(MessageEvent.class);
    HttpRequest httpRequest = Mockito.mock(HttpRequest.class);
    Mockito.doReturn("http://myRouterHost:4567/value_schema/" + storeName + "/" + valueSchemaId).when(httpRequest).getUri();
    Mockito.doReturn(httpRequest).when(event).getMessage();

    // Mock ChannelHandlerContext
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.doReturn(ch).when(ctx).getChannel();

    MetaDataHandler handler = new MetaDataHandler(null, schemaRepo, clusterName);
    handler.messageReceived(ctx, event);
    ArgumentCaptor<DownstreamMessageEvent> captor = ArgumentCaptor.forClass(DownstreamMessageEvent.class);
    Mockito.verify(ctx).sendDownstream(captor.capture());
    DownstreamMessageEvent messageEvent = captor.getValue();
    DefaultHttpResponse response = (DefaultHttpResponse)messageEvent.getMessage();
    Assert.assertEquals(response.getStatus().getCode(), 404);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "text/plain");
  }

  @Test
  public void testAllValueSchemaLookup() throws IOException {
    String storeName = "test_store";
    String valueSchemaStr1 = "\"string\"";
    String valueSchemaStr2 = "\"long\"";
    String clusterName = "test-cluster";
    int valueSchemaId1 = 1;
    int valueSchemaId2 = 2;
    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    SchemaEntry valueSchemaEntry1 = new SchemaEntry(valueSchemaId1, valueSchemaStr1);
    SchemaEntry valueSchemaEntry2 = new SchemaEntry(valueSchemaId2, valueSchemaStr2);
    Mockito.doReturn(Arrays.asList(valueSchemaEntry1, valueSchemaEntry2)).when(schemaRepo).getValueSchemas(storeName);

    // Mock MessageEvent
    MessageEvent event = Mockito.mock(MessageEvent.class);
    HttpRequest httpRequest = Mockito.mock(HttpRequest.class);
    Mockito.doReturn("http://myRouterHost:4567/value_schema/" + storeName).when(httpRequest).getUri();
    Mockito.doReturn(httpRequest).when(event).getMessage();

    // Mock ChannelHandlerContext
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.doReturn(ch).when(ctx).getChannel();

    MetaDataHandler handler = new MetaDataHandler(null, schemaRepo, clusterName);
    handler.messageReceived(ctx, event);
    ArgumentCaptor<DownstreamMessageEvent> captor = ArgumentCaptor.forClass(DownstreamMessageEvent.class);
    Mockito.verify(ctx).sendDownstream(captor.capture());
    DownstreamMessageEvent messageEvent = captor.getValue();
    DefaultHttpResponse response = (DefaultHttpResponse)messageEvent.getMessage();
    Assert.assertEquals(response.getStatus().getCode(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    MultiSchemaResponse multiSchemaResponse = mapper.readValue(response.getContent().array(), MultiSchemaResponse.class);

    Assert.assertEquals(multiSchemaResponse.getName(), storeName);
    Assert.assertEquals(multiSchemaResponse.getCluster(), clusterName);
    Assert.assertFalse(multiSchemaResponse.isError());
    MultiSchemaResponse.Schema[] schemas = multiSchemaResponse.getSchemas();
    Assert.assertEquals(schemas.length, 2);
    Assert.assertEquals(schemas[0].getId(), valueSchemaId1);
    Assert.assertEquals(schemas[0].getSchemaStr(), valueSchemaStr1);
    Assert.assertEquals(schemas[1].getId(), valueSchemaId2);
    Assert.assertEquals(schemas[1].getSchemaStr(), valueSchemaStr2);
  }

  @Test
  public void testAllValueSchemaLookupWithNoValueSchema() throws IOException {
    String storeName = "test_store";
    String clusterName = "test-cluster";
    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    Mockito.doReturn(Collections.EMPTY_LIST).when(schemaRepo).getValueSchemas(storeName);

    // Mock MessageEvent
    MessageEvent event = Mockito.mock(MessageEvent.class);
    HttpRequest httpRequest = Mockito.mock(HttpRequest.class);
    Mockito.doReturn("http://myRouterHost:4567/value_schema/" + storeName).when(httpRequest).getUri();
    Mockito.doReturn(httpRequest).when(event).getMessage();

    // Mock ChannelHandlerContext
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.doReturn(ch).when(ctx).getChannel();

    MetaDataHandler handler = new MetaDataHandler(null, schemaRepo, clusterName);
    handler.messageReceived(ctx, event);
    ArgumentCaptor<DownstreamMessageEvent> captor = ArgumentCaptor.forClass(DownstreamMessageEvent.class);
    Mockito.verify(ctx).sendDownstream(captor.capture());
    DownstreamMessageEvent messageEvent = captor.getValue();
    DefaultHttpResponse response = (DefaultHttpResponse)messageEvent.getMessage();
    Assert.assertEquals(response.getStatus().getCode(), 200);
    Assert.assertEquals(response.headers().get(CONTENT_TYPE), "application/json");
    MultiSchemaResponse multiSchemaResponse = mapper.readValue(response.getContent().array(), MultiSchemaResponse.class);

    Assert.assertEquals(multiSchemaResponse.getName(), storeName);
    Assert.assertEquals(multiSchemaResponse.getCluster(), clusterName);
    Assert.assertFalse(multiSchemaResponse.isError());
    MultiSchemaResponse.Schema[] schemas = multiSchemaResponse.getSchemas();
    Assert.assertEquals(schemas.length, 0);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*storeName required.*")
  public void testInvalidValueSchemaPath() throws IOException {
    String storeName = "test_store";
    String clusterName = "test-cluster";
    // Mock ReadOnlySchemaRepository
    ReadOnlySchemaRepository schemaRepo = Mockito.mock(ReadOnlySchemaRepository.class);
    Mockito.doReturn(null).when(schemaRepo).getKeySchema(storeName);

    // Mock MessageEvent
    MessageEvent event = Mockito.mock(MessageEvent.class);
    HttpRequest httpRequest = Mockito.mock(HttpRequest.class);
    Mockito.doReturn("http://myRouterHost:4567/value_schema/").when(httpRequest).getUri();
    Mockito.doReturn(httpRequest).when(event).getMessage();

    // Mock ChannelHandlerContext
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.doReturn(ch).when(ctx).getChannel();

    MetaDataHandler handler = new MetaDataHandler(null, schemaRepo, clusterName);
    handler.messageReceived(ctx, event);
  }

  @Test
  public void testStorageRequest() throws IOException {
    String storeName = "test_store";
    String clusterName = "test-cluster";

    // Mock MessageEvent
    MessageEvent event = Mockito.mock(MessageEvent.class);
    HttpRequest httpRequest = Mockito.mock(HttpRequest.class);
    Mockito.doReturn("http://myRouterHost:4567/storage/" + storeName + "/abc").when(httpRequest).getUri();
    Mockito.doReturn(httpRequest).when(event).getMessage();

    // Mock ChannelHandlerContext
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel ch = Mockito.mock(Channel.class);
    Mockito.doReturn(ch).when(ctx).getChannel();

    MetaDataHandler handler = new MetaDataHandler(null, null, clusterName);
    handler.messageReceived(ctx, event);
    // '/storage' request should be handled by upstream, instead of current MetaDataHandler
    Mockito.verify(ctx, Mockito.times(1)).sendUpstream(Mockito.any());
  }
}
