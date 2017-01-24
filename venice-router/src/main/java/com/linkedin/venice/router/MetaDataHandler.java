package com.linkedin.venice.router;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controllerapi.MasterControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.api.VenicePathParserHelper;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.ExceptionUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;


/**
 * This MetaDataHandle is used to handle the following meta data requests:
 * 1. Controller lookup: /controller, and it will return master controller url as the content.
 * 2. Key schema lookup: /key_schema/${storeName}, and it will return key schema in json format.
 *    The client can use {@link com.linkedin.venice.controllerapi.SchemaResponse} to parse it.
 * 3. Single value schema lookup: /value_schema/${storeName}/${valueSchemaId}, and it will return
 *    the corresponding value schema in json format.
 *    The client can use {@link com.linkedin.venice.controllerapi.SchemaResponse} to parse it.
 * 4. All value schema lookup: /value_schema/{$storeName}, and it will return all the value schema
 *    of the specified store in json format. The client can use
 *    {@link com.linkedin.venice.controllerapi.MultiSchemaResponse} to parse it.
 */
@ChannelHandler.Sharable
public class MetaDataHandler extends SimpleChannelInboundHandler<HttpRequest> {
  private static final Logger logger = Logger.getLogger(MetaDataHandler.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private final RoutingDataRepository routing;
  private final ReadOnlySchemaRepository schemaRepo;
  private final String clusterName;

  public MetaDataHandler(RoutingDataRepository routing, ReadOnlySchemaRepository schemaRepo, String clusterName){
    super();
    this.routing = routing;
    this.schemaRepo = schemaRepo;
    this.clusterName = clusterName;
  }

  private void setupResponseAndFlush(HttpResponseStatus status, byte[] body, boolean isJson,
                                     ChannelHandlerContext ctx) {
    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.wrappedBuffer(body));
    try {
      if (!isJson) {
        response.headers().set(CONTENT_TYPE, HttpConstants.TEXT_PLAIN);
      } else {
        response.headers().set(CONTENT_TYPE, HttpConstants.JSON);
      }
    } catch (NoSuchMethodError e){ // netty version conflict
      ClassLoader cl = ClassLoader.getSystemClassLoader();
      URL[] urls = ((URLClassLoader)cl).getURLs();
      logger.warn("NoSuchMethodError, probably from netty version conflict.  Printing netty on classpath: ", e);
      Arrays.asList(urls).stream().filter(url -> url.getFile().contains("netty")).forEach(logger::warn);
      throw e;
    }
    response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
    ctx.writeAndFlush(response);
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpRequest req) throws IOException {
    VenicePathParserHelper helper = new VenicePathParserHelper(req.uri());
    String resourceType = helper.getResourceType(); //may be null
    if (VenicePathParser.TYPE_MASTER_CONTROLLER.equals(resourceType)){
      // URI: /master_controller
      handleControllerLookup(ctx);
    } else if (VenicePathParser.TYPE_KEY_SCHEMA.equals(resourceType)) {
      // URI: /key_schema/${storeName}
      // For key schema lookup, we only consider storeName
      handleKeySchemaLookup(ctx, helper);
    } else if (VenicePathParser.TYPE_VALUE_SCHEMA.equals(resourceType)) {
      // The request could fetch one value schema by id or all the value schema for the given store
      // URI: /value_schema/{$storeName} - Get all the value schema
      // URI: /value_schema/{$storeName}/{$valueSchemaId} - Get single value schema
      handleValueSchemaLookup(ctx, helper);
    } else {
      ctx.fireChannelRead(req);
      return;
    }
  }

  private void handleControllerLookup(ChannelHandlerContext ctx) throws IOException {
    MasterControllerResponse responseObject = new MasterControllerResponse();
    responseObject.setCluster(clusterName);
    responseObject.setUrl(routing.getMasterController().getUrl());
    setupResponseAndFlush(OK, mapper.writeValueAsBytes(responseObject), true, ctx);
  }

  private void handleKeySchemaLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper) throws IOException {
    String storeName = helper.getResourceName();
    if (null == storeName || storeName.isEmpty()) {
      throw new VeniceException("storeName required, valid path should be : /"
          + VenicePathParser.TYPE_KEY_SCHEMA + "/${storeName}");
    }
    SchemaEntry keySchema = schemaRepo.getKeySchema(storeName);
    if (null == keySchema) {
      byte[] errBody = new String("Key schema for store: " + storeName + " doesn't exist").getBytes();
      setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
      return;
    }
    SchemaResponse responseObject = new SchemaResponse();
    responseObject.setCluster(clusterName);
    responseObject.setName(storeName);
    responseObject.setId(keySchema.getId());
    responseObject.setSchemaStr(keySchema.getSchema().toString());
    setupResponseAndFlush(OK, mapper.writeValueAsBytes(responseObject), true, ctx);
  }

  private void handleValueSchemaLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper) throws IOException {
    String storeName = helper.getResourceName();
    if (null == storeName || storeName.isEmpty()) {
      throw new VeniceException("storeName required, valid path should be : /" + VenicePathParser.TYPE_VALUE_SCHEMA
          + "/${storeName} or /" + VenicePathParser.TYPE_VALUE_SCHEMA + "/${storeName}/${valueSchemaId}");
    }
    String id = helper.getKey();
    if (null == id || id.isEmpty()) {
      // URI: /value_schema/{$storeName}
      // Get all the value schema
      MultiSchemaResponse responseObject = new MultiSchemaResponse();
      responseObject.setCluster(clusterName);
      responseObject.setName(storeName);
      Collection<SchemaEntry> valueSchemaEntries = schemaRepo.getValueSchemas(storeName);
      int schemaNum = valueSchemaEntries.size();
      MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[schemaNum];
      int cur = 0;
      for (SchemaEntry entry : valueSchemaEntries) {
        schemas[cur] = new MultiSchemaResponse.Schema();
        schemas[cur].setId(entry.getId());
        schemas[cur].setSchemaStr(entry.getSchema().toString());
        ++cur;
      }
      responseObject.setSchemas(schemas);
      setupResponseAndFlush(OK, mapper.writeValueAsBytes(responseObject), true, ctx);
    } else {
      // URI: /value_schema/{$storeName}/{$valueSchemaId}
      // Get single value schema
      SchemaResponse responseObject = new SchemaResponse();
      responseObject.setCluster(clusterName);
      responseObject.setName(storeName);
      SchemaEntry valueSchema = schemaRepo.getValueSchema(storeName, Integer.parseInt(id));
      if (null == valueSchema) {
        byte[] errBody = new String("Value schema doesn't exist for schema id: " + id
            + " of store: " + storeName).getBytes();
        setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
        return;
      }
      responseObject.setId(valueSchema.getId());
      responseObject.setSchemaStr(valueSchema.getSchema().toString());
      setupResponseAndFlush(OK, mapper.writeValueAsBytes(responseObject), true, ctx);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws Exception {
    logger.error("Got exception while handling meta data request", e.getCause());
    try {
      if (ExceptionUtils.recursiveClassEquals(e.getCause(), IOException.class)) {
        logger.warn("Caught exception is IOException, not sending response");
        // No need to send back error response since the connection has some issue.
        return;
      }
      String stackTraceStr = ExceptionUtils.stackTraceToString(e.getCause());
      setupResponseAndFlush(INTERNAL_SERVER_ERROR, stackTraceStr.getBytes(),
          false, ctx);
    } catch (Exception ex) {
      logger.error("Got exception while trying to send error response", ex);
    } finally {
      ctx.channel().close();
    }
  }
}
