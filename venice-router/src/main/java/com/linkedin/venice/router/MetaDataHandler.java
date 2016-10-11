package com.linkedin.venice.router;

import com.linkedin.venice.controllerapi.MasterControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.api.VenicePathParserHelper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.ExceptionUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

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
public class MetaDataHandler extends SimpleChannelUpstreamHandler {
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
                                     ChannelHandlerContext ctx, Channel ch) {
    HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
    response.setContent(ChannelBuffers.wrappedBuffer(body));
    if (!isJson) {
      response.headers().set(CONTENT_TYPE, "text/plain");
    } else {
      response.headers().set(CONTENT_TYPE, "application/json");
    }
    response.headers().set(CONTENT_LENGTH, response.getContent().readableBytes());
    ChannelFuture f = Channels.future(ch);
    /**
     * There is a {@link com.linkedin.ddsstorage.netty3.handlers.StaleConnectionHandler}, which
     * handles closing idle connections, so we don't need to explicitly close the connection here.
     */
    Channels.write(ctx, f, response);
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) throws IOException {
    Object msg = event.getMessage();
    Channel ch = event.getChannel();
    if (msg instanceof HttpRequest) {
      HttpRequest req = (HttpRequest) msg;
      VenicePathParserHelper helper = new VenicePathParserHelper(req.getUri());
      String resourceType = helper.getResourceType(); //may be null
      if (VenicePathParser.TYPE_MASTER_CONTROLLER.equals(resourceType)){
        // URI: /master_controller
        handleControllerLookup(ctx, ch);
      } else if (VenicePathParser.TYPE_KEY_SCHEMA.equals(resourceType)) {
        // URI: /key_schema/${storeName}
        // For key schema lookup, we only consider storeName
        handleKeySchemaLookup(ctx, ch, helper);
      } else if (VenicePathParser.TYPE_VALUE_SCHEMA.equals(resourceType)) {
        // The request could fetch one value schema by id or all the value schema for the given store
        // URI: /value_schema/{$storeName} - Get all the value schema
        // URI: /value_schema/{$storeName}/{$valueSchemaId} - Get single value schema
        handleValueSchemaLookup(ctx, ch, helper);
      } else {
        ctx.sendUpstream(event);
        return;
      }
    }
  }

  private void handleControllerLookup(ChannelHandlerContext ctx, Channel ch) throws IOException {
    MasterControllerResponse responseObject = new MasterControllerResponse();
    responseObject.setCluster(clusterName);
    responseObject.setUrl(routing.getMasterController().getUrl());
    setupResponseAndFlush(OK, mapper.writeValueAsBytes(responseObject), true, ctx, ch);
  }

  private void handleKeySchemaLookup(ChannelHandlerContext ctx, Channel ch, VenicePathParserHelper helper) throws IOException {
    String storeName = helper.getResourceName();
    if (null == storeName || storeName.isEmpty()) {
      throw new VeniceException("storeName required, valid path should be : /"
          + VenicePathParser.TYPE_KEY_SCHEMA + "/${storeName}");
    }
    SchemaEntry keySchema = schemaRepo.getKeySchema(storeName);
    if (null == keySchema) {
      byte[] errBody = new String("Key schema for store: " + storeName + " doesn't exist").getBytes();
      setupResponseAndFlush(NOT_FOUND, errBody, false, ctx, ch);
      return;
    }
    SchemaResponse responseObject = new SchemaResponse();
    responseObject.setCluster(clusterName);
    responseObject.setName(storeName);
    responseObject.setId(keySchema.getId());
    responseObject.setSchemaStr(keySchema.getSchema().toString());
    setupResponseAndFlush(OK, mapper.writeValueAsBytes(responseObject), true, ctx, ch);
  }

  private void handleValueSchemaLookup(ChannelHandlerContext ctx, Channel ch, VenicePathParserHelper helper) throws IOException {
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
      setupResponseAndFlush(OK, mapper.writeValueAsBytes(responseObject), true, ctx, ch);
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
        setupResponseAndFlush(NOT_FOUND, errBody, false, ctx, ch);
        return;
      }
      responseObject.setId(valueSchema.getId());
      responseObject.setSchemaStr(valueSchema.getSchema().toString());
      setupResponseAndFlush(OK, mapper.writeValueAsBytes(responseObject), true, ctx, ch);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
    logger.error("Got exception while handling meta data request", e.getCause());
    try {
      if (ExceptionUtils.recursiveClassEquals(e.getCause(), IOException.class)) {
        // No need to send back error response since the connection has some issue.
        return;
      }
      String stackTraceStr = ExceptionUtils.stackTraceToString(e.getCause());
      setupResponseAndFlush(INTERNAL_SERVER_ERROR, stackTraceStr.getBytes(),
          false, ctx, e.getChannel());
    } catch (Exception ex) {
      logger.error("Got exception while trying to send error response", ex);
    } finally {
      ctx.getChannel().close();
    }
  }
}
