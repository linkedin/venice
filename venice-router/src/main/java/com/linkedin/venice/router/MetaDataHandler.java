package com.linkedin.venice.router;

import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.MasterControllerResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.api.VenicePathParserHelper;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import static com.linkedin.venice.utils.NettyUtils.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;


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
  private final ReadOnlyStoreConfigRepository storeConfigRepo;
  private final String clusterName;
  private final Map<String, String> clusterToD2Map;

  public MetaDataHandler(RoutingDataRepository routing, ReadOnlySchemaRepository schemaRepo, String clusterName, ReadOnlyStoreConfigRepository storeConfigRepo, Map<String, String> clusterToD2Map){
    super();
    this.routing = routing;
    this.schemaRepo = schemaRepo;
    this.clusterName = clusterName;
    this.storeConfigRepo = storeConfigRepo;
    this.clusterToD2Map = clusterToD2Map;
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
    } else if (VenicePathParser.TYPE_CLUSTER_DISCOVERY.equals(resourceType)) {
      // URI: /discover_cluster/${storeName}
      hanldeD2ServiceLookup(ctx, helper);
    } else {
      // SimpleChannelInboundHandler automatically releases the request after channelRead0 is done.
      // since we're passing it on to the next handler, we need to retain an extra reference.
      ReferenceCountUtil.retain(req);
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
    checkStoreName(storeName, "/" + VenicePathParser.TYPE_KEY_SCHEMA + "/${storeName}");
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
    checkStoreName(storeName,
        "/" + VenicePathParser.TYPE_VALUE_SCHEMA + "/${storeName} or /" + VenicePathParser.TYPE_VALUE_SCHEMA
            + "/${storeName}/${valueSchemaId}");
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

  private void hanldeD2ServiceLookup(ChannelHandlerContext ctx, VenicePathParserHelper helper)
      throws IOException {
    String storeName = helper.getResourceName();
    checkStoreName(storeName, "/" + VenicePathParser.TYPE_CLUSTER_DISCOVERY + "/${storeName}");
    Optional<StoreConfig> config = storeConfigRepo.getStoreConfig(storeName);
    if (!config.isPresent() || Utils.isNullOrEmpty(config.get().getCluster())) {
      byte[] errBody = new String("Cluster for store: " + storeName + " doesn't exist").getBytes();
      setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
      return;
    }
    String clusterName = config.get().getCluster();
    String d2Service = getD2ServiceByClusterName(clusterName);
    if (Utils.isNullOrEmpty(d2Service)) {
      byte[] errBody = new String("D2 service for store: " + storeName + " doesn't exist").getBytes();
      setupResponseAndFlush(NOT_FOUND, errBody, false, ctx);
      return;
    }
    D2ServiceDiscoveryResponse responseObject = new D2ServiceDiscoveryResponse();
    responseObject.setCluster(config.get().getCluster());
    responseObject.setName(config.get().getStoreName());
    responseObject.setD2Service(d2Service);
    setupResponseAndFlush(OK, mapper.writeValueAsBytes(responseObject), true, ctx);
  }

  private String getD2ServiceByClusterName(String clusterName) {
    return clusterToD2Map.get(clusterName);
  }

  private void checkStoreName(String storeName, String path) {
    if (Utils.isNullOrEmpty(storeName)) {
      throw new VeniceException("storeName required, valid path should be : " + path);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws Exception {
    logger.error("Got exception while handling meta data request: " + e.getMessage(), e);
    try {
      if (null != e.getCause() && ExceptionUtils.recursiveClassEquals(e.getCause(), IOException.class)) {
        logger.warn("Caught exception is IOException, not sending response");
        // No need to send back error response since the connection has some issue.
        return;
      }
      String stackTraceStr = ExceptionUtils.stackTraceToString(e);
      setupResponseAndFlush(INTERNAL_SERVER_ERROR, stackTraceStr.getBytes(),
          false, ctx);
    } catch (Exception ex) {
      logger.error("Got exception while trying to send error response", ex);
    } finally {
      ctx.channel().close();
    }
  }

}
