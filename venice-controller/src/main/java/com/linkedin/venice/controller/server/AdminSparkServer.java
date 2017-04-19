package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.AuditInfo;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.stats.SparkServerStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.List;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import spark.Request;
import spark.Response;
import spark.Service;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;


/**
 * Controller admin API leveraging sparkjava: http://sparkjava.com/documentation.html
 */
public class AdminSparkServer extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(AdminSparkServer.class);

  private final int port;
  private final Admin admin;
  protected static final ObjectMapper mapper = new ObjectMapper();
  final private SparkServerStats stats;

  private static String REQUEST_START_TIME =  "startTime";
  private static String REQUEST_SUCCEED = "succeed";

  // In order to build multiple controller in a single JVM, we create a new http service instance for each of AdminSparkServer instance.
  private final Service httpService;


  public AdminSparkServer(int port, Admin admin, MetricsRepository metricsRepository) {
    this.port = port;
    //Note: admin is passed in as a reference.  The expectation is the source of the admin will
    //      close it so we don't close it in stopInner()
    this.admin = admin;
    stats = new SparkServerStats(metricsRepository, "controller_spark_server");
    httpService = Service.ignite();
  }

  @Override
  public boolean startInner() throws Exception {
    httpService.port(port);

    httpService.before((request, response) -> {
      AuditInfo audit = new AuditInfo(request);
      logger.info(audit.toString());

      stats.recordRequest();
      request.attribute(REQUEST_START_TIME, System.currentTimeMillis());
      request.attribute(REQUEST_SUCCEED, true);
    });

    httpService.after((request, response) -> {
      AuditInfo audit = new AuditInfo(request);
      long latency = System.currentTimeMillis() - (long)request.attribute(REQUEST_START_TIME);
      if ((boolean)request.attribute(REQUEST_SUCCEED)) {
        logger.info(audit.successString());
        stats.recordSuccessfulRequest();
        stats.recordSuccessfulRequestLatency(latency);
      } else {
        logger.info(audit.failureString(response.body()));
        stats.recordFailedRequest();
        stats.recordFailedRequestLatency(latency);
      }
    });

    httpService.get(CREATE_VERSION.getPath(), (request, response) -> {
      response.type(HttpConstants.TEXT_HTML);
      return writeMenu("Create New Store", CREATE_VERSION.getPath(), CREATE_VERSION.getParams ());
    });

    httpService.get(SET_VERSION.getPath(), (request, response) -> {
      response.type(HttpConstants.TEXT_HTML);
      return writeMenu("Set Active Version", SET_VERSION.getPath(), SET_VERSION.getParams());
    });

    httpService.get(LIST_STORES.getPath(), StoresRoutes.getAllStores(admin));
    httpService.get(CLUSTER_HELATH_STORES.getPath(), StoresRoutes.getAllStoresStatuses(admin));
    httpService.get(STORE.getPath(), StoresRoutes.getStore(admin));
    // With the get STORE endpoint above, the following endpoints can be deprecated.
    httpService.get(CURRENT_VERSION.getPath(), CurrentVersion.getRoute(admin));
    httpService.get(ACTIVE_VERSIONS.getPath(), ActiveVersions.getRoute(admin));

    httpService.get(JOB.getPath(), JobRoutes.jobStatus(admin));
    httpService.post(KILL_OFFLINE_PUSH_JOB.getPath(), JobRoutes.killOfflinePushJob(admin));
    httpService.post(SKIP_ADMIN.getPath(), SkipAdminRoute.getRoute(admin));

    httpService.post(CREATE_VERSION.getPath(), CreateVersion.getRoute(admin));
    httpService.post(NEW_STORE.getPath(), CreateStore.getRoute(admin));
    httpService.post(UPDATE_STORE.getPath(), StoresRoutes.updateStore(admin));

    httpService.post(ENABLE_STORE.getPath(), StoresRoutes.enableStore(admin));

    httpService.post(DELETE_ALL_VERSIONS.getPath(), StoresRoutes.deleteAllVersions(admin));

    httpService.post(SET_VERSION.getPath(), StoresRoutes.setCurrentVersion(admin));

    httpService.get(ClUSTER_HEALTH_INSTANCES.getPath(), NodesAndReplicas.listAllNodesStatus(admin));
    httpService.get(LIST_NODES.getPath(), NodesAndReplicas.listAllNodes(admin));
    httpService.get(LIST_REPLICAS.getPath(), NodesAndReplicas.listReplicasForStore(admin));
    httpService.get(NODE_REPLICAS.getPath(), NodesAndReplicas.listReplicasForStorageNode(admin));
    httpService.get(NODE_REMOVABLE.getPath(), NodesAndReplicas.isNodeRemovable(admin));
    httpService.post(WHITE_LIST_ADD_NODE.getPath(), NodesAndReplicas.addNodeIntoWhiteList(admin));
    httpService.post(WHITE_LIST_REMOVE_NODE.getPath(), NodesAndReplicas.removeNodeFromWhiteList(admin));
    httpService.post(REMOVE_NODE.getPath(), NodesAndReplicas.removeNodeFromCluster(admin));

    // Operations for key schema/value schema
    httpService.get(GET_KEY_SCHEMA.getPath(), SchemaRoutes.getKeySchema(admin));
    httpService.post(ADD_VALUE_SCHEMA.getPath(), SchemaRoutes.addValueSchema(admin));
    httpService.get(GET_VALUE_SCHEMA.getPath(), SchemaRoutes.getValueSchema(admin));
    httpService.post(GET_VALUE_SCHEMA_ID.getPath(), SchemaRoutes.getValueSchemaID(admin));
    httpService.get(GET_ALL_VALUE_SCHEMA.getPath(), SchemaRoutes.getAllValueSchema(admin));

    httpService.post(SET_OWNER.getPath(), StoresRoutes.setOwner(admin));
    httpService.post(SET_PARTITION_COUNT.getPath(), StoresRoutes.setPartitionCount(admin));

    // This API should be used by CORP controller only. H2V could talk to any of controllers in CORP to find who is the
    // current master CORP controller. In other colos, router will find the master controller instead of calling this API.
    httpService.get(MASTER_CONTROLLER.getPath(), MasterController.getRoute(admin));

    httpService.get(EXECUTION.getPath(), AdminCommandExecutionRoutes.getExecution(admin));
    httpService.get(LAST_SUCCEED_EXECUTION_ID.getPath(), AdminCommandExecutionRoutes.getLastSucceedExecutionId(admin));

    httpService.awaitInitialization(); // Wait for server to be initialized

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    httpService.stop();
  }

  private String writeMenu(String title, String postAction, List<String> parameters) {
    StringBuilder sb = new StringBuilder();
    sb.append("<html>");
    sb.append("<head><title>Venice " + title + "</title></head>\r\n");
    sb.append("<body bgcolor=white><style>td{font-size: 12pt;}</style>");
    sb.append("<table border=\"0\">");
    sb.append("<tr><td><h1>Venice " + title + "</h1></td></tr>");
    sb.append("</table>\r\n");
    // FORM
    sb.append("<CENTER><HR WIDTH=\"100%\" NOSHADE color=\"blue\"></CENTER>");
    sb.append("<FORM ACTION=\"" + postAction + "\" METHOD=\"POST\">");
    sb.append("<table border=\"0\">");
    for (String param : parameters){
      sb.append("<tr><td>"+param+": <br> <input type=text name=\""+param+"\" size=20></td></tr>");
    }
    sb.append("<tr><td><INPUT TYPE=\"submit\" NAME=\"Send\" VALUE=\"Send\"></INPUT></td>");
    sb.append("<td><INPUT TYPE=\"reset\" NAME=\"Clear\" VALUE=\"Clear\" ></INPUT></td></tr>");
    sb.append("</table></FORM>\r\n");
    sb.append("<CENTER><HR WIDTH=\"100%\" NOSHADE color=\"blue\"></CENTER>");
    sb.append("</body>");
    sb.append("</html>");
    return sb.toString();
  }

  protected static void validateParams(Request request, List<String> requiredParams, Admin admin){
    String clusterName = request.queryParams(CLUSTER);
    if (Utils.isNullOrEmpty(clusterName)){
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, CLUSTER + " is a required parameter");
    }
    if (!request.pathInfo().equals(MASTER_CONTROLLER.getPath()) && !admin.isMasterController(clusterName)){
      // Skip master controller check for '/master_controller' request
      throw new VeniceHttpException(HttpConstants.SC_MISDIRECTED_REQUEST, "This is not the active controller");
    }
    for (String param : requiredParams){
      if (Utils.isNullOrEmpty(request.queryParams(param))){
        throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, param + " is a required parameter");
      }
    }
  }

  protected static String getOptionalParameterValue(Request request, String parameter, String defaultValue) {
    String value = request.queryParams(parameter);
    if (value == null) {
      value = defaultValue;
    }
    return value;
  }

  protected static void handleError(Throwable e, Request request, Response response) {
    StringBuilder sb = new StringBuilder("Request params were: ");
    request.queryMap().toMap().forEach((k, v) -> {  /* Map<String, String[]> */
      sb.append(k).append("=").append(String.join(",",v)).append(" ");
    });
    String errMsg = sb.toString();
    logger.error(errMsg, e);
    if (e instanceof Error) {
      throw (Error) e;
    }
    int statusCode = e instanceof VeniceException ?
        ((VeniceException) e).getHttpStatusCode() :
        HttpStatus.SC_INTERNAL_SERVER_ERROR;
    response.status(statusCode);

    request.attribute(REQUEST_SUCCEED, false);
  }
}
