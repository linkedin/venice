package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.AuditInfo;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.stats.ControllerStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Utils;
import java.util.List;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import spark.Request;
import spark.Response;
import spark.Spark;

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
  private final ControllerStats stats = ControllerStats.getInstance();

  private static String REQUEST_START_TIME =  "startTime";
  private static String REQUEST_SUCCEED = "succeed";

  public AdminSparkServer(int port, Admin admin) {
    this.port = port;
    //Note: admin is passed in as a reference.  The expectation is the source of the admin will
    //      close it so we don't close it in stopInner()
    this.admin = admin;
  }

  @Override
  public boolean startInner() throws Exception {
    Spark.port(port);

    Spark.before((request, response) -> {
      AuditInfo audit = new AuditInfo(request);
      logger.info(audit.toString());

      stats.recordRequest();
      request.attribute(REQUEST_START_TIME, System.currentTimeMillis());
      request.attribute(REQUEST_SUCCEED, true);
    });

    Spark.after((request, response) -> {
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

    Spark.get(CREATE.getPath(), (request, response) -> {
      response.type(HttpConstants.TEXT_HTML);
      return writeMenu("Create New Store", CREATE.getPath(), CREATE.getParams());
    });

    Spark.get(SETVERSION.getPath(), (request, response) -> {
      response.type(HttpConstants.TEXT_HTML);
      return writeMenu("Set Active Version", SETVERSION.getPath(), SETVERSION.getParams());
    });

    Spark.get(LIST_STORES.getPath(), StoresRoutes.getAllStores(admin));
    Spark.get(STORE.getPath(), StoresRoutes.getStore(admin));
    // With the get STORE endpoint above, the following endpoints can be deprecated.
    Spark.get(CURRENT_VERSION.getPath(), CurrentVersion.getRoute(admin));
    Spark.get(ACTIVE_VERSIONS.getPath(), ActiveVersions.getRoute(admin));
    Spark.get(NEXTVERSION.getPath(), NextVersion.getRoute(admin));

    Spark.get(JOB.getPath(), JobStatus.getRoute(admin));

    Spark.post(CREATE.getPath(), CreateVersion.getRoute(admin)); /* create new store-version for testing */
    Spark.post(NEWSTORE.getPath(), CreateStore.getRoute(admin));

    Spark.post(PAUSE_STORE.getPath(), StoresRoutes.pauseStore(admin));
    Spark.post(SETVERSION.getPath(), SetVersion.getRoute(admin));
    Spark.post(RESERVE_VERSION.getPath(), ReserveVersion.getRoute(admin));

    Spark.get(LIST_NODES.getPath(), NodesAndReplicas.listAllNodes(admin));
    Spark.get(LIST_REPLICAS.getPath(), NodesAndReplicas.listReplicasForStore(admin));
    Spark.get(NODE_REPLICAS.getPath(), NodesAndReplicas.listReplicasForStorageNode(admin));
    Spark.get(NODE_REMOVABLE.getPath(), NodesAndReplicas.isNodeRemovable(admin));

    // Operations for key schema/value schema
    Spark.post(INIT_KEY_SCHEMA.getPath(), SchemaRoutes.initKeySchema(admin));
    Spark.get(GET_KEY_SCHEMA.getPath(), SchemaRoutes.getKeySchema(admin));
    Spark.post(ADD_VALUE_SCHEMA.getPath(), SchemaRoutes.addValueSchema(admin));
    Spark.get(GET_VALUE_SCHEMA.getPath(), SchemaRoutes.getValueSchema(admin));
    Spark.post(GET_VALUE_SCHEMA_ID.getPath(), SchemaRoutes.getValueSchemaID(admin));
    Spark.get(GET_ALL_VALUE_SCHEMA.getPath(), SchemaRoutes.getAllValueSchema(admin));

    /**
     * This API should be used by CORP controller only. H2V could talk to any of controllers in CORP to find who is the
     * current master CORP controller. In other colos, router will find the master controller instead of calling this API.
     */
    Spark.get(GET_MASTER_CONTROLLER.getPath(), MasterController.getRoute(admin));

    Spark.awaitInitialization(); // Wait for server to be initialized

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    Spark.stop(); // must be qualified so it doesn't call AbstractVeniceService.stop()
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
    if (!admin.isMasterController(clusterName)){
      throw new VeniceHttpException(HttpConstants.SC_MISDIRECTED_REQUEST, "This is not the active controller");
    }

    for (String param : requiredParams){
      if (Utils.isNullOrEmpty(request.queryParams(param))){
        throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, param + " is a required parameter");
      }
    }
  }

  protected static void handleError(VeniceException e, Request request, Response response){
    StringBuilder sb = new StringBuilder("Request params were: ");
    request.queryMap().toMap().forEach((k, v) -> {  /* Map<String, String[]> */
      sb.append(k).append("=").append(String.join(",",v)).append(" ");
    });
    String errMsg = sb.toString();
    logger.error(errMsg, e);
    int statusCode = e instanceof VeniceHttpException ?
        ((VeniceHttpException) e).getHttpStatusCode() :
        HttpStatus.SC_INTERNAL_SERVER_ERROR;
    response.status(statusCode);

    request.attribute(REQUEST_SUCCEED, false);
  }
}
