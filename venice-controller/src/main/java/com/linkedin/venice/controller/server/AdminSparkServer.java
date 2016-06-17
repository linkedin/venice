package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
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


/**
 * Controller admin API leveraging sparkjava: http://sparkjava.com/documentation.html
 */
public class AdminSparkServer extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(AdminSparkServer.class);

  private final int port;
  private final Admin admin;
  protected static final ObjectMapper mapper = new ObjectMapper();

  public AdminSparkServer(int port, Admin admin) {
    super("controller-admin-server");
    this.port = port;
    //Note: admin is passed in as a reference.  The expectation is the source of the admin will
    //      close it so we don't close it in stopInner()
    this.admin = admin;
  }

  @Override
  public void startInner() throws Exception {
    Spark.port(port);

    Spark.get(CREATE_PATH, (request, response) -> {
      response.type(HttpConstants.TEXT_HTML);
      return writeMenu("Create New Store", CREATE_PATH, CREATE_PARAMS);
    });

    Spark.get(SETVERSION_PATH, (request, response) -> {
      response.type(HttpConstants.TEXT_HTML);
      return writeMenu("Set Active Version", SETVERSION_PATH, SETVERSION_PARAMS);
    });

    Spark.get(CURRENT_VERSION_PATH, CurrentVersion.getRoute(admin));

    Spark.get(ACTIVE_VERSIONS_PATH, ActiveVersions.getRoute(admin));

    Spark.get(JOB_PATH, JobStatus.getRoute(admin));

    Spark.post(CREATE_PATH, CreateVersion.getRoute(admin));

    Spark.post(NEWSTORE_PATH, CreateStore.getRoute(admin));

    Spark.get(LIST_STORES_PATH, AllStores.getRoute(admin));

    // Only to be used manually and for testing purposes.
    Spark.post(SETVERSION_PATH, SetVersion.getRoute(admin));

    /**
     * Query the controller for the next version of a store that should be created
     */
    Spark.get(NEXTVERSION_PATH, NextVersion.getRoute(admin));

    Spark.post(RESERVE_VERSION_PATH, ReserveVersion.getRoute(admin));

    // Operations for key schema/value schema
    Spark.post(INIT_KEY_SCHEMA, SchemaRoutes.initKeySchema(admin));
    Spark.get(GET_KEY_SCHEMA_PATH, SchemaRoutes.getKeySchema(admin));
    Spark.post(ADD_VALUE_SCHEMA_PATH, SchemaRoutes.addValueSchema(admin));
    Spark.get(GET_VALUE_SCHEMA_PATH, SchemaRoutes.getValueSchema(admin));
    Spark.post(GET_VALUE_SCHEMA_ID_PATH, SchemaRoutes.getValueSchemaID(admin));
    Spark.get(GET_ALL_VALUE_SCHEMA_PATH, SchemaRoutes.getAllValueSchema(admin));

    Spark.awaitInitialization(); // Wait for server to be initialized
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
  }
}
