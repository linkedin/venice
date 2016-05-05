package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.VersionResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.Utils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private final ObjectMapper mapper = new ObjectMapper();

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

    Spark.get(JOB_PATH, (request, response) -> {
      JobStatusQueryResponse responseObject = new JobStatusQueryResponse();
      try {
        validateParams(request, JOB_PARMAS);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        responseObject.setVersion(Utils.parseIntFromString(request.queryParams(VERSION), VERSION));
        Version version = new Version(responseObject.getName(), responseObject.getVersion());
        //TODO Support getting streaming job's status in the future.
        String jobStatus = admin.getOffLineJobStatus(responseObject.getCluster(), version.kafkaTopicName()).toString();
        responseObject.setStatus(jobStatus);
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return mapper.writeValueAsString(responseObject);
    });

    Spark.post(CREATE_PATH, (request, response) -> {
      VersionCreationResponse responseObject = new VersionCreationResponse();
      try {
        validateParams(request, CREATE_PARAMS);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        responseObject.setOwner(request.queryParams(OWNER));
        // Store size in Bytes
        long storeSize = Utils.parseLongFromString(request.queryParams(STORE_SIZE), STORE_SIZE);
        responseObject.setPartitions(3); // TODO actual partitioning logic based on store size
        responseObject.setReplicas(1); // TODO configurable replication
        try { // TODO: use admin to update store with new owner?  Set owner at version level for audit history?
          admin.addStore(responseObject.getCluster(), responseObject.getName(), responseObject.getOwner());
        } catch (VeniceException e) { // TODO method on admin to see if store already created?
          logger.warn("Store" + responseObject.getName() + " probably already created.", e);
        }
        Version version = admin
            .incrementVersion(responseObject.getCluster(), responseObject.getName(), responseObject.getPartitions(),
                responseObject.getReplicas());
        responseObject.setVersion(version.getNumber());
        responseObject.setKafkaTopic(version.kafkaTopicName());
        responseObject.setKafkaBootstrapServers(admin.getKafkaBootstrapServers());
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return mapper.writeValueAsString(responseObject);
    });

    Spark.post(NEWSTORE_PATH, (request, response) -> {
      NewStoreResponse responseObject = new NewStoreResponse();
      try {
        validateParams(request, NEWSTORE_PARAMS);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        responseObject.setOwner(request.queryParams(OWNER));
        admin.addStore(responseObject.getCluster(), responseObject.getName(), responseObject.getOwner());
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return mapper.writeValueAsString(responseObject);
    });

    // Only to be used manually and for testing purposes.
    Spark.post(SETVERSION_PATH, (request, response) -> {
      Map<String, String> responseMap = new HashMap<>();
      try {
        validateParams(request, SETVERSION_PARAMS); //throws venice exception
        String clusterName = request.queryParams(CLUSTER);
        String storeName = request.queryParams(NAME);
        int version = Utils.parseIntFromString(request.queryParams(VERSION), VERSION);
        admin.setCurrentVersion(clusterName, storeName, version);
        responseMap.put(STATUS, "success");
      } catch (VeniceException e) {
        responseMap.put(ERROR, e.getMessage());
        handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return mapper.writeValueAsString(responseMap);
    });

    /**
     * Query the controller for the next version of a store that should be created
     */
    Spark.get(NEXTVERSION_PATH, (request, response) -> {
      VersionResponse responseObject = new VersionResponse();
      try {
        validateParams(request, NEXTVERSION_PARAMS);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        Version version = admin.peekNextVersion(responseObject.getCluster(), responseObject.getName());
        responseObject.setVersion(version.getNumber());
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return mapper.writeValueAsString(responseObject);
    });

    Spark.post(RESERVE_VERSION_PATH, (request, response) -> {
      VersionResponse responseObject = new VersionResponse();
      try {
        validateParams(request, RESERVE_VERSION_PARAMS);
        responseObject.setCluster(request.queryParams(CLUSTER));
        responseObject.setName(request.queryParams(NAME));
        responseObject.setVersion(Utils.parseIntFromString(request.queryParams(VERSION), VERSION));
        admin.reserveVersion(responseObject.getCluster(), responseObject.getName(), responseObject.getVersion());
      } catch (VeniceException e) {
        responseObject.setError(e.getMessage());
        handleError(e, request, response);
      }
      response.type(HttpConstants.JSON);
      return mapper.writeValueAsString(responseObject);
  });

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

  private void validateParams(Request request, List<String> requiredParams){
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

  private void handleError(VeniceException e, Request request, Response response){
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
