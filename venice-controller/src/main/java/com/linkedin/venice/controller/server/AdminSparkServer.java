package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
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
import spark.Spark;


/**
 * Controller admin API leveraging sparkjava: http://sparkjava.com/documentation.html
 */
public class AdminSparkServer extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(AdminSparkServer.class);

  private final int port;
  private final String clusterName;
  private final Admin admin;
  private final ObjectMapper mapper = new ObjectMapper();

  public AdminSparkServer(int port, String clusterName, Admin admin) {
    super("controller-admin-server");
    this.port = port;
    this.clusterName = clusterName;
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
      int versionNumber = 0;
      String storeName = null;
      JobStatusQueryResponse responseObject = new JobStatusQueryResponse();
      response.type(HttpConstants.JSON);
      //validate parameters at first.
      try {
        validateParams(request, JOB_PARMAS);
        storeName = request.queryParams(NAME);
        versionNumber = Utils
            .parseIntFromString(request.queryParams(VERSION), VERSION);
      } catch (VeniceException e) {
        String errorMsg = getErrorMessage("Failed to validate parameters.", JOB_PARMAS, request);
        logger.error(errorMsg, e);
        responseObject.setError(errorMsg + e.getMessage());
        response.status(HttpStatus.SC_BAD_REQUEST);
        return mapper.writeValueAsString(responseObject);
      }
      //Do the query.
      try {
        responseObject.setName(storeName);
        responseObject.setVersion(versionNumber);
        Version version = new Version(storeName, versionNumber);
        //TODO Support getting streaming job's status in the future.
        String jobStatus = admin.getOffLineJobStatus(clusterName, version.kafkaTopicName()).toString();
        responseObject.setStatus(jobStatus);
      } catch (Exception e) {
        String errorMsg =
            getErrorMessage("Error: when querying the job's status from admin.", JOB_PARMAS,
                request);
        logger.error(errorMsg, e);
        responseObject.setError(errorMsg + e.getMessage());
        response.status(HttpStatus.SC_INTERNAL_SERVER_ERROR);
      }
      return mapper.writeValueAsString(responseObject);
    });

    Spark.post(CREATE_PATH, (request, response) -> {
      StoreCreationResponse responseObject = new StoreCreationResponse();
      try {
        validateParams(request, CREATE_PARAMS); //throws venice exception
        responseObject.setName(request.queryParams(NAME));
        responseObject.setOwner(request.queryParams(OWNER));
        int storeSizeMb = Utils.parseIntFromString(request.queryParams(STORE_SIZE),
            STORE_SIZE);
        responseObject.setPartitions(3); // TODO actual partitioning logic based on store size
        responseObject.setReplicas(1); // TODO configurable replication
        try { // TODO: use admin to update store with new owner?  Set owner at version level for audit history?
          admin.addStore(clusterName, responseObject.getName(), responseObject.getOwner());
        } catch (VeniceException e) { // TODO method on admin to see if store already created?
          logger.warn("Store" + responseObject.getName() + " probably already created.", e);
        }
        Version version = admin.incrementVersion(clusterName, responseObject.getName(), responseObject.getPartitions(),
            responseObject.getReplicas());
        responseObject.setVersion(version.getNumber());
        responseObject.setKafkaTopic(version.kafkaTopicName());
        responseObject.setKafkaBootstrapServers(admin.getKafkaBootstrapServers());
      } catch (VeniceException e) {
        logger.error(e);
        responseObject.setError(e.getMessage());
        response.status(HttpStatus.SC_BAD_REQUEST);
      } catch (Exception e) {
        logger.error(e);
        responseObject.setError(e.getMessage());
        response.status(HttpStatus.SC_INTERNAL_SERVER_ERROR);
      }
      response.type(HttpConstants.JSON);
      return mapper.writeValueAsString(responseObject);
    });

    // Only to be used manually for testing purposes.  No corresponding method in controller client.
    Spark.post(SETVERSION_PATH, (request, response) -> {
      Map<String, String> responseMap = new HashMap<>();
      try {
        validateParams(request, SETVERSION_PARAMS); //throws venice exception
        String storeName = request.queryParams(NAME);
        int version = Utils
            .parseIntFromString(request.queryParams(VERSION), VERSION);
        admin.setCurrentVersion(clusterName, storeName, version);
        responseMap.put(STATUS, "success");
      } catch (VeniceException e) {
        logger.error(e);
        responseMap.put(ERROR, e.getMessage());
        response.status(HttpStatus.SC_BAD_REQUEST);
      } catch (Exception e) {
        logger.error(e);
        responseMap.put(ERROR, e.getMessage());
        response.status(HttpStatus.SC_INTERNAL_SERVER_ERROR);
      }
      response.type(HttpConstants.JSON);
      return mapper.writeValueAsString(responseMap);
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
    sb.append("<tr><td><h1>" + title + "</h1></td></tr>");
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
    for (String param : requiredParams){
      if (null == request.queryParams(param)){
        throw new VeniceException(param + " is a required parameter");
      }
    }
  }

  private String getErrorMessage(String summary, List<String> params, Request request) {
    String errorMsg = summary + " Parameters:";
    for (String paramName : params) {
      errorMsg += paramName + "=" + request.queryParams(paramName);
    }
    return errorMsg + ".";
  }
}
