package com.linkedin.venice.controller.server;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.StoreCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.service.AbstractVeniceService;
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

  public AdminSparkServer(int port, String clusterName, Admin admin){
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

    Spark.get(ControllerApiConstants.CREATE_PATH, (request, response) -> {
      response.type(HttpConstants.TEXT_HTML);
      return writeMenu("Venice Store Creator", ControllerApiConstants.CREATE_PATH,
          ControllerApiConstants.CREATE_PARAMS);
    });

    Spark.get(ControllerApiConstants.SETVERSION_PATH, (request, response) -> {
      response.type(HttpConstants.TEXT_HTML);
      return writeMenu("Venice Store Creator", ControllerApiConstants.SETVERSION_PATH,
          ControllerApiConstants.SETVERSION_PARAMS);
    });

    Spark.get(ControllerApiConstants.JOB_PATH, (request, response) -> {
      validateParams(request, ControllerApiConstants.JOB_PARMAS);

      Map<String, String> responseMap = new HashMap<>();
      try {
        int versionNumber = Integer.valueOf(request.queryParams(ControllerApiConstants.VERSION));
        String storeName = request.queryParams(ControllerApiConstants.NAME);
        Version version = new Version(storeName, versionNumber);
        //TODO Support getting streaming job's status in the further.
        String jobStatus = admin.getOffLineJobStatus(clusterName, version.kafkaTopicName()).toString();

        responseMap.put(ControllerApiConstants.STATUS, jobStatus);
      } catch (NumberFormatException e) {
        String errorMsg = ControllerApiConstants.VERSION + " must be an integer. Now it's:" + request
            .queryParams(ControllerApiConstants.VERSION) + ".";
        logger.error(errorMsg, e);
        responseMap.put(ControllerApiConstants.ERROR, errorMsg);
        response.status(HttpStatus.SC_BAD_REQUEST);
      } catch (VeniceException e) {
        String errorMsg =
            "Error: when querying the job's status for store:" + request.queryParams(ControllerApiConstants.NAME)
                + " version:" + request.queryParams(ControllerApiConstants.VERSION) + ".";
        logger.error(errorMsg, e);
        responseMap.put(ControllerApiConstants.ERROR, errorMsg + e.getMessage());
        response.status(HttpStatus.SC_INTERNAL_SERVER_ERROR);
      } catch (Exception e) {
        logger.error(e);
        responseMap.put(ControllerApiConstants.ERROR, e.getMessage());
        response.status(HttpStatus.SC_INTERNAL_SERVER_ERROR);
      }
      response.type(HttpConstants.JSON);
      return mapper.writeValueAsString(responseMap);
    });

    Spark.post(ControllerApiConstants.CREATE_PATH, (request, response) -> {
      StoreCreationResponse responseObject = new StoreCreationResponse();
      try {
        validateParams(request, ControllerApiConstants.CREATE_PARAMS); //throws venice exception
        responseObject.setName(request.queryParams(ControllerApiConstants.NAME));
        responseObject.setOwner(request.queryParams(ControllerApiConstants.OWNER));
        try {
          int storeSizeMb = Integer.valueOf(request.queryParams(ControllerApiConstants.STORE_SIZE));
        } catch (NumberFormatException e) {
          throw new VeniceException(ControllerApiConstants.STORE_SIZE + " must be an integer", e);
        }
        responseObject.setPartitions(3); // TODO actual partitioning logic based on store size
        responseObject.setReplicas(1); // TODO configurable replication
        try { // TODO: use admin to update store with new owner?  Set owner at version level for audit history?
          admin.addStore(clusterName, responseObject.getName(), responseObject.getOwner());
        } catch (VeniceException e) { // TODO method on admin to see if store already created?
          logger.warn("Store probably already created.", e);
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
    Spark.post(ControllerApiConstants.SETVERSION_PATH, (request, response) -> {
      Map<String, String> responseMap = new HashMap<>();
      try {
        validateParams(request, ControllerApiConstants.SETVERSION_PARAMS); //throws venice exception
        String storeName = request.queryParams(ControllerApiConstants.NAME);
        int version = Integer.valueOf(request.queryParams(ControllerApiConstants.VERSION));
        admin.setCurrentVersion(clusterName, storeName, version);
        responseMap.put(ControllerApiConstants.STATUS, "success");
      } catch (VeniceException e) {
        responseMap.put(ControllerApiConstants.ERROR, e.getMessage());
        response.status(HttpStatus.SC_BAD_REQUEST);
      } catch (Exception e) {
        logger.error(e);
        responseMap.put(ControllerApiConstants.ERROR, e.getMessage());
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
    sb.append("<head><title>" + title + "</title></head>\r\n");
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
}
