package com.linkedin.venice.controller.server;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;


public class ClusterRoutesTest {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static final String TEST_CLUSTER = "test_cluster";

  @Test
  public void testUpdateDarkClusterConfig() throws Exception {
    VeniceHelixAdmin mockVeniceHelixAdmin = mock(VeniceHelixAdmin.class);
    VeniceControllerMultiClusterConfig controllerMultiClusterConfig = mock(VeniceControllerMultiClusterConfig.class);

    doReturn(true).when(mockVeniceHelixAdmin).isLeaderControllerFor(anyString());
    when(mockVeniceHelixAdmin.getMultiClusterConfigs()).thenReturn(controllerMultiClusterConfig);
    doReturn(true).when(controllerMultiClusterConfig).isDarkCluster();

    Request request = mock(Request.class);

    QueryParamsMap queryParamsMap = mock(QueryParamsMap.class);
    when(request.queryMap()).thenReturn(queryParamsMap);

    Map<String, String[]> queryMapData = new HashMap<>();
    queryMapData.put(ControllerApiConstants.CLUSTER, new String[] { TEST_CLUSTER });
    queryMapData.put(ControllerApiConstants.STORES_TO_REPLICATE, new String[] { "store1,store2" });

    when(queryParamsMap.toMap()).thenReturn(queryMapData);
    when(request.queryParams(ControllerApiConstants.CLUSTER)).thenReturn(TEST_CLUSTER);
    Route updateDarkClusterConfigRoute =
        new ClusterRoutes(false, Optional.empty()).updateDarkClusterConfig(mockVeniceHelixAdmin);

    ControllerResponse response = OBJECT_MAPPER.readValue(
        updateDarkClusterConfigRoute.handle(request, mock(Response.class)).toString(),
        ControllerResponse.class);
    assertFalse(response.isError());
  }
}
