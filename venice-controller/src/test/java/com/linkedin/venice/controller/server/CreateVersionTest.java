package com.linkedin.venice.controller.server;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.utils.DataProviderUtils;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.security.auth.x500.X500Principal;
import javax.servlet.http.HttpServletRequest;
import org.apache.http.HttpStatus;
import org.testng.annotations.Test;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;
import spark.Route;

import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;
import static com.linkedin.venice.controllerapi.ControllerRoute.*;
import static org.mockito.Mockito.*;


public class CreateVersionTest {
  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testCreateVersionWithACL(boolean checkReadMethod) {
    String storeName = "test_store";
    String user = "test_user";

    // Mock an Admin
    Admin admin = mock(Admin.class);

    // Mock a certificate
    X509Certificate certificate = mock(X509Certificate.class);
    X509Certificate[] certificateArray = new X509Certificate[1];
    certificateArray[0] = certificate;
    X500Principal principal = new X500Principal("CN=" + user);
    doReturn(principal).when(certificate).getSubjectX500Principal();

    // Mock a spark request
    Request request = mock(Request.class);
    doReturn("localhost").when(request).host();
    doReturn("0.0.0.0").when(request).ip();
    HttpServletRequest rawRequest = mock(HttpServletRequest.class);
    doReturn(rawRequest).when(request).raw();
    doReturn(certificateArray).when(rawRequest).getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME);
    doReturn(storeName).when(request).queryParams(NAME);

    // Mock a spark response
    Response response = mock(Response.class);

    // Mock a AccessClient
    DynamicAccessController accessClient = mock(DynamicAccessController.class);

    /**
     * Build a CreateVersion route.
     */
    CreateVersion createVersion = new CreateVersion(Optional.of(accessClient), checkReadMethod);
    Route createVersionRoute = createVersion.requestTopicForPushing(admin);

    // Not a whitelist user.
    doReturn(false).when(accessClient).isWhitelistUsers(certificate, storeName, "GET");

    /**
     * Create version should fail if user doesn't have "Write" method access to the topic
     */
    try {
      doReturn(false).when(accessClient).hasAccessToTopic(certificate, storeName, "Write");
      createVersionRoute.handle(request, response);
    } catch (Exception e) {
      throw new VeniceException(e);
    }

    /**
     * Response should be 403 if user doesn't have "Write" method access
     */
    verify(response).status(org.apache.http.HttpStatus.SC_FORBIDDEN);

    if (checkReadMethod) {
      // Mock another response
      Response response2 = mock(Response.class);
      /**
       * Create version should fail if user has "Write" method access but not "Read" method access to topics.
       */
      try {
        doReturn(true).when(accessClient).hasAccessToTopic(certificate, storeName, "Write");
        doReturn(false).when(accessClient).hasAccessToTopic(certificate, storeName, "Read");
        createVersionRoute.handle(request, response2);
      } catch (Exception e) {
        throw new VeniceException(e);
      }

      verify(response2).status(org.apache.http.HttpStatus.SC_FORBIDDEN);
    }
  }

  @Test
  public void testCreateVersionFailsIfIncrementalPushMadeWithHybridStoreWithoutMakingFullPush() {
    String storeName = "test_store";
    String user = "test_user";
    String clusterName = "test_cluster";
    String pushJobId1 = "push_1";
    String hostname = "localhost";

    // Mock an Admin
    Admin admin = mock(Admin.class);
    doReturn(true).when(admin).isMasterController(clusterName);
    doReturn(true).when(admin).whetherEnableBatchPushFromAdmin();

    // Mock a certificate
    X509Certificate certificate = mock(X509Certificate.class);
    X509Certificate[] certificateArray = new X509Certificate[1];
    certificateArray[0] = certificate;
    X500Principal principal = new X500Principal("CN=" + user);
    doReturn(principal).when(certificate).getSubjectX500Principal();

    // Setting query params
    Map<String, String[]> queryMap = new HashMap<>();
    queryMap.put("store_name", new String[]{storeName});
    queryMap.put("store_size", new String[]{"0"});
    queryMap.put("push_type", new String[]{Version.PushType.INCREMENTAL.name()});
    queryMap.put("push_job_id", new String[]{pushJobId1});
    queryMap.put("hostname", new String[]{hostname});

    HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
    doReturn(queryMap).when(httpServletRequest).getParameterMap();
    doReturn(certificateArray).when(httpServletRequest).getAttribute(CONTROLLER_SSL_CERTIFICATE_ATTRIBUTE_NAME);
    QueryParamsMap queryParamsMap = new QueryParamsMap(httpServletRequest);

    // Mock a spark request
    Request request = mock(Request.class);
    doReturn(httpServletRequest).when(request).raw();
    doReturn(queryParamsMap).when(request).queryMap();

    doReturn(hostname).when(request).host();
    doReturn("0.0.0.0").when(request).ip();
    doReturn(clusterName).when(request).queryParams(CLUSTER);
    doReturn(REQUEST_TOPIC.getPath()).when(request).pathInfo();

    for (Map.Entry<String, String[]> queryParam : queryMap.entrySet()) {
      doReturn(queryParam.getValue()[0]).when(request).queryParams(queryParam.getKey());
    }

    // Setting up a store with hybrid and incremental enabled and incremental policy = INCREMENTAL_PUSH_SAME_AS_REAL_TIME
    Store store = new ZKStore(storeName, "abc@linkedin.com", 10, PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH, ReadStrategy.ANY_OF_ONLINE, OfflinePushStrategy.WAIT_ALL_REPLICAS, 1);
    store.setIncrementalPushPolicy(IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME);
    store.setHybridStoreConfig(new HybridStoreConfigImpl(0, 1, HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD));

    doReturn(store).when(admin).getStore(clusterName, storeName);

    // Setting up a version that doesn't have the incremental policy set as INCREMENTAL_PUSH_SAME_AS_REAL_TIME
    Version version = new VersionImpl(storeName, 1, pushJobId1);
    version.setIncrementalPushPolicy(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC);
    doReturn(version).when(admin).incrementVersionIdempotent(clusterName, storeName, pushJobId1, 0, 0,
        Version.PushType.INCREMENTAL, false, false, null, Optional.empty(), Optional.empty(), -1);

    // Mock a spark response
    Response response = mock(Response.class);

    // Mock a AccessClient
    DynamicAccessController accessClient = mock(DynamicAccessController.class);

    /**
     * Build a CreateVersion route.
     */
    CreateVersion createVersion = new CreateVersion(Optional.of(accessClient), false);
    Route createVersionRoute = createVersion.requestTopicForPushing(admin);

    doReturn(true).when(accessClient).isWhitelistUsers(certificate, storeName, "GET");

    /**
     * Create version should fail if the store is hybrid and incremental but the current version's incremental push
     * policy is not "INCREMENTAL_PUSH_SAME_AS_REAL_TIME".
     */
    try {
      createVersionRoute.handle(request, response);
    } catch (Exception e) {
      throw new VeniceException(e);
    }

    verify(response).status(HttpStatus.SC_NOT_IMPLEMENTED);
  }
}
