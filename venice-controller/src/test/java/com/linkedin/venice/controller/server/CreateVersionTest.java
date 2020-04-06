package com.linkedin.venice.controller.server;

import static com.linkedin.venice.VeniceConstants.*;
import static org.mockito.Mockito.*;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;

import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Optional;
import javax.security.auth.x500.X500Principal;
import javax.servlet.http.HttpServletRequest;
import org.apache.log4j.Logger;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import spark.Request;
import spark.Response;
import spark.Route;


public class CreateVersionTest {
  @DataProvider(name = "checkReadMethod")
  public static Object[][] checkReadMethod() {
    return new Object[][] {
        {false}, {true}
    };
  }

  @Test(dataProvider = "checkReadMethod")
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
}
