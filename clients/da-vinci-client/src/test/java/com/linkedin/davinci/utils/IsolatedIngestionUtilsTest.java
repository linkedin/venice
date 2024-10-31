package com.linkedin.davinci.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.ingestion.protocol.enums.IngestionAction;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.testng.annotations.Test;


public class IsolatedIngestionUtilsTest {
  @Test
  public void testGetIngestionActionFromRequest() {
    HttpRequest request;
    // Validate REPORT
    request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "http://127.0.0.1:27015/REPORT");
    assertEquals(IsolatedIngestionUtils.getIngestionActionFromRequest(request), IngestionAction.REPORT);
    // Validate METRIC
    request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "http://127.0.0.1:27015/METRIC");
    assertEquals(IsolatedIngestionUtils.getIngestionActionFromRequest(request), IngestionAction.METRIC);
    // Validate COMMAND
    request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "http://127.0.0.1:27015/COMMAND");
    assertEquals(IsolatedIngestionUtils.getIngestionActionFromRequest(request), IngestionAction.COMMAND);
    // Invalid URI style 1
    request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "http://127.0.0.1:27015");
    HttpRequest finalRequest = request;
    assertThrows(VeniceException.class, () -> IsolatedIngestionUtils.getIngestionActionFromRequest(finalRequest));
    // Invalid URI style 2
    request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "http://127.0.0.1:27015/FAIL");
    HttpRequest finalRequest2 = request;
    assertThrows(VeniceException.class, () -> IsolatedIngestionUtils.getIngestionActionFromRequest(finalRequest2));
  }

  @Test
  public void testExecuteShellCommand() {
    String pwdResult = IsolatedIngestionUtils.executeShellCommand("pwd");
    assertNotNull(pwdResult);
    assertTrue(pwdResult.startsWith("/"));

    String nonExistingCommandResult = IsolatedIngestionUtils.executeShellCommand("blablabla");
    assertNotNull(nonExistingCommandResult);
    assertEquals(nonExistingCommandResult, "");

    String failedCommandResult = IsolatedIngestionUtils.executeShellCommand("cp bogus1 bogus2");
    assertNotNull(failedCommandResult);
    assertEquals(failedCommandResult, "");

    String emptyCommandResult = IsolatedIngestionUtils.executeShellCommand("");
    assertNotNull(emptyCommandResult);
    assertEquals(emptyCommandResult, "");

    assertThrows(VeniceException.class, () -> IsolatedIngestionUtils.executeShellCommand("kill xyz"));
  }
}
