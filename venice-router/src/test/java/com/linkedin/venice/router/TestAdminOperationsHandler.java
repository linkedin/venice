package com.linkedin.venice.router;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.acl.AccessController;
import com.linkedin.venice.acl.AclException;
import com.linkedin.venice.router.stats.AdminOperationsStats;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSession;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.linkedin.venice.router.AdminOperationsHandler.*;
import static com.linkedin.venice.router.api.VenicePathParser.*;
import static org.mockito.Mockito.*;


public class TestAdminOperationsHandler {
  private RouterServer router;
  private AdminOperationsStats stats;
  private VeniceRouterConfig config;
  private AccessController accessController;
  private AdminOperationsHandler adminOperationsHandler;

  private static final String BASE_ADMIN_URI = String.join("/", "", TYPE_ADMIN);
  private static final String READ_QUOTA_THROTTLE_URI = String.join("/", BASE_ADMIN_URI, TASK_READ_QUOTA_THROTTLE);
  private static final String READ_QUOTA_THROTTLE_ENABLE_URI = String.join("/", READ_QUOTA_THROTTLE_URI, ACTION_ENABLE);
  private static final String READ_QUOTA_THROTTLE_DISABLE_URI = String.join("/", READ_QUOTA_THROTTLE_URI, ACTION_DISABLE);

  private static final String INCORRECT_ADMIN_TASK = String.join("/", BASE_ADMIN_URI, "incorrect");
  private static final String READ_QUOTA_THROTTLE_INCORRECT_ACTION_URI = String.join("/", READ_QUOTA_THROTTLE_URI, "incorrect");

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final ConfigFake fake = new ConfigFake();

  private class ConfigFake {
    private boolean readThrottlingEnabled;
    private boolean earlyThrottleEnabled;
  };

  @DataProvider(name = "TwoBoolean")
  public static Object[][] twoBoolean() {
    return new Object[][] {
        { false, false },
        { false, true },
        { true, false },
        { true, true },
    };
  }

  @DataProvider(name = "FourBoolean")
  public static Object[][] fourBoolean() {
    return new Object[][] {
        { false, false, false, false },
        { false, false, false, true },
        { false, false, true, false },
        { false, false, true, true },
        { false, true, false, false },
        { false, true, false, true },
        { false, true, true, false },
        { false, true, true, true },
        { true, false, false, false },
        { true, false, false, true },
        { true, false, true, false },
        { true, false, true, true },
        { true, true, false, false },
        { true, true, false, true },
        { true, true, true, false },
        { true, true, true, true },
    };
  }

  @BeforeMethod
  public void setupTest() throws AclException {
    router = mock(RouterServer.class);
    stats = mock(AdminOperationsStats.class);
    config = mock(VeniceRouterConfig.class);
    doReturn(config).when(router).getConfig();
    doReturn(1000L).when(config).getReadQuotaThrottlingLeaseTimeoutMs();

    doAnswer(invocation -> {
      fake.readThrottlingEnabled = invocation.getArgument(0, Boolean.class);
      return null;
    }).when(config).setReadThrottlingEnabled(anyBoolean());

    doAnswer(invocation -> fake.readThrottlingEnabled).when(config).isReadThrottlingEnabled();

    doAnswer(invocation -> {
      fake.earlyThrottleEnabled = invocation.getArgument(0, Boolean.class);
      return null;
    }).when(config).setEarlyThrottleEnabled(anyBoolean());

    doAnswer(invocation -> fake.earlyThrottleEnabled).when(config).isEarlyThrottleEnabled();

    accessController = null;
  }

  private void setupAccessController(boolean accessControllerPresent) throws AclException {
    if (accessControllerPresent) {
      accessController = mock(AccessController.class);
      doReturn(true).when(accessController).hasAccessToAdminOperation(any(), any());
    }
  }

  private void setInitialConfig(boolean initialReadThrottlingEnabled, boolean initialEarlyThrottleEnabled) {
    config.setReadThrottlingEnabled(initialReadThrottlingEnabled);
    config.setEarlyThrottleEnabled(initialEarlyThrottleEnabled);
  }

  private FullHttpResponse passRequestToAdminOperationsHandler(HttpMethod method, String requestUri, boolean isSSL)
      throws IOException {
    Channel channel = mock(Channel.class);
    doReturn(new InetSocketAddress(1500)).when(channel).remoteAddress();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    doReturn(channel).when(ctx).channel();
    ChannelPipeline pipe = mock(ChannelPipeline.class);
    when(ctx.pipeline()).thenReturn(pipe);

    if (isSSL) {
      // Certificate
      SslHandler sslHandler = mock(SslHandler.class);
      when(pipe.get(SslHandler.class)).thenReturn(sslHandler);
      SSLEngine sslEngine = mock(SSLEngine.class);
      when(sslHandler.engine()).thenReturn(sslEngine);
      SSLSession sslSession = mock(SSLSession.class);
      when(sslEngine.getSession()).thenReturn(sslSession);
      X509Certificate cert = mock(X509Certificate.class);
      when(sslSession.getPeerCertificates()).thenReturn(new Certificate[]{cert});
    }

    FullHttpRequest httpRequest = mock(FullHttpRequest.class);
    Mockito.doReturn(EmptyHttpHeaders.INSTANCE).when(httpRequest).headers();
    Mockito.doReturn(requestUri).when(httpRequest).uri();
    Mockito.doReturn(method).when(httpRequest).method();

    adminOperationsHandler.channelRead0(ctx, httpRequest);
    ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(ctx).writeAndFlush(captor.capture());

    FullHttpResponse response = (FullHttpResponse) captor.getValue();
    return response;
  }

  private void verifyReadThrottlingStatus(FullHttpResponse response, boolean isSSL, boolean accessControllerPresent, boolean readThrottlingEnabled, boolean earlyThrottleEnabled)
      throws IOException, AclException {
    Map responseContent = objectMapper.readValue(response.content().toString(StandardCharsets.UTF_8), Map.class);
    if (accessControllerPresent && isSSL) {
      verify(accessController, times(1)).hasAccessToAdminOperation(any(), eq(TASK_READ_QUOTA_THROTTLE));
      Mockito.clearInvocations(accessController);
    }

    if (!accessControllerPresent || isSSL) {
      Assert.assertEquals(response.status(), HttpResponseStatus.OK);
      Assert.assertEquals(responseContent.get(READ_THROTTLING_ENABLED), String.valueOf(readThrottlingEnabled));
      Assert.assertEquals(responseContent.get(EARLY_THROTTLE_ENABLED), String.valueOf(earlyThrottleEnabled));
    } else {
      Assert.assertEquals(response.status(), HttpResponseStatus.FORBIDDEN);
      Assert.assertNotNull(responseContent.get("error"));
    }
  }

  private void verifyErrorResponse(FullHttpResponse response, boolean isSSL, boolean accessControllerPresent, HttpResponseStatus expectedResponseStatus)
      throws IOException, AclException {
    Map responseContent = objectMapper.readValue(response.content().toString(StandardCharsets.UTF_8), Map.class);
    if (accessControllerPresent && isSSL) {
      verify(accessController, times(1)).hasAccessToAdminOperation(any(), anyString());
      Mockito.clearInvocations(accessController);
    }

    if (!accessControllerPresent || isSSL) {
      Assert.assertEquals(response.status(), expectedResponseStatus);
    } else {
      Assert.assertEquals(response.status(), HttpResponseStatus.FORBIDDEN);
    }
    Assert.assertNotNull(responseContent.get("error"));
  }

  @Test(dataProvider = "FourBoolean")
  public void testRouterReadQuotaThrottleControl(boolean initialReadThrottlingEnabled, boolean initialEarlyThrottleEnabled, boolean isSSL, boolean accessControllerPresent)
      throws IOException, AclException {
    setInitialConfig(initialReadThrottlingEnabled, initialEarlyThrottleEnabled);
    setupAccessController(accessControllerPresent);
    adminOperationsHandler = new AdminOperationsHandler(accessController, router, stats);

    FullHttpResponse initialResponse = passRequestToAdminOperationsHandler(HttpMethod.GET, READ_QUOTA_THROTTLE_URI, isSSL);
    verifyReadThrottlingStatus(initialResponse, isSSL, accessControllerPresent, initialReadThrottlingEnabled, initialEarlyThrottleEnabled);

    FullHttpResponse disableThrottleResponse = passRequestToAdminOperationsHandler(HttpMethod.POST, READ_QUOTA_THROTTLE_DISABLE_URI, isSSL);
    verifyReadThrottlingStatus(disableThrottleResponse, isSSL, accessControllerPresent, false, false);

    FullHttpResponse enableThrottleResponse = passRequestToAdminOperationsHandler(HttpMethod.POST, READ_QUOTA_THROTTLE_ENABLE_URI, isSSL);
    verifyReadThrottlingStatus(enableThrottleResponse, isSSL, accessControllerPresent, initialReadThrottlingEnabled, initialEarlyThrottleEnabled);

    FullHttpResponse disableThrottleResponseLease = passRequestToAdminOperationsHandler(HttpMethod.POST, READ_QUOTA_THROTTLE_DISABLE_URI, isSSL);
    verifyReadThrottlingStatus(disableThrottleResponseLease, isSSL, accessControllerPresent, false, false);

    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, false, () -> {
      FullHttpResponse response = passRequestToAdminOperationsHandler(HttpMethod.GET, READ_QUOTA_THROTTLE_URI, isSSL);
      verifyReadThrottlingStatus(response, isSSL, accessControllerPresent, initialReadThrottlingEnabled, initialEarlyThrottleEnabled);
    });
  }

  @Test(dataProvider = "TwoBoolean")
  public void testIncorrectAdminOperations(boolean isSSL, boolean accessControllerPresent)
      throws IOException, AclException {
    setupAccessController(accessControllerPresent);
    adminOperationsHandler = new AdminOperationsHandler(accessController, router, stats);

    FullHttpResponse incorrectAdminTaskResponse = passRequestToAdminOperationsHandler(HttpMethod.GET, INCORRECT_ADMIN_TASK, isSSL);
    verifyErrorResponse(incorrectAdminTaskResponse, isSSL, accessControllerPresent, HttpResponseStatus.NOT_IMPLEMENTED);

    FullHttpResponse readQuotaThrottleIncorrectActionResponse = passRequestToAdminOperationsHandler(HttpMethod.GET, READ_QUOTA_THROTTLE_INCORRECT_ACTION_URI, isSSL);
    verifyErrorResponse(readQuotaThrottleIncorrectActionResponse, isSSL, accessControllerPresent, HttpResponseStatus.BAD_REQUEST);
  }
}
