package com.linkedin.venice.httpclient5;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.async.MinimalHttpAsyncClient;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.config.Http1Config;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.http.ssl.TLS;
import org.apache.hc.core5.http2.HttpVersionPolicy;
import org.apache.hc.core5.http2.config.H2Config;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.Timeout;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class HttpClient5Test {
  private static final Logger LOGGER = LogManager.getLogger(HttpClient5Test.class);
  /**
   * This is not very safe since the port can be grabbed by some parallel tests.
   * Will tune this logic if this test become very flaky.
   */
  private final int port = Utils.getFreePort();

  private void sendRequest(CloseableHttpAsyncClient httpClient, int iteration, boolean failOnTimeout) {
    LOGGER.info("Iteration: {}", iteration);
    String url = "https://localhost:" + port;
    SimpleRequestBuilder simpleRequestBuilder = SimpleRequestBuilder.create(Method.GET).setUri(url);
    Future<SimpleHttpResponse> responseFuture =
        httpClient.execute(simpleRequestBuilder.build(), new FutureCallback<SimpleHttpResponse>() {
          @Override
          public void completed(SimpleHttpResponse result) {
            byte[] body = result.getBodyBytes();
            LOGGER.info("received response: {}", new String(body));
          }

          @Override
          public void failed(Exception ex) {
            LOGGER.error("Failed to send request", ex);
          }

          @Override
          public void cancelled() {
            LOGGER.error("Request got cancelled");
          }
        });
    try {
      responseFuture.get(3, TimeUnit.SECONDS);
    } catch (TimeoutException te) {
      if (failOnTimeout) {
        Assert.fail("Request timed out");
      } else {
        LOGGER.error("Request timed out");
      }
    } catch (Exception e) {
      LOGGER.error("Received other types of exception: {}", e.getCause().getClass());
    }
  }

  private static String getTempFilePath() {
    try {
      File file = File.createTempFile("httpclient5_test_temp", null);
      file.deleteOnExit();
      return file.getAbsolutePath();
    } catch (IOException e) {
      throw new VeniceException("Failed to create temp file", e);
    }
  }

  private ForkedJavaProcess spinupServerProcess() throws IOException {
    final String tempFilePathToNotifyServerStartedFully = getTempFilePath();
    ForkedJavaProcess serverProcess = ForkedJavaProcess.exec(
        NettyH2Server.class,
        Arrays.asList(Integer.toString(port), tempFilePathToNotifyServerStartedFully),
        Collections.emptyList(),
        ForkedJavaProcess.getClasspath(),
        true,
        Optional.empty());
    LOGGER.info("Server process id: {}", serverProcess.pid());
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      if (!serverProcess.isAlive()) {
        Assert.fail("Process with pid: " + serverProcess.pid() + " is still not alive yet");
      } else {
        // Check temp file to see whether the server is fully started or not.
        File file = new File(tempFilePathToNotifyServerStartedFully);
        Assert.assertTrue(file.length() > 0, "Server is not fully started");
      }
    });

    return serverProcess;
  }

  private void forceKillProcess(long pid) throws IOException, InterruptedException {
    Runtime rt = Runtime.getRuntime();
    // A concatenated string allows the system to execute multiple commands, like "kill -9 xxx&; rm -rf /"
    // An array of string will treat "pid" as one argument in one command
    Process pr = rt.exec(new String[] { "kill -9 ", String.valueOf(pid) });
    pr.waitFor();
  }

  private void testPeerCrashAndRecovery(CloseableHttpAsyncClient httpClient) throws Exception {
    ForkedJavaProcess serverProcess = spinupServerProcess();
    int iterationToStopProcess = 145;
    int iterationToRestartProcess = 150;
    try {
      for (int i = 0; i < 200; ++i) {
        // Manually kill -9 process before iteration 150
        if (i == iterationToStopProcess) {
          forceKillProcess(serverProcess.pid());
          LOGGER.info("Killed process: {}", serverProcess.pid());
        }
        if (i == iterationToRestartProcess) {
          serverProcess = spinupServerProcess();
        }
        boolean failOnTimeout = true;
        if (i >= iterationToStopProcess && i < iterationToRestartProcess) {
          failOnTimeout = false;
        }
        sendRequest(httpClient, i, failOnTimeout);
      }
    } finally {
      if (serverProcess != null) {
        serverProcess.destroy();
      }
    }
  }

  @Test
  public void testWithH2SpecificAPI() throws Exception {
    SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();
    CloseableHttpAsyncClient httpClient = HttpAsyncClients.customHttp2()
        .setTlsStrategy(
            VeniceClientTlsStrategyBuilder.create()
                .setSslContext(sslFactory.getSSLContext())
                .setTlsVersions(TLS.V_1_3, TLS.V_1_2)
                .build())
        .setIOReactorConfig(IOReactorConfig.custom().setSoTimeout(Timeout.ofSeconds(1)).build())
        .setDefaultRequestConfig(
            RequestConfig.custom()
                .setConnectTimeout(Timeout.ofSeconds(1))
                .setResponseTimeout(Timeout.ofSeconds(1))
                .build())
        .build();
    try {
      httpClient.start();
      testPeerCrashAndRecovery(httpClient);
    } finally {
      httpClient.close(CloseMode.GRACEFUL);
    }
  }

  @Test
  public void testWithH2CompatibleAPI() throws Exception {
    SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();
    final IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
        .setSoKeepAlive(true)
        .setTcpNoDelay(true)
        .setSoTimeout(Timeout.ofSeconds(1))
        .setIoThreadCount(2)
        .build();
    final TlsStrategy tlsStrategy = VeniceClientTlsStrategyBuilder.create()
        .setSslContext(sslFactory.getSSLContext())
        .setTlsVersions(TLS.V_1_3, TLS.V_1_2)
        .build();
    final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
        .setTlsStrategy(tlsStrategy)
        .setMaxConnTotal(1)
        .setMaxConnPerRoute(1)
        .build();
    final MinimalHttpAsyncClient client = HttpAsyncClients.createMinimal(
        HttpVersionPolicy.FORCE_HTTP_2,
        H2Config.DEFAULT,
        Http1Config.DEFAULT,
        ioReactorConfig,
        connectionManager);
    try {
      client.start();
      testPeerCrashAndRecovery(client);
    } finally {
      if (client != null) {
        client.close(CloseMode.GRACEFUL);
      }
    }
  }
}
