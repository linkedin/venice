package com.linkedin.venice.d2;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.servers.ZooKeeperConnectionManager;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Runs a daemon thread that consistently sends HTTP GET request to
 * admin healthcheck endpoint. If there are consecutive N bad status,
 * shuts down zookeeper announcer and waits until it receives N
 * consecutive good status to restart ZooKeeperAnnouncer
 *
 * @author Oby Sumampouw (osumampouw@linkedin.com)
 * @author Xialin Zhu
 */
public class D2HealthChecker {
  private static final Logger LOGGER = LogManager.getLogger(D2HealthChecker.class);
  private final ScheduledExecutorService _scheduledExecutorService;
  private final long _intervalMs;
  private final ZooKeeperConnectionManager _zkManager;
  private final String _healthCheckUrl;
  private final int _minConsecutiveCount;
  private volatile boolean _isBadState = false;
  private volatile boolean _isShutDown = false;
  private final HttpClient _httpClient;
  private final HttpGet _request;

  public D2HealthChecker(
      ZooKeeperConnectionManager zkManager,
      long intervalMs,
      String healthCheckUrl,
      int minConsecutiveCount,
      ScheduledExecutorService executorService,
      int httpTimeoutMs) {
    _scheduledExecutorService = executorService;
    _intervalMs = intervalMs;
    _zkManager = zkManager;
    _healthCheckUrl = healthCheckUrl;
    _minConsecutiveCount = minConsecutiveCount;

    RequestConfig requestConfig =
        RequestConfig.custom().setSocketTimeout(httpTimeoutMs).setConnectionRequestTimeout(httpTimeoutMs).build();
    _httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();
    _request = new HttpGet(_healthCheckUrl);
  }

  public void shutdown() {
    if (!_isShutDown) {
      _scheduledExecutorService.shutdown();
    } else {
      LOGGER.warn("Received a call to shutdown but shut down is already in process");
    }
  }

  public void start() throws Exception {
    if (_healthCheckUrl != null && !_healthCheckUrl.trim().isEmpty()) {
      _scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        private final AtomicInteger _consecutiveCount = new AtomicInteger(0);

        @Override
        public void run() {
          HttpResponse response = null;
          try {
            response = _httpClient.execute(_request);
          } catch (Exception e) {
            // pretty much any kind of exception is bad
            LOGGER.error("Problem connecting to D2 health check endpoint", e);
          }
          try {
            boolean isResponseGood = isResponseGood(response);
            if (_isBadState) {
              processBadState(isResponseGood);
            } else {
              processGoodState(isResponseGood);
            }
          } catch (IOException e) {
            LOGGER.error("Received an error while checking health of " + _healthCheckUrl, e);
          }

          if (response != null) {
            EntityUtils.consumeQuietly(response.getEntity());
          }
        }

        private boolean isResponseGood(HttpResponse response) {
          if (response == null) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("HttpResponse is null");
            }
            return false;
          }
          int responseCode = response.getStatusLine().getStatusCode();
          if (responseCode != HttpStatus.SC_OK) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Received statusCode = " + responseCode);
            }
            return false;
          }

          String responseEntity;
          try {
            responseEntity = EntityUtils.toString(response.getEntity()).trim();
          } catch (IOException e) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Received an error while reading the response entity");
            }
            return false;
          }
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Received response ==" + response);
          }
          if ("GOOD".equals(responseEntity)) {
            return true;
          }
          return false;
        }

        private void becomeBadState() {
          _zkManager.markDownAllServers(new Callback<None>() {
            @Override
            public void onError(Throwable e) {
              LOGGER.error("Failed marking down _zkManager. Retrying markDown() on the next heart beat.", e);
            }

            @Override
            public void onSuccess(None result) {
              // set to bad state here because it's ok to be sending multiple markdown signals in case callback is slow
              _isBadState = true;
              LOGGER.info("BecomeBadState() successful");
            }
          });
        }

        private void recoverToGoodState() {
          _zkManager.markUpAllServers(new Callback<None>() {
            @Override
            public void onError(Throwable e) {
              LOGGER.error("Failed marking up _zkManager. Retrying markUp() on the next heart beat.", e);
            }

            @Override
            public void onSuccess(None result) {
              // set to good state here because it's ok to be sending multiple markup signals in case callback is slow
              _isBadState = false;
              LOGGER.info("RecoverToGoodState() successful");
            }
          });
        }

        private void processBadState(boolean isResponseGood) throws IOException {

          if (isResponseGood) {
            if (incrementAndGet() >= _minConsecutiveCount) {
              recoverToGoodState();
            }
          } else {
            resetCount();
          }
        }

        private void processGoodState(boolean isResponseGood) throws IOException {
          if (!isResponseGood) {
            if (incrementAndGet() >= _minConsecutiveCount) {
              becomeBadState();
            }
          } else {
            resetCount();
          }
        }

        private int incrementAndGet() {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Health check isBad =" + isBadState() + ". Signal against current status received. ConsecutiveCount = "
                    + _consecutiveCount + " out of " + _minConsecutiveCount + " minConsecutiveCount");
          }
          return _consecutiveCount.incrementAndGet();
        }

        private void resetCount() {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Health check isBad =" + isBadState()
                    + ". Signal consistent with current status received. Resetting consecutiveCount. MinConsecutiveCount = "
                    + _minConsecutiveCount);
          }
          _consecutiveCount.set(0);
        }
      }, 0, _intervalMs, TimeUnit.MILLISECONDS);
    } else {
      LOGGER.warn("Health check url is null or empty so we are not using health check for D2 purposes");
    }
  }

  boolean isShutDown() {
    return _isShutDown;
  }

  String getHealthCheckUrl() {
    return _healthCheckUrl;
  }

  int getMinConsecutiveCount() {
    return _minConsecutiveCount;
  }

  long getIntervalMs() {
    return _intervalMs;
  }

  public boolean isBadState() {
    return _isBadState;
  }

  // only used for testing
  void setBadState(boolean isBadState) {
    _isBadState = isBadState;
  }
}
