package com.linkedin.venice.controllerapi.transport;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.ACCESS_PERMISSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLUSTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.INCLUDE_SYSTEM_STORES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.IS_SYSTEM_STORE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.KEY_SCHEMA;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_JOB_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_CONFIG_NAME_FILTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_CONFIG_VALUE_FILTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VALUE_SCHEMA;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.controllerapi.ControllerTransport;
import com.linkedin.venice.controllerapi.LeaderControllerResponse;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.QueryParams;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.request.DiscoverLeaderControllerRequest;
import com.linkedin.venice.controllerapi.request.EmptyPushRequest;
import com.linkedin.venice.controllerapi.request.GetStoreRequest;
import com.linkedin.venice.controllerapi.request.ListStoresRequest;
import com.linkedin.venice.controllerapi.request.NewStoreRequest;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.http.HttpStatus;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ControllerHttpTransport implements ControllerTransportAdapter {
  private static final Logger LOGGER = LogManager.getLogger(ControllerHttpTransport.class);

  private final ControllerTransportAdapterConfigs transportAdapterConfigs;

  private final SSLFactory sslFactory;
  private String leaderControllerUrl;
  private int requestTimeoutMs;
  private int maxAttempts;
  private int queryJobStatusTimeout;
  private final List<String> controllerDiscoveryUrls;

  private String clusterName;

  public ControllerHttpTransport(ControllerTransportAdapterConfigs transportAdapterConfigs) {
    this.transportAdapterConfigs = transportAdapterConfigs;
    this.controllerDiscoveryUrls = transportAdapterConfigs.getControllerDiscoveryUrls();
    this.sslFactory = transportAdapterConfigs.getSslFactory();
    this.leaderControllerUrl = null;

    this.requestTimeoutMs = transportAdapterConfigs.getRequestTimeout();
    this.maxAttempts = transportAdapterConfigs.getMaxAttempts();
    this.queryJobStatusTimeout = transportAdapterConfigs.getQueryJobStatusTimeout();

    this.clusterName = transportAdapterConfigs.getClusterName();
  }

  @Override
  public LeaderControllerResponse discoverLeaderController(
      DiscoverLeaderControllerRequest discoverLeaderControllerRequest) {
    List<String> urls = new ArrayList<>(this.controllerDiscoveryUrls);
    Collections.shuffle(urls);

    Exception lastConnectException = null;
    Exception lastException = null;
    try (ControllerTransport transport = new ControllerTransport(Optional.ofNullable(sslFactory))) {
      for (String url: urls) {
        try {
          QueryParams params = newParams().add(CLUSTER, discoverLeaderControllerRequest.getClusterName());
          LeaderControllerResponse leaderControllerResponse =
              transport.request(url, ControllerRoute.LEADER_CONTROLLER, params, LeaderControllerResponse.class);
          String leaderControllerUrl = leaderControllerResponse.getUrl();
          LOGGER.info("Discovered leader controller {} from {}", leaderControllerUrl, url);
          return leaderControllerResponse;
        } catch (Exception e) {
          LOGGER.warn("Unable to discover leader controller from {}", url);
          if (ExceptionUtils.recursiveClassEquals(e, ConnectException.class)) {
            lastConnectException = e;
          } else {
            lastException = e;
          }
        }
      }
    }

    /**
     * During normal operation, some hosts might be down for maintenance. Over time, the host list might even get
     * stale. So, when requests are made to hosts which no longer host venice controllers or routers,
     * {@link ConnectException} will be thrown. When the request actually leads to an error, all active hosts in
     * the host list will throw this error and the inactive hosts will throw the {@link ConnectException}. In such
     * cases, the {@link ConnectException} is not actionable by the user. If after trying all controllers and routers,
     * we still don't have any other non-{@link ConnectException}, the {@link ConnectException} is the most actionable
     * exception, and we throw that.
     */
    if (lastException == null) {
      lastException = lastConnectException;
    }

    String message = "Unable to discover leader controller from " + this.controllerDiscoveryUrls;
    LOGGER.error(message, lastException);
    throw new VeniceException(message, lastException);
  }

  private String getLeaderControllerUrl(String clusterName) {
    leaderControllerUrl = discoverLeaderController(new DiscoverLeaderControllerRequest(clusterName)).getUrl();
    return leaderControllerUrl;
  }

  @Override
  public NewStoreResponse createNewStore(NewStoreRequest newStoreRequest) {
    QueryParams params = newParams().add(NAME, newStoreRequest.getStoreName())
        .add(OWNER, newStoreRequest.getOwner())
        .add(KEY_SCHEMA, newStoreRequest.getKeySchema())
        .add(VALUE_SCHEMA, newStoreRequest.getValueSchema());
    if (newStoreRequest.isSystemStore()) {
      params.add(IS_SYSTEM_STORE, true);
    }
    if (newStoreRequest.getAccessPermissions() != null) {
      params.add(ACCESS_PERMISSION, newStoreRequest.getAccessPermissions());
    }
    return request(ControllerRoute.NEW_STORE, params, NewStoreResponse.class);
  }

  @Override
  public StoreResponse getStore(GetStoreRequest request) {
    QueryParams params = newParams().add(NAME, request.getStoreName()).add(CLUSTER, request.getClusterName());
    return request(ControllerRoute.STORE, params, StoreResponse.class);
  }

  @Override
  public VersionCreationResponse emptyPush(EmptyPushRequest request) {
    QueryParams params = newParams().add(NAME, request.getStoreName()).add(PUSH_JOB_ID, request.getPushJobId());
    return request(ControllerRoute.EMPTY_PUSH, params, VersionCreationResponse.class);
  }

  @Override
  public MultiStoreResponse listStores(ListStoresRequest request) {
    QueryParams queryParams = newParams().add(INCLUDE_SYSTEM_STORES, !request.isExcludeSystemStores());
    if (request.getStoreConfigNameFilter() != null) {
      queryParams.add(STORE_CONFIG_NAME_FILTER, request.getStoreConfigNameFilter());
    }
    if (request.getStoreConfigValueFilter() != null) {
      queryParams.add(STORE_CONFIG_VALUE_FILTER, request.getStoreConfigValueFilter());
    }
    return request(ControllerRoute.LIST_STORES, queryParams, MultiStoreResponse.class);
  }

  private static String encodeQueryParams(QueryParams params) {
    return URLEncodedUtils.format(params.getNameValuePairs(), StandardCharsets.UTF_8);
  }

  private <T extends ControllerResponse> T request(ControllerRoute route, QueryParams params, Class<T> responseType) {
    return request(route, params, responseType, requestTimeoutMs, maxAttempts, null);
  }

  private <T extends ControllerResponse> T request(
      ControllerRoute route,
      QueryParams params,
      Class<T> responseType,
      byte[] data) {
    return request(route, params, responseType, requestTimeoutMs, maxAttempts, data);
  }

  private <T extends ControllerResponse> T request(
      ControllerRoute route,
      QueryParams params,
      Class<T> responseType,
      int timeoutMs,
      int maxAttempts,
      byte[] data) {
    Exception lastException = null;
    boolean logErrorMessage = true;
    try (ControllerTransport transport = new ControllerTransport(Optional.ofNullable(sslFactory))) {
      for (int attempt = 1; attempt <= maxAttempts; ++attempt) {
        try {
          String leaderControllerUrl = getLeaderControllerUrl(params.getString(CLUSTER).get());
          return transport.request(leaderControllerUrl, route, params, responseType, timeoutMs, data);
        } catch (ExecutionException | TimeoutException e) {
          // Controller is unreachable. Let's wait for a new leader to be elected.
          // Total wait time should be at least leader election time (~30 seconds)
          lastException = e;
        } catch (VeniceHttpException e) {
          if (e.getHttpStatusCode() == HttpStatus.SC_PRECONDITION_FAILED) {
            logErrorMessage = false;
            throw e;
          }
          if (e.getHttpStatusCode() != HttpConstants.SC_MISDIRECTED_REQUEST) {
            throw e;
          }
          // leader controller has changed. Let's wait for a new leader to realize it.
          lastException = e;
        } catch (Exception e) {
          lastException = e;
        }

        if (attempt < maxAttempts) {
          LOGGER.info(
              "Retrying controller request, attempt = {}/{}, controller = {}, route = {}, params = {}, timeout = {}",
              attempt,
              maxAttempts,
              this.leaderControllerUrl,
              route.getPath(),
              params.getNameValuePairs(),
              timeoutMs,
              lastException);
          Utils.sleep(5 * Time.MS_PER_SECOND);
        }
      }
    } catch (Exception e) {
      lastException = e;
    }

    String message =
        "An error occurred during controller request." + " controller = " + this.leaderControllerUrl + ", route = "
            + route.getPath() + ", params = " + params.getAbbreviatedNameValuePairs() + ", timeout = " + timeoutMs;
    return makeErrorResponse(message, lastException, responseType, logErrorMessage);
  }

  private static <T extends ControllerResponse> T makeErrorResponse(
      String message,
      Exception exception,
      Class<T> responseType) {
    return makeErrorResponse(message, exception, responseType, true);
  }

  private static <T extends ControllerResponse> T makeErrorResponse(
      String message,
      Exception exception,
      Class<T> responseType,
      boolean logErrorMessage) {
    if (logErrorMessage) {
      LOGGER.error(message, exception);
    }
    try {
      T response = responseType.newInstance();
      response.setError(message, exception);
      return response;
    } catch (InstantiationException | IllegalAccessException e) {
      LOGGER.error("Unable to instantiate controller response {}", responseType.getName(), e);
      throw new VeniceException(message, exception);
    }
  }

  /***
   * Add all global parameters in this method. Always use a form of this method to generate
   * a new list of NameValuePair objects for making HTTP requests.
   * @return
   */
  private QueryParams newParams() {
    return addCommonParams(new QueryParams());
  }

  private QueryParams addCommonParams(QueryParams params) {
    return params.add(CLUSTER, this.clusterName);
  }
}
