package com.linkedin.venice.router.httpclient;

import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.security.ssl.access.control.SSLEngineComponentFactory;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.router.api.path.VenicePath;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;


public class R2StorageNodeClient implements StorageNodeClient {
  private final Client r2Client;
  private final Optional<SSLEngineComponentFactory> sslFactory;

  public R2StorageNodeClient(Client r2Client, Optional<SSLEngineComponentFactory> sslFactory) {
    this.r2Client = r2Client;
    this.sslFactory = sslFactory;
  }

  @Override
  public void query(
      Instance host,
      VenicePath path,
      Consumer<PortableHttpResponse> completedCallBack,
      Consumer<Throwable> failedCallBack,
      BooleanSupplier cancelledCallBack,
      long queryStartTimeInNS) throws RouterException {

    RestRequest request = path.composeRestRequest(host.getHostUrl(sslFactory.isPresent()));

    r2Client.restRequest(request, new R2ClientCallback(completedCallBack, failedCallBack, cancelledCallBack));
  }

  @Override
  public void start() {
  }

  @Override
  public void close() {
    r2Client.shutdown(null);
  }
}
