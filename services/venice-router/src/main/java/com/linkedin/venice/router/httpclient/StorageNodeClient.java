package com.linkedin.venice.router.httpclient;

import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.router.api.path.VenicePath;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;


public interface StorageNodeClient extends Closeable {
  /**
   * Start the client and prepare the required resources.
   */
  void start();

  /**
   * Close the client and release all resources held by the client
   */
  void close();

  /**
   * Send a request to storage node
   *
   * @param host              The target host to which the request will be sent
   * @param path              contains information like URI, request content, HTTP method, etc.
   * @param completedCallBack Callback function for a complete response
   * @param failedCallBack    if any exception thrown in the channel
   * @param cancelledCallBack for requests that are cancelled by the channel
   * @throws RouterException
   */
  void query(
      Instance host,
      VenicePath path,
      Consumer<PortableHttpResponse> completedCallBack,
      Consumer<Throwable> failedCallBack,
      BooleanSupplier cancelledCallBack) throws RouterException;

  default boolean isInstanceReadyToServe(String instanceId) {
    return true;
  }

  default void sendRequest(VeniceMetaDataRequest request, CompletableFuture<PortableHttpResponse> responseConsumer) {

  }
}
