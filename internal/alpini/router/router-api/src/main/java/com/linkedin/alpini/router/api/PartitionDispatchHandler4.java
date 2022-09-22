package com.linkedin.alpini.router.api;

import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
@NettyVersion(Netty.NETTY_4_1)
public interface PartitionDispatchHandler4<H, P extends ResourcePath<K>, K>
    extends PartitionDispatchHandler<H, P, K, BasicHttpRequest, FullHttpResponse, HttpResponseStatus> {
  static <H, P extends ResourcePath<K>, K> PartitionDispatchHandler4<H, P, K> wrap(
      PartitionDispatchHandler4<H, P, K> dispatchHandler) {
    return dispatchHandler;
  }
}
