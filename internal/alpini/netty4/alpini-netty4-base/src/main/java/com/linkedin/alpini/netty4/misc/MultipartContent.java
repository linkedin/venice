package com.linkedin.alpini.netty4.misc;

import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface MultipartContent extends HttpContent {
  /**
   * Returns the headers of this message.
   */
  HttpHeaders headers();

}
