package com.linkedin.venice.router.httpclient;

import io.netty.buffer.ByteBuf;
import java.io.IOException;


/**
 * This interface exposes the commonalities between the Apache httpasyncclient and Netty and R2 client.
 */
public interface PortableHttpResponse {
  /**
   *
   * @return Status code of the response, like 200, 400, 500 etc.
   */
  int getStatusCode();

  /**
   * Returns the response as ByteBuf.
   * @return
   * @throws IOException
   */
  ByteBuf getContentInByteBuf() throws IOException;

  /**
   *
   * @param headerName
   * @return whether response contains header name specified in the parameters
   */
  boolean containsHeader(String headerName);

  /**
   *
   * @param headerName
   * @return the value of the first header for header name specified in the parameters
   */
  String getFirstHeader(String headerName);
}
