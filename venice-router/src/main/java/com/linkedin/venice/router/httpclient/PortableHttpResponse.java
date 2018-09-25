package com.linkedin.venice.router.httpclient;

import io.netty.buffer.ByteBuf;
import java.io.IOException;


/**
 * This interface exposes the commonalities between the Apache httpasyncclient and Netty.
 */
public interface PortableHttpResponse {
  /**
   *
   * @return Status code of the response, like 200, 400, 500 etc.
   */
  int getStatusCode();

  /**
   * Notice that this function can be invoked for only one time; the netty client implementation of this function
   * will release the ByteBuf once the data are copied.
   * @return response content as byte array
   * @throws IOException
   */
  byte[] getContentInBytes() throws IOException;

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
