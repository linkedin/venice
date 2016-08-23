package com.linkedin.venice.client.exceptions;

/***
 * Thrown by the venice thin client if the server responds with a 500
 */
public class VeniceServerException extends VeniceClientException {

  public VeniceServerException(String msg) {
    super(msg);
  }

  public VeniceServerException(){
    super();
  }

}
