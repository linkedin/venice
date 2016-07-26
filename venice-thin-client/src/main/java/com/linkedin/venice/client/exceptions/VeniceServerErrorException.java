package com.linkedin.venice.client.exceptions;

/***
 * Thrown by the venice thin client if the server responds with a 500
 */
public class VeniceServerErrorException extends VeniceClientException {

  public VeniceServerErrorException(String msg) {
    super(msg);
  }

  public VeniceServerErrorException(){
    super();
  }

}
