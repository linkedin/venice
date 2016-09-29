package com.linkedin.venice.client.exceptions;

/***
 * Thrown by the venice thin client if something goes wrong with the request
 * Parent class to VeniceServerException
 */
public class VeniceClientException extends RuntimeException {

  public VeniceClientException(Throwable e){
    super(e);
  }

  public VeniceClientException(String msg){
    super(msg);
  }

  public VeniceClientException(){
    super();
  }

}
