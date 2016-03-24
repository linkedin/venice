package com.linkedin.venice.client;

/***
 * Thrown by the venice thin client if something goes wrong with the request
 * Parent class to VeniceNotFoundException and VeniceServerErrorException
 */
public class VeniceClientException extends Exception{

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
