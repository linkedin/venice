package com.linkedin.venice.client;

/***
 * Thrown by the venice thin client if the server responds with a 404
 */
public class VeniceNotFoundException extends VeniceClientException {

  public VeniceNotFoundException(){
    super();
  }

  public VeniceNotFoundException(String msg){
    super(msg);
  }

}
