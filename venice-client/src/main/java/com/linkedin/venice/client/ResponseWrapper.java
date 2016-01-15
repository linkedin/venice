package com.linkedin.venice.client;

/**
 * Wrapper around a byte array so this object can be passed as a holder for the byte array
 */
public class ResponseWrapper {
  byte[] response;
  public ResponseWrapper(){
    this.response = null;
  }

  public synchronized void setResponse(byte[] response){
    this.response = response;
  }

  public synchronized byte[] getResponse(){
    return response;
  }

  public synchronized boolean isReady(){
    return response != null;
  }
}
