package com.linkedin.venice.exceptions;

public class VeniceHttpException extends VeniceException {
  private int code;
  public VeniceHttpException(int code){
    super();
    this.code = code;
  }

  public VeniceHttpException(int code, String message){
    super(message);
    this.code = code;
  }

  public VeniceHttpException(int code, Throwable cause){
    super(cause);
    this.code = code;
  }

  public VeniceHttpException(int code, String message, Throwable cause){
    super(message, cause);
    this.code = code;
  }

  @Override
  public int getHttpStatusCode(){
    return code;
  }
}
