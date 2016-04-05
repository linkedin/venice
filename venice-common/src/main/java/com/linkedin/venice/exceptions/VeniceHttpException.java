package com.linkedin.venice.exceptions;

import com.linkedin.venice.exceptions.VeniceException;

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

  public int getHttpStatusCode(){
    return code;
  }
}
