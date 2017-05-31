package com.linkedin.venice.client.exceptions;

/***
 * {@link VeniceClientException} with a http code
 */
public class VeniceClientHttpException extends VeniceClientException {
  private int httpStatus;

  public VeniceClientHttpException(String msg, int httpStatus) {
    super(msg);
    this.httpStatus = httpStatus;
  }

  public VeniceClientHttpException(int httpStatus){
    super();
    this.httpStatus = httpStatus;
  }

  public int getHttpStatus() {
    return this.httpStatus;
  }
}
