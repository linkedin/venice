package com.linkedin.venice.client.exceptions;

/***
 * {@link VeniceClientException} with a http code
 */
public class VeniceClientHttpException extends VeniceClientException {
  private int httpStatus;

  private static String getMessageForHttpStatus(int httpStatus) {
    return "http status: " + httpStatus;
  }

  public VeniceClientHttpException(String msg, int httpStatus) {
    super(getMessageForHttpStatus(httpStatus) + (msg.isEmpty() ? "" : ", " + msg));
    this.httpStatus = httpStatus;
  }

  public VeniceClientHttpException(int httpStatus) {
    super(getMessageForHttpStatus(httpStatus));
    this.httpStatus = httpStatus;
  }

  public int getHttpStatus() {
    return this.httpStatus;
  }
}
