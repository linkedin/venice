package com.linkedin.venice.router.streaming;

public interface StreamingCallback<T> {
  /**
   * The callback method that the user can implement to do asynchronous handling of the response. T defines the type of
   * the result. When T=Void then there is no specific response expected but any errors are reported on the
   * exception object. For all other values of T, one of the two arguments would be null.
   * @param result The result of the request. This would be non null when the request executed successfully
   * @param exception The exception that was reported on execution of the request
   */
  void onCompletion(T result, Exception exception);
}
