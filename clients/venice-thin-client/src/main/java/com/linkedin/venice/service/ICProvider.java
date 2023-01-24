package com.linkedin.venice.service;

import java.util.concurrent.Callable;


/**
 * An interface for implementation of IC( invocation-context) provider class for calls between various deployable services.
 */
public interface ICProvider {
  <T> T call(String traceContext, Callable<T> callable) throws Exception;
}
