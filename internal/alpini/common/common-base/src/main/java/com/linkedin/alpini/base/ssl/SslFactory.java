package com.linkedin.alpini.base.ssl;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;


/**
 * SSL Factory interface to get common SSL objects.
 * All SSL factory implementations should implement this interface.
 */
public interface SslFactory {
  /**
   * @see SSLContext
   * @return an {@link SSLContext} object
   */
  SSLContext getSSLContext();

  /**
   * @see SSLParameters
   * @return an {@link SSLParameters} object
   */
  SSLParameters getSSLParameters();

  /**
   * @return Whether the implementation enforces SSL
   */
  boolean isSslEnabled();
}
