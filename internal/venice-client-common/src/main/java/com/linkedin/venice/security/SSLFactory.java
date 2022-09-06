package com.linkedin.venice.security;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;


/**
 * Venice SSL Factory interface. All the SSL factory plug-in from Venice product in LinkedIn as well as
 * the local SSL factory used in test cases should implement this interface.
 */
public interface SSLFactory {
  /**
   * @return an instance of {@link SSLContext}
   */
  public SSLContext getSSLContext();

  /**
   * @return an instance of {@link SSLParameters}
   */
  public SSLParameters getSSLParameters();

  /**
   * @return whether SSL is enabled
   */
  public boolean isSslEnabled();
}
