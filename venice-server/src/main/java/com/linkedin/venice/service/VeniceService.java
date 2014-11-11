package com.linkedin.venice.service;

/**
 * Blueprint for all Services initiated from Venice Server
 *
 */
public interface VeniceService {

  /**
   * @return The name of this service
   */
  public String getName();

  /**
   * Start the service.
   */
  public void start()
      throws Exception;

  /**
   * Stop the service
   */
  public void stop()
      throws Exception;

  /**
   * @return true iff the service is started
   */
  public boolean isStarted();
}
