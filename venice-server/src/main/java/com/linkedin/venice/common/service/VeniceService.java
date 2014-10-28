package com.linkedin.venice.common.service;
/**
 * Blueprint for all Services initiated from Venice Server
 *
 */
public interface VeniceService {

    /**
     * @return The type of this service
     */
    public ServiceType getType();

    /**
     * Start the service.
     */
    public void start();

    /**
     * Stop the service
     */
    public void stop();

    /**
     * @return true iff the service is started
     */
    public boolean isStarted();

}
