package com.linkedin.venice.controller;

import com.linkedin.venice.config.VeniceStorePartitionInformation;


/**
 * Created by athirupa on 2/1/16.
 */
public interface Admin {
    public void start(String clusterName);

    public void addStore(String clusterName, VeniceStorePartitionInformation info );

    public void stop(String clusterName);
}
