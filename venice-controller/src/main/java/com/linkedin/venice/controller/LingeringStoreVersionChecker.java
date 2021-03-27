package com.linkedin.venice.controller;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Time;

public interface LingeringStoreVersionChecker {

    /**
     * Check if a version has been lingering around
     * @param store
     * @param version
     * @return true if the provided version is has been lingering (so that it can be killed potentially)
     */
    boolean isStoreVersionLingering(Store store, Version version, Time time, Admin controllerAdmin);
}
