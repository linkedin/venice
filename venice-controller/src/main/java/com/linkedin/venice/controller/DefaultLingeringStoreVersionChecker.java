package com.linkedin.venice.controller;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Time;
import java.util.concurrent.TimeUnit;


class DefaultLingeringStoreVersionChecker implements LingeringStoreVersionChecker {

    DefaultLingeringStoreVersionChecker() { }

    @Override
    public boolean isStoreVersionLingering(Store store, Version version, Time time, Admin controllerAdmin) {
        final long bootstrapDeadlineMs = version.getCreatedTime() + TimeUnit.HOURS.toMillis(store.getBootstrapToOnlineTimeoutInHours());
        return time.getMilliseconds() > bootstrapDeadlineMs;
    }
}
