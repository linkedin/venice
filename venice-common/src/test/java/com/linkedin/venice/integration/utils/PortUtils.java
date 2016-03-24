package com.linkedin.venice.integration.utils;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * Utility class to get a free port.
 *
 * TODO: Get rid of this. Replaced by {@link TestUtils}.
 */
@Deprecated
public class PortUtils {
    private static AtomicInteger lastPort = new AtomicInteger(50000);
    /**
     * TODO: Get rid of this. Replaced by {@link TestUtils#getFreePort()}.
     * @return a free port to be used by tests.
     */
    @Deprecated
    public static int getFreePort() {
        return lastPort.getAndIncrement();
    }
}
