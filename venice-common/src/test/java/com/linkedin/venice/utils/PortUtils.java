package com.linkedin.venice.utils;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * Utility class to get a free port.
 */
public class PortUtils {
    private static AtomicInteger lastPort = new AtomicInteger(50000);
    /**
     * TODO: Make this smarter.
     * @return a free port to be used by tests.
     */
    public static int getFreePort() {
        return lastPort.getAndIncrement();
    }
}
