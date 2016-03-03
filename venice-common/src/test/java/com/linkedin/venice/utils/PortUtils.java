package com.linkedin.venice.utils;

/**
 * Utility class to get a free port.
 */
public class PortUtils {
    private static int lastPort = 50000;
    /**
     * TODO: Make this smarter.
     * @return a free port to be used by tests.
     */
    public static int getFreePort() {
        return lastPort++;
    }
}
