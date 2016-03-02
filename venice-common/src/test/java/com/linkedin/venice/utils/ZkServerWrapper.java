package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * This class contains a ZK server, and provides facilities for cleaning up
 * its side effects when we're done using it.
 */
public class ZkServerWrapper implements Closeable {
    // CLASS-LEVEL STATE AND APIs

    private static final Logger LOGGER = Logger.getLogger(ZkServerWrapper.class);

    // TODO: Make sure the hardcoded values below make sense

    // ZK configs
    private static final int TICK_TIME = 2000;
    private static final int MAX_SESSION_TIMEOUT = 1000000;
    private static final int NUM_CONNECTIONS = 5000;

    // Test config
    private static final int MAX_ATTEMPT = 10;

    /**
     * This function attempts to find a free port and spins up a ZK server
     * instance which listens on that port. It also generates a randomly
     * named directory for the ZK state.
     * @return
     */
    public static ZkServerWrapper getZkServer()  {
        String tmpDirectory = System.getProperty("java.io.tmpdir");

        String directoryName = "ZK-Data-" + RandomGenUtils.getRandomIntwithin(Integer.MAX_VALUE);
        File dir = new File(tmpDirectory, directoryName).getAbsoluteFile();
        Exception lastException = new VeniceException("There is no spoon.");
        String errorMessage = "OHAI.";

        for (int attempt = 1; attempt <= MAX_ATTEMPT; attempt++) {
            try {
                ZooKeeperServer zooKeeperServer = new ZooKeeperServer(dir, dir, TICK_TIME);
                zooKeeperServer.setMaxSessionTimeout(MAX_SESSION_TIMEOUT);

                NIOServerCnxn.Factory standaloneServerFactory = new NIOServerCnxn.Factory(
                        new InetSocketAddress(PortUtils.getFreePort()), NUM_CONNECTIONS);
                standaloneServerFactory.startup(zooKeeperServer);

                return new ZkServerWrapper(zooKeeperServer, dir);
            } catch (Exception e) {
                lastException = e;
                errorMessage = "Got " + e.getClass().getSimpleName() +
                        " while trying to start ZK. Attempt #" + attempt + "/" + MAX_ATTEMPT + ".";
                LOGGER.debug(errorMessage);
            }
        }

        throw new VeniceException(errorMessage + " Aborting.", lastException);
    }

    // INSTANCE-LEVEL STATE AND APIs

    private final ZooKeeperServer zkServer;
    private final File dataDirectory;

    public ZkServerWrapper(ZooKeeperServer zkServer, File dataDirectory) {
        this.zkServer = zkServer;
        this.dataDirectory = dataDirectory;
    }

    /**
     * @return the port of the ZK server
     */
    public int getPort() {
        return zkServer.getClientPort();
    }

    /**
     * @return the address of the ZK server in the form of "hostname:port"
     */
    public String getZkAddress() {
        return "localhost:" + getPort();
    }

    public void close() {
        zkServer.shutdown();
        try {
            FileUtils.deleteDirectory(dataDirectory);
        } catch (IOException e) {
            LOGGER.error("Failed to delete ZK data directory: " + dataDirectory.getAbsolutePath(), e);
        }
    }
}
