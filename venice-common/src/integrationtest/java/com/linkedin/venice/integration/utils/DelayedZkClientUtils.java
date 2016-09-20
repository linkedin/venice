package com.linkedin.venice.integration.utils;

import org.apache.zookeeper.ClientCnxnSocketDelayedNIO;
import org.apache.zookeeper.ZooKeeper;


/**
 * If test cases were executed in parallel in same JVM, the behavior of utils might not be what you expected, because
 * the scopes of these methods are the whole jvm level. It means if you start using delayed socket IO for zk clients in
 * test case1 at point t1, the new zk clients that would be created after t1 in other running test cases would also be
 * affected.
 */
public class DelayedZkClientUtils {
  public static void startDelayingSocketIoForNewZkClients(long socketIoDelayLowerBound, long socketIoDelayUpperBound) {
    System.setProperty(ZooKeeper.ZOOKEEPER_CLIENT_CNXN_SOCKET, ClientCnxnSocketDelayedNIO.class.getName());
    ClientCnxnSocketDelayedNIO.setDefaultSocketIoDelayLowerBoundMS(socketIoDelayLowerBound);
    ClientCnxnSocketDelayedNIO.setDefaultSocketIoDelayUpperBoundMS(socketIoDelayUpperBound);
  }

  public static void stopDelayingSocketIoForNewZkClients() {
    System.clearProperty(ZooKeeper.ZOOKEEPER_CLIENT_CNXN_SOCKET);
  }
}
