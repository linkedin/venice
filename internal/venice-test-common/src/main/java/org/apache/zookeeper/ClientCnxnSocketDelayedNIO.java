package org.apache.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.client.ZKClientConfig;


/**
 * Customized zookeeper {@link ClientCnxnSocket} implementation that introduce the latency while reading and writing
 * data to/from socket.
 * <p>
 * Please be aware that read a packet from socket need 2 read operations. The first one reads length of packet and the
 * second one read a {@link java.nio.ByteBuffer} with the length read before. Each zookeeper operation either a read or
 * write operation is composed by 2 steps: write a request packet to socket and read the response packet from socket. So
 * the totally delay for a completed zk operation is <b>3*socketIoDelay<b/>, socketIoDelay in
 * [socketIoDelayLowerBoundMS, socketIoDelayUpperBoundMS].
 * <p>
 * In case of simulating distributed Env. in a local box, you could use this class to replace to default zookeeper
 * socket to introduce any latency you want. For example, test sending state transition from controller to storage
 * nodes. You could make socket of one node dramatically slower to simulate one node become BOOTSTRAP behind than other
 * nodes.
 */
public class ClientCnxnSocketDelayedNIO extends ClientCnxnSocketNIO {
  private static final Logger LOGGER = LogManager.getLogger(ClientCnxnSocketDelayedNIO.class);
  /**
   * How long is the socket IO operation is delayed at least. It's the default value when creating {@link
   * ClientCnxnSocketDelayedNIO} instances.
   */
  private volatile static long defaultSocketIoDelayLowerBoundMS = 10;
  /**
   * How long is the socket IO operation is delayed at most. It's the default value when creating {@link
   * ClientCnxnSocketDelayedNIO} instances.
   */
  private volatile static long defaultSocketIoDelayUpperBoundMS = 20;

  private final long socketIoDelayLowerBoundMS;

  private final long socketIoDelayUpperBoundMS;

  ClientCnxnSocketDelayedNIO(ZKClientConfig clientConfig) throws IOException {
    super(clientConfig);
    socketIoDelayLowerBoundMS = defaultSocketIoDelayLowerBoundMS;
    socketIoDelayUpperBoundMS = defaultSocketIoDelayUpperBoundMS;
  }

  /**
   * Set up lower bound of delay of socket IO, please set it up before creating zookeeper client.
   *
   * @param defaultSocketIoDelayLowerBoundMS
   */
  public static void setDefaultSocketIoDelayLowerBoundMS(long defaultSocketIoDelayLowerBoundMS) {
    if (defaultSocketIoDelayLowerBoundMS < 0) {
      throw new IllegalArgumentException("defaultSocketIoDelayLowerBoundMS must be a non-negative number.");
    }
    ClientCnxnSocketDelayedNIO.defaultSocketIoDelayLowerBoundMS = defaultSocketIoDelayLowerBoundMS;
  }

  /**
   * Set up upper bound of delay of socket IO, please set it up before creating zookeeper client.
   *
   * @param defaultSocketIoDelayUpperBoundMS
   */
  public static void setDefaultSocketIoDelayUpperBoundMS(long defaultSocketIoDelayUpperBoundMS) {
    if (defaultSocketIoDelayUpperBoundMS < defaultSocketIoDelayLowerBoundMS) {
      throw new IllegalArgumentException(
          "defaultSocketIoDelayUpperBoundMS must be larger than defaultSocketIoDelayLowerBoundMS.");
    }
    ClientCnxnSocketDelayedNIO.defaultSocketIoDelayUpperBoundMS = defaultSocketIoDelayUpperBoundMS;
  }

  @Override
  void doIO(List<ClientCnxn.Packet> pendingQueue, ClientCnxn cnxn) throws InterruptedException, IOException {
    if (socketIoDelayUpperBoundMS > 0) {
      long socketIoDelay =
          (long) (Math.random() * (socketIoDelayUpperBoundMS - socketIoDelayLowerBoundMS) + socketIoDelayLowerBoundMS);
      try {
        TimeUnit.MILLISECONDS.sleep(socketIoDelay);
        LOGGER.debug("Delayed: {} ms", socketIoDelay);
      } catch (InterruptedException e) {
        LOGGER.error("Delay is interrupted, io operation will continue to be executed.", e);
      }
    }
    super.doIO(pendingQueue, cnxn);
  }
}
