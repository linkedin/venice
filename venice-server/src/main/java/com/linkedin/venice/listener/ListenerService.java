package com.linkedin.venice.listener;

import com.linkedin.venice.server.PartitionNodeAssignmentRepository;
import com.linkedin.venice.server.StoreRepository;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StorageEngineFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;


/**
 * Service that listens on configured port to accept incoming GET requests
 */
public class ListenerService extends AbstractVeniceService{
  private static final Logger logger = Logger.getLogger(ListenerService.class.getName());

  private final StoreRepository storeRepository;
  private final VeniceConfigService veniceConfigService;
  private final ConcurrentMap<String, StorageEngineFactory> persistenceTypeToStorageEngineFactoryMap;
  private final PartitionNodeAssignmentRepository partitionNodeAssignmentRepository;
  private Channel nettyServerChannel;
  static final ChannelGroup allChannels = new DefaultChannelGroup();

  protected ThreadPoolExecutor workerPool;
  private ServerBootstrap bootstrap = null;
  int port;

  //TODO: move netty config to a config file
  private static int numberRestServiceNettyWorkerThreads = 1;
  private static int numberRestServiceNettyBossThreads = 1;
  private static int nettyBacklogSize = 1000;


  public ListenerService(StoreRepository storeRepository, VeniceConfigService veniceConfigService,
      PartitionNodeAssignmentRepository partitionNodeAssignmentRepository) {
    super("listener-service");
    this.storeRepository = storeRepository;
    this.veniceConfigService = veniceConfigService;
    this.persistenceTypeToStorageEngineFactoryMap = new ConcurrentHashMap<String, StorageEngineFactory>();
    this.partitionNodeAssignmentRepository = partitionNodeAssignmentRepository;
    this.port = Integer.parseInt(veniceConfigService.getVeniceServerConfig().getListenerPort());
  }

  @Override
  public void startInner() throws Exception {
    this.workerPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(numberRestServiceNettyWorkerThreads);
    this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newFixedThreadPool(numberRestServiceNettyBossThreads),
        workerPool));
    this.bootstrap.setOption("backlog", nettyBacklogSize);
    this.bootstrap.setOption("child.tcpNoDelay", true);
    this.bootstrap.setOption("child.keepAlive", true);
    this.bootstrap.setOption("child.reuseAddress", true);
    this.bootstrap.setPipelineFactory(new ListenerPipelineFactory(storeRepository));

    // Bind and start to accept incoming connections.
    this.nettyServerChannel = this.bootstrap.bind(new InetSocketAddress(this.port));
    allChannels.add(nettyServerChannel);
    logger.info("REST service started on port: " + this.port);

  }

  @Override
  public void stopInner()
      throws Exception {

  }

  private byte[] get(Integer partition, String store, byte[] key){
    AbstractStorageEngine storageEngine = storeRepository.getLocalStorageEngine(store);
    return storageEngine.get(partition, key);
  }
}
