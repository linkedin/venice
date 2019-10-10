package com.linkedin.venice.integration.utils;

import com.linkedin.datastream.DatastreamRestClient;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.connectors.kafka.KafkaConnectorFactory;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerFactoryImpl;
import com.linkedin.datastream.kafka.KafkaCluster;
import com.linkedin.datastream.kafka.KafkaTransportProviderAdmin;
import com.linkedin.datastream.kafka.KafkaTransportProviderAdminFactory;
import com.linkedin.datastream.server.CoordinatorConfig;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.EmbeddedDatastreamCluster;
import com.linkedin.datastream.server.assignment.BroadcastStrategyFactory;
import com.linkedin.venice.replication.BrooklinTopicReplicator;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;



public class BrooklinWrapper extends ProcessWrapper {
  public static final String TPNAME = BrooklinTopicReplicator.TRANSPORT_PROVIDER_NAME; //Kafka transport provider
  public static final String CONNECTOR = BrooklinTopicReplicator.BROOKLIN_CONNECTOR_NAME;
  public static final String SERVICE_NAME = "brooklin";

  /**
   * This is package private because the only way to call this should be from
   * {@link ServiceFactory#getBrooklinWrapper(KafkaBrokerWrapper)}.
   *
   * @return a function which yields a {@link BrooklinWrapper} instance
   */
  static StatefulServiceProvider<BrooklinWrapper> generateService(KafkaBrokerWrapper kafka) {
    return (String serviceName, int port, File dir) -> {
      //Note: ignores the passed in port
      return new BrooklinWrapper(dir, kafka);
    };
  }

  private EmbeddedDatastreamCluster brooklin;

  /**
   */
  private BrooklinWrapper(File dataDirectory, KafkaBrokerWrapper kafka) {
    super(SERVICE_NAME, dataDirectory);

    try {
      Properties kafkaConnectorProperties = new Properties();
      kafkaConnectorProperties.put("factoryClassName", KafkaConnectorFactory.class.getCanonicalName());
      kafkaConnectorProperties.put("assignmentStrategyFactory", BroadcastStrategyFactory.class.getCanonicalName());
      kafkaConnectorProperties.put("consumerFactoryClassName", KafkaConsumerFactoryImpl.class.getCanonicalName());
      kafkaConnectorProperties.put("consumer.client.id", "venice-controller-war");

      Map<String, Properties> connectorProperties = new HashMap<>();
      connectorProperties.put(CONNECTOR, kafkaConnectorProperties);

      /**
       * The intent of this abstraction is to re-use ZK, otherwise Brooklin starts its own embedded ZK...
       */
      KafkaCluster brooklinKafkaCluster = new KafkaCluster() {
        @Override
        public String getBrokers() {
          return kafka.getAddress();
        }

        @Override
        public String getZkConnection() {
          return kafka.getZkAddress();
        }

        @Override
        public boolean isStarted() {
          return kafka.isRunning();
        }

        @Override
        public void startup() {
          // no op
        }

        @Override
        public void shutdown() {
          // no op
        }
      };

      Properties overrides = createOverrideProperties(brooklinKafkaCluster);

      // This call, which does not pass the KafkaCluster created above, will generate its own embedded ZK internally
      // brooklin = EmbeddedDatastreamCluster.newTestDatastreamCluster(connectorProperties, overrides);

      brooklin = EmbeddedDatastreamCluster.newTestDatastreamCluster(brooklinKafkaCluster, connectorProperties, overrides);
    } catch (IOException e) {
      throw new RuntimeException("IO Exception when creating embedded brooklin", e);
    } catch (DatastreamException e) {
      throw new RuntimeException("DatastreamException when creating embedded brooklin", e);
    }
  }

  /**
   * @see {@link ProcessWrapper#getHost()}
   */
  public String getHost() {
    return Utils.getHostName();
  }

  /**
   * @see {@link ProcessWrapper#getPort()}
   */
  public int getPort() {
    List<Integer> ports = brooklin.getDatastreamPorts();
    if (ports.isEmpty()){
      throw new RuntimeException("There are no ports");
    } else {
      //EmbeddedDatastreamCluster can run multiple datastream servers, ports is a list to support multiple servers.
      return ports.get(0);
    }
  }

  @Override
  protected void internalStart() throws Exception {
    brooklin.startup();
  }

  @Override
  protected void internalStop() throws Exception {
    brooklin.shutdown();
  }

  @Override
  protected void newProcess() throws Exception {
    throw new RuntimeException("Cannot start a new process for Brooklin");
  }

  public DatastreamRestClient getBrooklinClient() {
    return brooklin.createDatastreamRestClient();
  }

  /**
   * For use by DatastreamRestClientFactory
   * @return
   */
  public String getBrooklinDmsUri(){
    return "http://" + getAddress();
  }

  /**
   * copied from EmbeddedDatastreamClusterFactory
   * @param brooklinKafkaCluster
   * @return
   */
  private static Properties createOverrideProperties(KafkaCluster brooklinKafkaCluster) {
    Properties overrideProperties = new Properties();

    overrideProperties.put(CoordinatorConfig.CONFIG_DEFAULT_TRANSPORT_PROVIDER, TPNAME);
    overrideProperties.put(DatastreamServer.CONFIG_TRANSPORT_PROVIDER_NAMES, TPNAME);
    String tpPrefix = DatastreamServer.CONFIG_TRANSPORT_PROVIDER_PREFIX + TPNAME + ".";
    overrideProperties.put(tpPrefix + DatastreamServer.CONFIG_FACTORY_CLASS_NAME,
        KafkaTransportProviderAdminFactory.class.getName());

    overrideProperties.put(tpPrefix + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brooklinKafkaCluster.getBrokers());
    overrideProperties.put(tpPrefix + ProducerConfig.CLIENT_ID_CONFIG, "testProducerClientId");
    overrideProperties.put(tpPrefix + KafkaTransportProviderAdmin.CONFIG_ZK_CONNECT,
        brooklinKafkaCluster.getZkConnection());

    return overrideProperties;
  }

}
