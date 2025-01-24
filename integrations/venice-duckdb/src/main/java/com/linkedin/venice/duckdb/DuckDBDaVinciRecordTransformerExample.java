package com.linkedin.venice.duckdb;

import static com.linkedin.venice.ConfigKeys.DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;


public class DuckDBDaVinciRecordTransformerExample {
  public static void main(String[] args) throws Exception {
    DaVinciConfig clientConfig = new DaVinciConfig();
    String clusterDiscoveryD2ServiceName = "venice-discovery";
    MetricsRepository metricsRepository = new MetricsRepository();
    String zkHosts = "zk-example-url.com:12913";

    VeniceProperties backendConfig =
        new PropertyBuilder().put(DA_VINCI_CURRENT_VERSION_BOOTSTRAPPING_SPEEDUP_ENABLED, true).build();

    String outputDir = "/example/output/dir";
    String storeName = "example_store_name";
    Set<String> columnsToProject = Collections.emptySet();

    String schema = IOUtils.toString(
        Objects.requireNonNull(
            DuckDBDaVinciRecordTransformerExample.class.getClassLoader().getResourceAsStream("ValueSchema.avsc")),
        StandardCharsets.UTF_8);
    Schema outputSchema = AvroCompatibilityHelper.parse(schema);

    DaVinciRecordTransformerConfig recordTransformerConfig = new DaVinciRecordTransformerConfig(
        (storeVersion, keySchema, inputValueSchema, outputValueSchema) -> new DuckDBDaVinciRecordTransformer(
            storeVersion,
            keySchema,
            inputValueSchema,
            outputValueSchema,
            false,
            outputDir,
            storeName,
            columnsToProject),
        GenericRecord.class,
        outputSchema);
    clientConfig.setRecordTransformerConfig(recordTransformerConfig);

    D2Client d2Client = new D2ClientBuilder().setZkHosts(zkHosts)
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    D2ClientUtils.startClient(d2Client);

    try (CachingDaVinciClientFactory factory =
        new CachingDaVinciClientFactory(d2Client, clusterDiscoveryD2ServiceName, metricsRepository, backendConfig)) {
      DaVinciClient<Integer, Object> clientWithRecordTransformer =
          factory.getAndStartGenericAvroClient(storeName, clientConfig);

      // Data will get written to DucksDB
      clientWithRecordTransformer.subscribeAll().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
