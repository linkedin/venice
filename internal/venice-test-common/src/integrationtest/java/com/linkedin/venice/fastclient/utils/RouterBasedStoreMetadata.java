package com.linkedin.venice.fastclient.utils;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.meta.AbstractStoreMetadata;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;


/**
 * TODO: get rid of this impl since {@link com.linkedin.venice.fastclient.meta.DaVinciClientBasedMetadata} is available.
 */
public class RouterBasedStoreMetadata extends AbstractStoreMetadata {
  private final ReadOnlyStoreRepository storeRepository;
  private final ReadOnlySchemaRepository schemaRepository;
  private final OnlineInstanceFinder onlineInstanceFinder;
  private final String storeName;

  public RouterBasedStoreMetadata(
      ReadOnlyStoreRepository storeRepository,
      ReadOnlySchemaRepository schemaRepository,
      OnlineInstanceFinder onlineInstanceFinder,
      String storeName,
      ClientConfig clientConfig) {
    super(clientConfig);
    this.storeRepository = storeRepository;
    this.schemaRepository = schemaRepository;
    this.onlineInstanceFinder = onlineInstanceFinder;
    this.storeName = storeName;
  }

  @Override
  public int getCurrentStoreVersion() {
    Store store = storeRepository.getStoreOrThrow(storeName);
    return store.getCurrentVersion();
  }

  @Override
  public int getPartitionId(int versionNumber, ByteBuffer key) {
    Store store = storeRepository.getStoreOrThrow(storeName);
    Optional<Version> version = store.getVersion(versionNumber);
    if (!version.isPresent()) {
      throw new VeniceClientException("Version: " + versionNumber + " doesn't exist");
    }
    int partitionCount = version.get().getPartitionCount();
    VenicePartitioner partitioner = new DefaultVenicePartitioner();
    return partitioner.getPartitionId(key, partitionCount);
  }

  @Override
  public List<String> getReplicas(int version, int partitionId) {
    String resource = Version.composeKafkaTopic(storeName, version);
    List<Instance> instances = onlineInstanceFinder.getReadyToServeInstances(resource, partitionId);
    return instances.stream().map(instance -> instance.getUrl(true)).collect(Collectors.toList());
  }

  @Override
  public VeniceCompressor getCompressor(CompressionStrategy compressionStrategy, int version) {
    return new CompressorFactory().getCompressor(compressionStrategy);
  }

  @Override
  public void start() {
    // do nothing
  }

  @Override
  public Schema getKeySchema() {
    return schemaRepository.getKeySchema(storeName).getSchema();
  }

  @Override
  public Schema getValueSchema(int id) {
    return schemaRepository.getValueSchema(storeName, id).getSchema();
  }

  @Override
  public int getValueSchemaId(Schema schema) {
    return schemaRepository.getValueSchemaId(storeName, schema.toString());
  }

  @Override
  public Schema getLatestValueSchema() {
    return schemaRepository.getSupersetOrLatestValueSchema(storeName).getSchema();
  }

  @Override
  public Integer getLatestValueSchemaId() {
    return schemaRepository.getSupersetOrLatestValueSchema(storeName).getId();
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
