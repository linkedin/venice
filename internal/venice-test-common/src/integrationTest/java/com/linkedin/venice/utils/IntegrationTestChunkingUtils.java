package com.linkedin.venice.utils;

import static com.linkedin.venice.utils.IntegrationTestReadUtils.serializeStringKeyToByteArray;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.linkedin.davinci.replication.RmdWithValueSchemaId;
import com.linkedin.davinci.replication.merge.RmdSerDe;
import com.linkedin.davinci.storage.chunking.ChunkingUtils;
import com.linkedin.davinci.storage.chunking.SingleGetChunkingAdapter;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.record.ValueRecord;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.testng.Assert;


public class IntegrationTestChunkingUtils {
  private static final ChunkedValueManifestSerializer CHUNKED_VALUE_MANIFEST_SERIALIZER =
      new ChunkedValueManifestSerializer(false);

  private IntegrationTestChunkingUtils() {
  }

  public static void validateValueChunks(
      VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionWrapper,
      String clusterName,
      String kafkaTopic,
      String key,
      Consumer<byte[]> validationFlow) {
    for (VeniceServerWrapper serverWrapper: multiRegionWrapper.getChildRegions()
        .get(0)
        .getClusters()
        .get(clusterName)
        .getVeniceServers()) {
      StorageEngine storageEngine = serverWrapper.getVeniceServer().getStorageService().getStorageEngine(kafkaTopic);
      assertNotNull(storageEngine);

      ChunkedValueManifest manifest = getChunkValueManifest(storageEngine, 0, key, false);
      Assert.assertNotNull(manifest);

      for (ByteBuffer chunkedKey: manifest.keysWithChunkIdSuffix) {
        byte[] chunkValueBytes = storageEngine.get(0, chunkedKey.array());
        validationFlow.accept(chunkValueBytes);
      }
    }
  }

  public static void validateChunksFromManifests(
      VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionWrapper,
      String clusterName,
      String kafkaTopic,
      int partition,
      ChunkedValueManifest valueManifest,
      ChunkedValueManifest rmdManifest,
      BiConsumer<byte[], byte[]> validationFlow,
      boolean isAAEnabled) {
    for (VeniceServerWrapper serverWrapper: multiRegionWrapper.getChildRegions()
        .get(0)
        .getClusters()
        .get(clusterName)
        .getVeniceServers()) {
      StorageEngine storageEngine = serverWrapper.getVeniceServer().getStorageService().getStorageEngine(kafkaTopic);
      assertNotNull(storageEngine);

      validateChunkDataFromManifest(storageEngine, partition, valueManifest, validationFlow, isAAEnabled);
      validateChunkDataFromManifest(storageEngine, partition, rmdManifest, validationFlow, isAAEnabled);
    }
  }

  public static ChunkedValueManifest getChunkValueManifest(
      StorageEngine storageEngine,
      int partition,
      String key,
      boolean isRmd) {
    byte[] serializedKeyBytes =
        ChunkingUtils.KEY_WITH_CHUNKING_SUFFIX_SERIALIZER.serializeNonChunkedKey(serializeStringKeyToByteArray(key));
    byte[] manifestValueBytes = isRmd
        ? storageEngine.getReplicationMetadata(partition, ByteBuffer.wrap(serializedKeyBytes))
        : storageEngine.get(partition, serializedKeyBytes);
    if (manifestValueBytes == null) {
      return null;
    }
    int schemaId = ValueRecord.parseSchemaId(manifestValueBytes);
    Assert.assertEquals(schemaId, AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion());
    return CHUNKED_VALUE_MANIFEST_SERIALIZER.deserialize(manifestValueBytes, schemaId);
  }

  public static void validateChunkDataFromManifest(
      StorageEngine storageEngine,
      int partition,
      ChunkedValueManifest manifest,
      BiConsumer<byte[], byte[]> validationFlow,
      boolean isAAEnabled) {
    if (manifest == null) {
      return;
    }
    for (int i = 0; i < manifest.keysWithChunkIdSuffix.size(); i++) {
      byte[] chunkKeyBytes = manifest.keysWithChunkIdSuffix.get(i).array();
      byte[] valueBytes = storageEngine.get(partition, chunkKeyBytes);
      byte[] rmdBytes =
          isAAEnabled ? storageEngine.getReplicationMetadata(partition, ByteBuffer.wrap(chunkKeyBytes)) : null;
      validationFlow.accept(valueBytes, rmdBytes);
    }
  }

  public static void validateRmdData(
      VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionWrapper,
      String clusterName,
      RmdSerDe rmdSerDe,
      String kafkaTopic,
      String key,
      Consumer<RmdWithValueSchemaId> rmdDataValidationFlow) {
    for (VeniceServerWrapper serverWrapper: multiRegionWrapper.getChildRegions()
        .get(0)
        .getClusters()
        .get(clusterName)
        .getVeniceServers()) {
      StorageEngine storageEngine = serverWrapper.getVeniceServer().getStorageService().getStorageEngine(kafkaTopic);
      assertNotNull(storageEngine);
      ValueRecord result = SingleGetChunkingAdapter
          .getReplicationMetadata(storageEngine, 0, serializeStringKeyToByteArray(key), true, null);
      boolean nullRmd = (result == null);
      assertFalse(nullRmd);
      byte[] value = result.serialize();
      RmdWithValueSchemaId rmdWithValueSchemaId = new RmdWithValueSchemaId();
      rmdSerDe.deserializeValueSchemaIdPrependedRmdBytes(value, rmdWithValueSchemaId);
      rmdDataValidationFlow.accept(rmdWithValueSchemaId);
    }
  }
}
