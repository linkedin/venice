package com.linkedin.venice.serialization;

import com.linkedin.venice.helix.VeniceJsonSerializer;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataRecoveryVersionConfig;
import com.linkedin.venice.meta.DataRecoveryVersionConfigImpl;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VersionJsonSerializerTest {
  @Test
  public void testSerializeAndDeserializeVersion() throws IOException {
    VeniceJsonSerializer<Version> versionVeniceJsonSerializer = new VeniceJsonSerializer<>(Version.class);
    Version version = new VersionImpl("test", 1, "test-push-job-id");
    // Set all the record type fields to make sure we can serialize and deserialize them properly.
    HybridStoreConfig hybridStoreConfig =
        new HybridStoreConfigImpl(100, 1, 10, DataReplicationPolicy.AGGREGATE, BufferReplayPolicy.REWIND_FROM_EOP);
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    partitionerConfig.setAmplificationFactor(2);
    version.setPartitionerConfig(partitionerConfig);
    DataRecoveryVersionConfig dataRecoveryVersionConfig = new DataRecoveryVersionConfigImpl("dc-0", false, 1);
    version.setDataRecoveryVersionConfig(dataRecoveryVersionConfig);
    // Set some fields to non-default values.
    version.setHybridStoreConfig(hybridStoreConfig);
    version.setChunkingEnabled(true);
    version.setIncrementalPushEnabled(true);
    version.setUseVersionLevelHybridConfig(true);
    version.setUseVersionLevelIncrementalPushEnabled(true);

    byte[] data = versionVeniceJsonSerializer.serialize(version, "");
    Version newVersion = versionVeniceJsonSerializer.deserialize(data, "");
    Assert.assertEquals(newVersion, version);
    Assert.assertTrue(newVersion.isLeaderFollowerModelEnabled());
  }
}
