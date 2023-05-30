package com.linkedin.venice.fastclient.meta;

import static com.linkedin.venice.fastclient.meta.utils.RequestBasedMetadataTestUtils.KEY_SCHEMA;
import static com.linkedin.venice.fastclient.meta.utils.RequestBasedMetadataTestUtils.VALUE_SCHEMA;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.meta.utils.RequestBasedMetadataTestUtils;
import java.io.IOException;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RequestBasedMetadataTest {
  private static final int CURRENT_VERSION = 1;
  private static final String REPLICA_NAME = "host1";

  @Test
  public void testMetadata() throws IOException {
    String storeName = "testStore";

    ClientConfig clientConfig = RequestBasedMetadataTestUtils.getMockClientConfig(storeName);
    RequestBasedMetadata requestBasedMetadata = null;

    try {
      requestBasedMetadata = RequestBasedMetadataTestUtils.getMockMetaData(clientConfig, storeName);
      Assert.assertEquals(requestBasedMetadata.getStoreName(), storeName);
      Assert.assertEquals(requestBasedMetadata.getCurrentStoreVersion(), CURRENT_VERSION);
      Assert
          .assertEquals(requestBasedMetadata.getReplicas(CURRENT_VERSION, 0), Collections.singletonList(REPLICA_NAME));
      Assert.assertEquals(requestBasedMetadata.getKeySchema().toString(), KEY_SCHEMA);
      Assert.assertEquals(requestBasedMetadata.getValueSchema(1).toString(), VALUE_SCHEMA);
      Assert.assertEquals(requestBasedMetadata.getLatestValueSchemaId(), Integer.valueOf(1));
      Assert.assertEquals(requestBasedMetadata.getLatestValueSchema().toString(), VALUE_SCHEMA);
      Assert.assertEquals(
          requestBasedMetadata.getCompressor(CompressionStrategy.ZSTD_WITH_DICT, CURRENT_VERSION),
          RequestBasedMetadataTestUtils.getZstdVeniceCompressor(storeName));
    } finally {
      if (requestBasedMetadata != null) {
        requestBasedMetadata.close();
      }
    }
  }
}
