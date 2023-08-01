package com.linkedin.venice.fastclient.meta;

import static com.linkedin.venice.fastclient.meta.utils.RequestBasedMetadataTestUtils.KEY_SCHEMA;
import static com.linkedin.venice.fastclient.meta.utils.RequestBasedMetadataTestUtils.VALUE_SCHEMA;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.meta.utils.RequestBasedMetadataTestUtils;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class RequestBasedMetadataTest {
  private static final int CURRENT_VERSION = 1;

  @Test
  public void testMetadata() throws IOException {
    String storeName = "testStore";

    ClientConfig clientConfig = RequestBasedMetadataTestUtils.getMockClientConfig(storeName);
    RequestBasedMetadata requestBasedMetadata = null;

    try {
      requestBasedMetadata = RequestBasedMetadataTestUtils.getMockMetaData(clientConfig, storeName);
      assertEquals(requestBasedMetadata.getStoreName(), storeName);
      assertEquals(requestBasedMetadata.getCurrentStoreVersion(), CURRENT_VERSION);
      assertEquals(
          requestBasedMetadata.getReplicas(CURRENT_VERSION, 0),
          Collections.singletonList(RequestBasedMetadataTestUtils.REPLICA_NAME));
      assertEquals(requestBasedMetadata.getKeySchema().toString(), KEY_SCHEMA);
      assertEquals(requestBasedMetadata.getValueSchema(1).toString(), VALUE_SCHEMA);
      assertEquals(requestBasedMetadata.getLatestValueSchemaId(), Integer.valueOf(1));
      assertEquals(requestBasedMetadata.getLatestValueSchema().toString(), VALUE_SCHEMA);
      assertEquals(
          requestBasedMetadata.getCompressor(CompressionStrategy.ZSTD_WITH_DICT, CURRENT_VERSION),
          RequestBasedMetadataTestUtils.getZstdVeniceCompressor(storeName));
      final RequestBasedMetadata finalRequestBasedMetadata = requestBasedMetadata;
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          () -> assertEquals(
              finalRequestBasedMetadata.getReplicas(CURRENT_VERSION, 0),
              Collections.singletonList(RequestBasedMetadataTestUtils.NEW_REPLICA_NAME)));
    } finally {
      if (requestBasedMetadata != null) {
        requestBasedMetadata.close();
      }
    }
  }
}
