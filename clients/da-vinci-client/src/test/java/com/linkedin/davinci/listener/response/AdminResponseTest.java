package com.linkedin.davinci.listener.response;

import static com.linkedin.davinci.listener.response.AdminResponse.IGNORED_COMPRESSION_DICT;

import com.linkedin.davinci.replication.BatchConflictResolutionPolicy;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AdminResponseTest {
  @Test
  public void testAddStoreVersionStateToSuppressDict() {
    StoreVersionState storeVersionState = new StoreVersionState();
    storeVersionState.compressionDictionary = ByteBuffer.wrap("big_dict".getBytes());
    storeVersionState.sorted = true;
    storeVersionState.chunked = false;
    storeVersionState.compressionStrategy = CompressionStrategy.ZSTD_WITH_DICT.ordinal();
    storeVersionState.batchConflictResolutionPolicy = BatchConflictResolutionPolicy.BATCH_WRITE_LOSES.getValue();
    storeVersionState.startOfPushTimestamp = System.currentTimeMillis();
    storeVersionState.endOfPushTimestamp = System.currentTimeMillis();

    StoreVersionState updatedState = AdminResponse.suppressCompressionDict(storeVersionState);

    Assert.assertEquals(updatedState.compressionDictionary.array(), IGNORED_COMPRESSION_DICT.array());

    Assert.assertEquals(
        AdminResponse.getResponseSchemaIdHeader(),
        AvroProtocolDefinition.SERVER_ADMIN_RESPONSE.getCurrentProtocolVersion());
  }
}
