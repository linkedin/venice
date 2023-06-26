package com.linkedin.venice.listener.grpc;

import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.protocols.VeniceReadServiceGrpc;
import com.linkedin.venice.protocols.VeniceServerResponse;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.EncodingUtils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.nio.charset.StandardCharsets;
import org.apache.avro.SchemaBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceReadServiceClient {
  private final static Logger LOGGER = LogManager.getLogger(VeniceReadServiceClient.class);
  private final ManagedChannel originChannel;
  private final String storeName;
  private final VeniceReadServiceGrpc.VeniceReadServiceBlockingStub stub;
  protected volatile RecordSerializer<String> keySerializer;

  public VeniceReadServiceClient(String address, String storeName) {
    originChannel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
    this.storeName = storeName;
    stub = VeniceReadServiceGrpc.newBlockingStub(originChannel);

    // will only work for this specific schema --> need to pass schema as parameter in next iteration
    keySerializer = FastSerializerDeserializerFactory.getAvroGenericSerializer(SchemaBuilder.builder().stringType());
  }

  public String get(int version, int partition, String keyString) {
    // encode keyString in base64
    String resourceName = storeName + "_v" + version;

    byte[] serializedString = keySerializer.serialize(keyString);
    String b64Key = EncodingUtils.base64EncodeToString(serializedString) + "?f=b64";

    VeniceClientRequest request = VeniceClientRequest.newBuilder()
        .setStoreName(storeName)
        .setResourceName(resourceName)
        .setPartition(partition)
        .setKeyString(b64Key)
        .build();

    VeniceServerResponse response = stub.get(request);
    String dataResponse = new String(response.getData().toByteArray(), StandardCharsets.US_ASCII);
    LOGGER.debug("grpc Client Response: '" + dataResponse + "'");
    // convert to string, not getting decoded properly. Has an extra space in the front. Will debug.
    return dataResponse.trim();
  }

  public void shutdown() {
    originChannel.shutdown();
  }
}
