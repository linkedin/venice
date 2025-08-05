package com.linkedin.venice.listener.grpc.handlers;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.protocols.CountByBucketRequest;
import com.linkedin.venice.protocols.CountByBucketResponse;
import com.linkedin.venice.protocols.CountByValueRequest;
import com.linkedin.venice.protocols.CountByValueResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Processor for handling gRPC requests in Venice server.
 */
public class VeniceServerGrpcRequestProcessor {
  private static final Logger LOGGER = LogManager.getLogger(VeniceServerGrpcRequestProcessor.class);

  private VeniceServerGrpcHandler head = null;
  private final StorageEngineRepository storageEngineRepository;
  private final ReadOnlySchemaRepository schemaRepository;
  private final ReadOnlyStoreRepository storeRepository;
  private final ThreadPoolExecutor executor;
  private final StorageEngineBackedCompressorFactory compressorFactory;
  private final CountByValueProcessor countByValueProcessor;
  private final CountByBucketProcessor countByBucketProcessor;

  // Default constructor for backward compatibility with existing tests
  public VeniceServerGrpcRequestProcessor() {
    this.storageEngineRepository = null;
    this.schemaRepository = null;
    this.storeRepository = null;
    this.executor = null;
    this.compressorFactory = null;
    this.countByValueProcessor = null;
    this.countByBucketProcessor = null;
  }

  // Main constructor with direct dependency injection
  public VeniceServerGrpcRequestProcessor(
      StorageEngineRepository storageEngineRepository,
      ReadOnlySchemaRepository schemaRepository,
      ReadOnlyStoreRepository storeRepository,
      ThreadPoolExecutor executor,
      StorageEngineBackedCompressorFactory compressorFactory) {
    this.storageEngineRepository = storageEngineRepository;
    this.schemaRepository = schemaRepository;
    this.storeRepository = storeRepository;
    this.executor = executor;
    this.compressorFactory = compressorFactory;

    if (this.storageEngineRepository != null && this.schemaRepository != null && this.storeRepository != null) {
      this.countByValueProcessor =
          new CountByValueProcessor(storageEngineRepository, schemaRepository, storeRepository, compressorFactory);
      this.countByBucketProcessor =
          new CountByBucketProcessor(storageEngineRepository, schemaRepository, storeRepository, compressorFactory);
    } else {
      LOGGER.warn("Some dependencies are null, countByValue and countByBucket functionality will not be available");
      this.countByValueProcessor = null;
      this.countByBucketProcessor = null;
    }
  }

  public void addHandler(VeniceServerGrpcHandler handler) {
    if (head == null) {
      head = handler;
      return;
    }

    VeniceServerGrpcHandler current = head;
    while (current.getNext() != null) {
      current = current.getNext();
    }

    current.addNextHandler(handler);
  }

  public void processRequest(GrpcRequestContext context) {
    if (head != null) {
      head.processRequest(context);
    }
  }

  /**
   * Process countByValue aggregation request with real data processing.
   */
  public CountByValueResponse processCountByValue(CountByValueRequest request) {
    if (countByValueProcessor == null) {
      LOGGER.warn("CountByValue dependencies not available");
      return CountByValueResponse.newBuilder()
          .setErrorCode(VeniceReadResponseStatus.INTERNAL_ERROR)
          .setErrorMessage("CountByValue dependencies not available")
          .build();
    }
    return countByValueProcessor.processCountByValue(request);
  }

  /**
   * Process countByBucket aggregation request with real data processing.
   */
  public CountByBucketResponse processCountByBucket(CountByBucketRequest request) {
    if (countByBucketProcessor == null) {
      LOGGER.warn("CountByBucket dependencies not available");
      return CountByBucketResponse.newBuilder()
          .setErrorCode(VeniceReadResponseStatus.INTERNAL_ERROR)
          .setErrorMessage("CountByBucket dependencies not available")
          .build();
    }
    return countByBucketProcessor.processCountByBucket(request);
  }
}
