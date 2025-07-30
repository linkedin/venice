package com.linkedin.venice.listener.grpc.handlers;

import com.linkedin.davinci.compression.StorageEngineBackedCompressorFactory;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.venice.listener.StorageReadRequestHandler;
import com.linkedin.venice.listener.grpc.GrpcRequestContext;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.protocols.CountByValueRequest;
import com.linkedin.venice.protocols.CountByValueResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Processor for handling gRPC requests in Venice server.
 */
public class VeniceServerGrpcRequestProcessor {
  private static final Logger LOGGER = LogManager.getLogger(VeniceServerGrpcRequestProcessor.class);
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  private VeniceServerGrpcHandler head = null;
  private final StorageReadRequestHandler storageReadRequestHandler;
  private final StorageEngineRepository storageEngineRepository;
  private final ReadOnlySchemaRepository schemaRepository;
  private final ReadOnlyStoreRepository storeRepository;
  private final ThreadPoolExecutor executor;
  private final StorageEngineBackedCompressorFactory compressorFactory;
  private final CountByValueProcessor countByValueProcessor;

  // Default constructor for backward compatibility with existing tests
  public VeniceServerGrpcRequestProcessor() {
    this.storageReadRequestHandler = null;
    this.storageEngineRepository = null;
    this.schemaRepository = null;
    this.storeRepository = null;
    this.executor = null;
    this.compressorFactory = null;
    this.countByValueProcessor = null;
  }

  public VeniceServerGrpcRequestProcessor(StorageReadRequestHandler storageReadRequestHandler) {
    this.storageReadRequestHandler = storageReadRequestHandler;

    // Check if we should force production mode
    boolean forceProductionMode = Boolean.getBoolean("venice.grpc.force.production.mode");

    // Use more graceful extraction method that handles test environments better
    if (forceProductionMode) {
      LOGGER.info("Force production mode enabled, using fallback extraction for better test compatibility");
      this.storageEngineRepository = DependencyExtractor.extractDependencyWithFallback(
          storageReadRequestHandler,
          "storageEngineRepository",
          StorageEngineRepository.class);
      this.schemaRepository = DependencyExtractor
          .extractDependencyWithFallback(storageReadRequestHandler, "schemaRepository", ReadOnlySchemaRepository.class);
      this.storeRepository = DependencyExtractor.extractDependencyWithFallback(
          storageReadRequestHandler,
          "metadataRepository",
          ReadOnlyStoreRepository.class);
      this.executor = DependencyExtractor
          .extractDependencyWithFallback(storageReadRequestHandler, "executor", ThreadPoolExecutor.class);
      this.compressorFactory = DependencyExtractor.extractDependencyWithFallback(
          storageReadRequestHandler,
          "compressorFactory",
          StorageEngineBackedCompressorFactory.class);
    } else {
      // Extract dependencies with graceful fallback
      this.storageEngineRepository = DependencyExtractor
          .extractDependency(storageReadRequestHandler, "storageEngineRepository", StorageEngineRepository.class);
      this.schemaRepository = DependencyExtractor
          .extractDependency(storageReadRequestHandler, "schemaRepository", ReadOnlySchemaRepository.class);
      this.storeRepository = DependencyExtractor
          .extractDependency(storageReadRequestHandler, "metadataRepository", ReadOnlyStoreRepository.class);
      this.executor =
          DependencyExtractor.extractDependency(storageReadRequestHandler, "executor", ThreadPoolExecutor.class);
      this.compressorFactory = DependencyExtractor.extractDependency(
          storageReadRequestHandler,
          "compressorFactory",
          StorageEngineBackedCompressorFactory.class);
    }

    if (this.storageEngineRepository != null && this.schemaRepository != null && this.storeRepository != null) {
      LOGGER.info("Successfully initialized VeniceServerGrpcRequestProcessor with extracted dependencies");
      this.countByValueProcessor = new CountByValueProcessor(
          this.storageEngineRepository,
          this.schemaRepository,
          this.storeRepository,
          this.compressorFactory);
    } else {
      LOGGER.warn("Some dependencies are null, countByValue functionality will use fallback behavior");
      this.countByValueProcessor = null;
    }
  }

  // Constructor for testing with direct dependency injection
  public VeniceServerGrpcRequestProcessor(
      StorageEngineRepository storageEngineRepository,
      ReadOnlySchemaRepository schemaRepository,
      ReadOnlyStoreRepository storeRepository,
      ThreadPoolExecutor executor,
      StorageEngineBackedCompressorFactory compressorFactory) {
    this.storageReadRequestHandler = null; // Not needed for testing
    this.storageEngineRepository = storageEngineRepository;
    this.schemaRepository = schemaRepository;
    this.storeRepository = storeRepository;
    this.executor = executor;
    this.compressorFactory = compressorFactory;
    this.countByValueProcessor =
        new CountByValueProcessor(storageEngineRepository, schemaRepository, storeRepository, compressorFactory);
  }

  // Constructor for testing with minimal dependencies (for basic functionality)
  public VeniceServerGrpcRequestProcessor(StorageReadRequestHandler storageReadRequestHandler, boolean isTestMode) {
    this.storageReadRequestHandler = storageReadRequestHandler;
    if (isTestMode) {
      // In test mode, try to extract dependencies but provide more graceful fallback behavior
      StorageEngineRepository extractedStorageEngineRepository = null;
      try {
        extractedStorageEngineRepository = DependencyExtractor.extractDependencyWithFallback(
            storageReadRequestHandler,
            "storageEngineRepository",
            StorageEngineRepository.class);
        LOGGER.info("Successfully extracted storageEngineRepository in test mode");
      } catch (Exception e) {
        LOGGER.warn("Could not extract storageEngineRepository in test mode: {}", e.getMessage());
      }
      this.storageEngineRepository = extractedStorageEngineRepository;

      ReadOnlySchemaRepository extractedSchemaRepository = null;
      try {
        extractedSchemaRepository = DependencyExtractor.extractDependencyWithFallback(
            storageReadRequestHandler,
            "schemaRepository",
            ReadOnlySchemaRepository.class);
        LOGGER.info("Successfully extracted schemaRepository in test mode");
      } catch (Exception e) {
        LOGGER.warn("Could not extract schemaRepository in test mode: {}", e.getMessage());
      }
      this.schemaRepository = extractedSchemaRepository;

      ReadOnlyStoreRepository extractedStoreRepository = null;
      try {
        extractedStoreRepository = DependencyExtractor.extractDependencyWithFallback(
            storageReadRequestHandler,
            "metadataRepository",
            ReadOnlyStoreRepository.class);
        LOGGER.info("Successfully extracted storeRepository in test mode");
      } catch (Exception e) {
        LOGGER.warn("Could not extract storeRepository in test mode: {}", e.getMessage());
      }
      this.storeRepository = extractedStoreRepository;

      ThreadPoolExecutor extractedExecutor = null;
      try {
        extractedExecutor = DependencyExtractor
            .extractDependencyWithFallback(storageReadRequestHandler, "executor", ThreadPoolExecutor.class);
        LOGGER.info("Successfully extracted executor in test mode");
      } catch (Exception e) {
        LOGGER.warn("Could not extract executor in test mode: {}", e.getMessage());
      }
      this.executor = extractedExecutor;

      StorageEngineBackedCompressorFactory extractedCompressorFactory = null;
      try {
        extractedCompressorFactory = DependencyExtractor.extractDependencyWithFallback(
            storageReadRequestHandler,
            "compressorFactory",
            StorageEngineBackedCompressorFactory.class);
        LOGGER.info("Successfully extracted compressorFactory in test mode");
      } catch (Exception e) {
        LOGGER.warn("Could not extract compressorFactory in test mode: {}", e.getMessage());
      }
      this.compressorFactory = extractedCompressorFactory;

      // Log overall status
      boolean allDependenciesAvailable = this.storageEngineRepository != null && this.schemaRepository != null
          && this.storeRepository != null && this.compressorFactory != null;
      LOGGER.info("Test mode dependency extraction complete. All dependencies available: {}", allDependenciesAvailable);

      // Initialize CountByValueProcessor if dependencies are available
      if (allDependenciesAvailable) {
        this.countByValueProcessor = new CountByValueProcessor(
            this.storageEngineRepository,
            this.schemaRepository,
            this.storeRepository,
            this.compressorFactory);
      } else {
        this.countByValueProcessor = null;
      }
    } else {
      // Normal mode - extract dependencies with exceptions
      this.storageEngineRepository = DependencyExtractor
          .extractDependency(storageReadRequestHandler, "storageEngineRepository", StorageEngineRepository.class);
      this.schemaRepository = DependencyExtractor
          .extractDependency(storageReadRequestHandler, "schemaRepository", ReadOnlySchemaRepository.class);
      this.storeRepository = DependencyExtractor
          .extractDependency(storageReadRequestHandler, "metadataRepository", ReadOnlyStoreRepository.class);
      this.executor =
          DependencyExtractor.extractDependency(storageReadRequestHandler, "executor", ThreadPoolExecutor.class);
      this.compressorFactory = DependencyExtractor.extractDependency(
          storageReadRequestHandler,
          "compressorFactory",
          StorageEngineBackedCompressorFactory.class);

      this.countByValueProcessor = new CountByValueProcessor(
          this.storageEngineRepository,
          this.schemaRepository,
          this.storeRepository,
          this.compressorFactory);
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
}
