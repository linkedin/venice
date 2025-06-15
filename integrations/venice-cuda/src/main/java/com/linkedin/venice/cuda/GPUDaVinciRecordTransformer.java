package com.linkedin.venice.cuda;

import com.linkedin.davinci.client.DaVinciRecordTransformer;
import com.linkedin.davinci.client.DaVinciRecordTransformerConfig;
import com.linkedin.davinci.client.DaVinciRecordTransformerResult;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This record transformer stores and manages data on a local GPU.  Data for a given
 * version is stored like a hashmap, and keys and values are stored as raw bytes.
 */
public class GPUDaVinciRecordTransformer<K, V, O> extends DaVinciRecordTransformer<K, V, O> {
  private static final Logger LOGGER = LogManager.getLogger(GPUDaVinciRecordTransformer.class);

  // Native methods for GPU operations
  static {
    NativeLibraryLoader.loadVeniceGPUTransformer();
  }

  // GPU memory management native methods
  private native long nativeInitializeGPU(int deviceId);

  private native void nativeShutdownGPU(long gpuContext);

  private native long nativeCreateHashMap(long gpuContext, int initialCapacity);

  private native void nativeDestroyHashMap(long gpuHashMapPtr);

  private native boolean nativePut(long gpuHashMapPtr, byte[] key, byte[] value);

  private native byte[] nativeGet(long gpuHashMapPtr, byte[] key);

  private native boolean nativeRemove(long gpuHashMapPtr, byte[] key);

  // Instance-specific GPU resources
  private volatile long gpuContext = 0;
  private volatile long gpuHashMapPtr = 0;
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);

  // Configuration
  private final int gpuDeviceId;
  private final int initialHashMapCapacity;

  /**
   * Constructor for a version-specific GPU transformer
   * 
   * @param storeVersion The version this transformer instance is handling
   * @param gpuDeviceId GPU device to use
   * @param initialHashMapCapacity Initial capacity for the GPU hashmap
   */
  public GPUDaVinciRecordTransformer(
      int storeVersion,
      Schema keySchema,
      Schema inputValueSchema,
      Schema outputValueSchema,
      DaVinciRecordTransformerConfig recordTransformerConfig,
      int gpuDeviceId,
      int initialHashMapCapacity) {
    super(storeVersion, keySchema, inputValueSchema, outputValueSchema, recordTransformerConfig);
    this.gpuDeviceId = gpuDeviceId;
    this.initialHashMapCapacity = initialHashMapCapacity;

    LOGGER.info("Created GPU transformer for version: {} on GPU: {}", storeVersion, gpuDeviceId);
  }

  @Override
  public void onStartVersionIngestion(boolean isCurrentVersion) {
    initializeGPUResources();
  }

  /**
   * Initialize GPU resources, called once on ingestion start
   */
  private synchronized void initializeGPUResources() {
    if (isInitialized.get()) {
      // Don't do it twice, exit early
      return;
    }

    LOGGER.info("Initializing GPU resources for version: {}", this.getStoreVersion());

    // Initialize GPU context
    gpuContext = nativeInitializeGPU(gpuDeviceId);
    if (gpuContext == 0) {
      throw new RuntimeException("Failed to initialize GPU context for version: " + this.getStoreVersion());
    }

    // Create GPU hashmap
    gpuHashMapPtr = nativeCreateHashMap(gpuContext, initialHashMapCapacity);
    if (gpuHashMapPtr == 0) {
      nativeShutdownGPU(gpuContext);
      throw new RuntimeException("Failed to create GPU hashmap for version: " + this.getStoreVersion());
    }

    isInitialized.set(true);
    LOGGER.info(
        "GPU resources initialized successfully for version: {} with capacity: {}",
        this.getStoreVersion(),
        initialHashMapCapacity);
  }

  @Override
  public void processPut(Lazy<K> key, Lazy<O> value, int partitionId) {
    // Ensure GPU is initialized, this should already be the case as onStart is called before
    // any records are ingested
    if (!isInitialized.get()) {
      initializeGPUResources();
    }

    try {
      // Serialize key and value. We don't bother much with the parameterized types in this implementation yet
      // since we're just dealing with raw bytes in this prototype
      byte[] keyBytes = serializeObject(key.get());
      byte[] valueBytes = serializeObject(value.get());

      // Store in GPU
      if (!nativePut(gpuHashMapPtr, keyBytes, valueBytes)) {
        LOGGER.error("Failed to store record in GPU for key: {} in version: {}", key.get(), this.getStoreVersion());
      }
    } catch (Exception e) {
      LOGGER.error("Error processing record for version: {}", this.getStoreVersion(), e);
    }
  }

  /**
   * Retrieve a value from GPU memory
   *
   * TODO: Right now this api just works off of raw bytes, but deserialization would
   *       would be nice.
   */
  public byte[] get(K key) {
    if (!isInitialized.get()) {
      return null;
    }

    try {
      byte[] keyBytes = serializeObject(key);
      return nativeGet(gpuHashMapPtr, keyBytes);
    } catch (Exception e) {
      LOGGER.error("Error retrieving value from GPU for version: {}", this.getStoreVersion(), e);
    }

    return null;
  }

  @Override
  public void processDelete(Lazy<K> key, int partitionId) {
    if (!isInitialized.get()) {
      return;
    }

    try {
      byte[] keyBytes = serializeObject(key);
      nativeRemove(gpuHashMapPtr, keyBytes);
    } catch (Exception e) {
      // TODO: Unsure if we should throw the exception here or not
      LOGGER.error("Error removing key from GPU for version: {}", this.getStoreVersion(), e);
    }
  }

  @Override
  public DaVinciRecordTransformerResult<O> transform(Lazy<K> key, Lazy<V> value, int partitionId) {
    // NoOp
    return new DaVinciRecordTransformerResult<>(DaVinciRecordTransformerResult.Result.UNCHANGED);
  }

  /**
   * Cleanup GPU resources when the ingestion task ends.
   */
  @Override
  public void onEndVersionIngestion(int currentVersion) {
    try {
      close();
    } catch (IOException e) {
      throw new VeniceException(e);
    }
  }

  // TODO: This is a hack. Ideally, an implementation of this
  // would take a argument for selecting the column from key/value for retrieval
  // and storage on the gpu (like a field which holds an embedding).
  private byte[] serializeObject(Object obj) {
    if (obj instanceof byte[]) {
      return (byte[]) obj;
    } else if (obj instanceof ByteBuffer) {
      ByteBuffer buffer = (ByteBuffer) obj;
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return bytes;
    } else if (obj instanceof String) {
      return ((String) obj).getBytes();
    } else {
      return obj.toString().getBytes();
    }
  }

  @Override
  public void close() throws IOException {
    if (!isInitialized.get()) {
      return;
    }

    LOGGER.info("Closing GPU transformer for version: {}", this.getStoreVersion());

    if (gpuHashMapPtr != 0) {
      nativeDestroyHashMap(gpuHashMapPtr);
      gpuHashMapPtr = 0;
    }

    if (gpuContext != 0) {
      nativeShutdownGPU(gpuContext);
      gpuContext = 0;
    }

    isInitialized.set(false);
    LOGGER.info("GPU transformer closed for version: {}", this.getStoreVersion());
  }

  public boolean isInitialized() {
    return isInitialized.get();
  }
}
