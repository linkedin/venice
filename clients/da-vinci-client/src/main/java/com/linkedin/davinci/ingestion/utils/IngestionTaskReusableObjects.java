package com.linkedin.davinci.ingestion.utils;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.nio.ByteBuffer;
import java.util.function.Supplier;
import org.apache.avro.io.BinaryDecoder;


public class IngestionTaskReusableObjects {
  private static final ThreadLocal<IngestionTaskReusableObjects> SINGLETON =
      ThreadLocal.withInitial(IngestionTaskReusableObjects::new);
  private static final IngestionTaskReusableObjects NO_REUSE_SINGLETON = new IngestionTaskReusableObjects(null, null);
  private static final byte[] BINARY_DECODER_PARAM = new byte[16];

  private final ByteBuffer reusedByteBuffer;
  private final BinaryDecoder binaryDecoder;

  private IngestionTaskReusableObjects(ByteBuffer reusedByteBuffer, BinaryDecoder binaryDecoder) {
    this.reusedByteBuffer = reusedByteBuffer;
    this.binaryDecoder = binaryDecoder;
  }

  private IngestionTaskReusableObjects() {
    this(
        ByteBuffer.allocate(1024 * 1024),
        AvroCompatibilityHelper.newBinaryDecoder(BINARY_DECODER_PARAM, 0, BINARY_DECODER_PARAM.length, null));
  }

  public BinaryDecoder getBinaryDecoder() {
    return binaryDecoder;
  }

  public ByteBuffer getReusedByteBuffer() {
    return reusedByteBuffer;
  }

  public enum Strategy {
    /**
     * No re-use, will pass null into the relevant code paths, which will result in new objects getting allocated on the
     * fly, and needing to be garbage collected.
     */
    NO_REUSE(() -> IngestionTaskReusableObjects.NO_REUSE_SINGLETON),

    /**
     * Thread-local per {@link com.linkedin.davinci.kafka.consumer.ActiveActiveStoreIngestionTask}. This avoids hot path
     * allocations but is likely not very efficient, since there can be many AASIT and each will have its own
     * independent set of objects, even if/when the AASIT becomes dormant. This is the first mode which was built, and
     * it is considered stable.
     */
    THREAD_LOCAL_PER_INGESTION_TASK(new Supplier<IngestionTaskReusableObjects>() {
      private final ThreadLocal<IngestionTaskReusableObjects> o =
          ThreadLocal.withInitial(IngestionTaskReusableObjects::new);

      @Override
      public IngestionTaskReusableObjects get() {
        return o.get();
      }
    }),

    /**
     * Thread-local per JVM. Likely the most efficient.
     */
    SINGLETON_THREAD_LOCAL(() -> IngestionTaskReusableObjects.SINGLETON.get());

    private final Supplier<IngestionTaskReusableObjects> supplier;

    Strategy(Supplier<IngestionTaskReusableObjects> supplier) {
      this.supplier = supplier;
    }

    public Supplier<IngestionTaskReusableObjects> supplier() {
      return this.supplier;
    }
  }
}
