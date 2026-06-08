package com.linkedin.venice.vpj;

import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_EXTERNAL_STORAGE_WRITER_CLASS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.task.datawriter.ExternalStorageWriter;
import com.linkedin.venice.meta.StorageMode;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;


/**
 * Helpers for the VPJ external-storage dual-write path: the gating predicate, the typed reflective loader
 * for {@link ExternalStorageWriter} impls, and the load+configure helper that ensures the impl is closed
 * if configuration fails.
 */
public final class ExternalStorageWriteUtils {
  private ExternalStorageWriteUtils() {
  }

  /**
   * Returns true iff both halves of the dual-write gate are satisfied:
   * <ul>
   *   <li>The VPJ-side {@code push.job.external.storage.writer.class} config is set to a non-empty value
   *       (i.e. an implementation is wired in for this push).</li>
   *   <li>The store-version-side {@code targetStorageMode} is {@link StorageMode#DUAL_WRITE} (i.e. the
   *       version being pushed to has opted into dual-write at the store level).</li>
   * </ul>
   * Either half missing means the path stays off and the partition writer falls back to today's Kafka-only
   * produce.
   */
  public static boolean isDualWriteToExternalStorageFromVpjEnabled(
      VeniceProperties jobProps,
      StorageMode targetStorageMode) {
    if (jobProps == null || targetStorageMode == null) {
      return false;
    }
    if (jobProps.getString(PUSH_JOB_EXTERNAL_STORAGE_WRITER_CLASS, "").isEmpty()) {
      return false;
    }
    return targetStorageMode == StorageMode.DUAL_WRITE;
  }

  /**
   * Reflectively load and instantiate an {@link ExternalStorageWriter} implementation, validating that the
   * configured class actually implements the interface before invoking its no-arg constructor.
   *
   * @throws VeniceException with a precise message when the class cannot be loaded, does not implement the
   *           SPI, or cannot be instantiated. Wrapping the underlying causes keeps stack traces useful while
   *           surfacing the configured class name in the message for operators triaging mis-configured pushes.
   */
  public static ExternalStorageWriter loadExternalStorageWriter(String className) {
    Class<?> loadedClass;
    try {
      loadedClass = ReflectUtils.loadClass(className);
    } catch (Exception e) {
      throw new VeniceException(
          "Failed to load " + ExternalStorageWriter.class.getSimpleName() + " class '" + className + "'",
          e);
    }
    if (!ExternalStorageWriter.class.isAssignableFrom(loadedClass)) {
      throw new VeniceException(
          "Configured class '" + className + "' does not implement " + ExternalStorageWriter.class.getName());
    }
    try {
      @SuppressWarnings("unchecked")
      Class<ExternalStorageWriter> typedClass = (Class<ExternalStorageWriter>) loadedClass;
      return ReflectUtils.callConstructor(typedClass, new Class<?>[0], new Object[0]);
    } catch (Exception e) {
      throw new VeniceException(
          "Failed to instantiate " + ExternalStorageWriter.class.getSimpleName() + " '" + className + "'",
          e);
    }
  }

  /**
   * Load the configured {@link ExternalStorageWriter} and invoke {@link ExternalStorageWriter#configure}.
   * If {@code configure} throws, the partially-constructed writer is closed (best-effort) before the
   * exception is propagated, so executor tasks do not leak connection pools / file handles / etc.
   */
  public static ExternalStorageWriter loadAndConfigure(
      String className,
      VeniceProperties jobProps,
      String topicName,
      int partitionId) {
    ExternalStorageWriter writer = loadExternalStorageWriter(className);
    try {
      writer.configure(jobProps, topicName, partitionId);
      return writer;
    } catch (RuntimeException e) {
      Utils.closeQuietlyWithErrorLogged(writer);
      throw e;
    }
  }
}
