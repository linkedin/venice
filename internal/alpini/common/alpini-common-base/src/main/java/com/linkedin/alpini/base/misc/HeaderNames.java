package com.linkedin.alpini.base.misc;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public enum HeaderNames {
  SINGLETON; // Effective Java, Item 3 - Enum singleton

  public static final String PROVIDER_CLASS_NAME = "com.linkedin.alpini.base.misc.spi.HeaderNames";

  public static final String CONTENT_LOCATION = "Content-Location";
  public static final String CONTENT_TYPE = "Content-Type";
  public static final String CONTENT_LENGTH = "Content-Length";
  public static final String SERVER = "Server";

  public static final String X_CLUSTER_NAME;
  public static final String X_ERROR_CLASS;
  public static final String X_ERROR_MESSAGE;
  public static final String X_ERROR_CAUSE_CLASS;
  public static final String X_ERROR_CAUSE_MESSAGE;
  // ESPENG-4693 Used to indicate that a response part definitely contains an error.
  // Absence of this header is not an indicator of absence of errors.
  public static final String X_ERROR_IN_RESPONSE;
  public static final String X_METRICS;
  public static final String X_MULTIPART_CONTENT_STATUS;
  public static final String X_PARTITION;
  public static final String X_REQUEST_ID;
  public static final String X_RETURN_METRICS;
  public static final String X_SERVED_BY;
  public static final String X_USE_RESPONSE_BOUNDARY;

  public interface HeaderNamesProvider {
  }

  @Retention(RetentionPolicy.RUNTIME)
  public @interface XClusterName {
    String value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  public @interface XErrorClass {
    String value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  public @interface XErrorMessage {
    String value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  public @interface XErrorCauseClass {
    String value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  public @interface XErrorCauseMessage {
    String value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  public @interface XErrorInResponse {
    String value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  public @interface XMetrics {
    String value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  public @interface XMultipartContentStatus {
    String value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  public @interface XPartition {
    String value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  public @interface XRequestId {
    String value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  public @interface XReturnMetrics {
    String value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  public @interface XServedBy {
    String value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  public @interface XResponseBoundary {
    String value();
  }

  private static <T extends Annotation> T get(Class<? extends HeaderNamesProvider> provider, Class<T> annotation) {
    T value = provider.getDeclaredAnnotation(annotation);
    if (value == null) {
      value = DefaultNames.class.getDeclaredAnnotation(annotation);
    }
    return value;
  }

  @XClusterName("X-Cluster-Name")
  @XErrorClass("X-Error-Class")
  @XErrorMessage("X-Error-Message")
  @XErrorCauseClass("X-Error-Cause-Class")
  @XErrorCauseMessage("X-Error-Cause-Message")
  @XErrorInResponse("X-Error-In-Response")
  @XMetrics("X-Metrics")
  @XMultipartContentStatus("X-Content-Status")
  @XPartition("X-Partition")
  @XRequestId("X-Request-ID")
  @XReturnMetrics("X-Return-Metrics")
  @XServedBy("X-Served-by")
  @XResponseBoundary("X-Use-Response-Boundary")
  private static class DefaultNames implements HeaderNamesProvider {
  }

  static {
    Class<? extends HeaderNamesProvider> providerClz;

    try {
      ClassLoader clzLoader = Thread.currentThread().getContextClassLoader();

      if (clzLoader == null) {
        clzLoader = ClassLoader.getSystemClassLoader();
      }

      Class<?> clz = Class.forName(PROVIDER_CLASS_NAME, false, clzLoader);

      providerClz = clz.asSubclass(HeaderNamesProvider.class);
    } catch (Throwable ignored) {
      providerClz = DefaultNames.class;
    }

    X_CLUSTER_NAME = get(providerClz, XClusterName.class).value();
    X_ERROR_CLASS = get(providerClz, XErrorClass.class).value();
    X_ERROR_MESSAGE = get(providerClz, XErrorMessage.class).value();
    X_ERROR_CAUSE_CLASS = get(providerClz, XErrorCauseClass.class).value();
    X_ERROR_CAUSE_MESSAGE = get(providerClz, XErrorCauseMessage.class).value();
    X_ERROR_IN_RESPONSE = get(providerClz, XErrorInResponse.class).value();
    X_METRICS = get(providerClz, XMetrics.class).value();
    X_MULTIPART_CONTENT_STATUS = get(providerClz, XMultipartContentStatus.class).value();
    X_PARTITION = get(providerClz, XPartition.class).value();
    X_REQUEST_ID = get(providerClz, XRequestId.class).value();
    X_RETURN_METRICS = get(providerClz, XReturnMetrics.class).value();
    X_SERVED_BY = get(providerClz, XServedBy.class).value();
    X_USE_RESPONSE_BOUNDARY = get(providerClz, XResponseBoundary.class).value();
  }
}
