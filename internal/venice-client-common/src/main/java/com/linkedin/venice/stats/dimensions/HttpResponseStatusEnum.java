package com.linkedin.venice.stats.dimensions;

import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.HashMap;
import java.util.Map;


/**
 * This is used to map all values of {@link HttpResponseStatus} to an Enum implementing
 * VeniceDimensionInterface to be able to use it in {@link com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum}
 * or other similar classes.
 */
public enum HttpResponseStatusEnum implements VeniceDimensionInterface {
  CONTINUE(HttpResponseStatus.CONTINUE), SWITCHING_PROTOCOLS(HttpResponseStatus.SWITCHING_PROTOCOLS),
  PROCESSING(HttpResponseStatus.PROCESSING), OK(HttpResponseStatus.OK), CREATED(HttpResponseStatus.CREATED),
  ACCEPTED(HttpResponseStatus.ACCEPTED),
  NON_AUTHORITATIVE_INFORMATION(HttpResponseStatus.NON_AUTHORITATIVE_INFORMATION),
  NO_CONTENT(HttpResponseStatus.NO_CONTENT), RESET_CONTENT(HttpResponseStatus.RESET_CONTENT),
  PARTIAL_CONTENT(HttpResponseStatus.PARTIAL_CONTENT), MULTI_STATUS(HttpResponseStatus.MULTI_STATUS),
  MULTIPLE_CHOICES(HttpResponseStatus.MULTIPLE_CHOICES), MOVED_PERMANENTLY(HttpResponseStatus.MOVED_PERMANENTLY),
  FOUND(HttpResponseStatus.FOUND), SEE_OTHER(HttpResponseStatus.SEE_OTHER),
  NOT_MODIFIED(HttpResponseStatus.NOT_MODIFIED), USE_PROXY(HttpResponseStatus.USE_PROXY),
  TEMPORARY_REDIRECT(HttpResponseStatus.TEMPORARY_REDIRECT), PERMANENT_REDIRECT(HttpResponseStatus.PERMANENT_REDIRECT),
  BAD_REQUEST(HttpResponseStatus.BAD_REQUEST), UNAUTHORIZED(HttpResponseStatus.UNAUTHORIZED),
  PAYMENT_REQUIRED(HttpResponseStatus.PAYMENT_REQUIRED), FORBIDDEN(HttpResponseStatus.FORBIDDEN),
  NOT_FOUND(HttpResponseStatus.NOT_FOUND), METHOD_NOT_ALLOWED(HttpResponseStatus.METHOD_NOT_ALLOWED),
  NOT_ACCEPTABLE(HttpResponseStatus.NOT_ACCEPTABLE),
  PROXY_AUTHENTICATION_REQUIRED(HttpResponseStatus.PROXY_AUTHENTICATION_REQUIRED),
  REQUEST_TIMEOUT(HttpResponseStatus.REQUEST_TIMEOUT), CONFLICT(HttpResponseStatus.CONFLICT),
  GONE(HttpResponseStatus.GONE), LENGTH_REQUIRED(HttpResponseStatus.LENGTH_REQUIRED),
  PRECONDITION_FAILED(HttpResponseStatus.PRECONDITION_FAILED),
  REQUEST_ENTITY_TOO_LARGE(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE),
  REQUEST_URI_TOO_LONG(HttpResponseStatus.REQUEST_URI_TOO_LONG),
  UNSUPPORTED_MEDIA_TYPE(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE),
  REQUESTED_RANGE_NOT_SATISFIABLE(HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE),
  EXPECTATION_FAILED(HttpResponseStatus.EXPECTATION_FAILED),
  MISDIRECTED_REQUEST(HttpResponseStatus.MISDIRECTED_REQUEST),
  UNPROCESSABLE_ENTITY(HttpResponseStatus.UNPROCESSABLE_ENTITY), LOCKED(HttpResponseStatus.LOCKED),
  FAILED_DEPENDENCY(HttpResponseStatus.FAILED_DEPENDENCY),
  UNORDERED_COLLECTION(HttpResponseStatus.UNORDERED_COLLECTION), UPGRADE_REQUIRED(HttpResponseStatus.UPGRADE_REQUIRED),
  PRECONDITION_REQUIRED(HttpResponseStatus.PRECONDITION_REQUIRED),
  TOO_MANY_REQUESTS(HttpResponseStatus.TOO_MANY_REQUESTS),
  REQUEST_HEADER_FIELDS_TOO_LARGE(HttpResponseStatus.REQUEST_HEADER_FIELDS_TOO_LARGE),
  INTERNAL_SERVER_ERROR(HttpResponseStatus.INTERNAL_SERVER_ERROR), NOT_IMPLEMENTED(HttpResponseStatus.NOT_IMPLEMENTED),
  BAD_GATEWAY(HttpResponseStatus.BAD_GATEWAY), SERVICE_UNAVAILABLE(HttpResponseStatus.SERVICE_UNAVAILABLE),
  GATEWAY_TIMEOUT(HttpResponseStatus.GATEWAY_TIMEOUT),
  HTTP_VERSION_NOT_SUPPORTED(HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED),
  VARIANT_ALSO_NEGOTIATES(HttpResponseStatus.VARIANT_ALSO_NEGOTIATES),
  INSUFFICIENT_STORAGE(HttpResponseStatus.INSUFFICIENT_STORAGE), NOT_EXTENDED(HttpResponseStatus.NOT_EXTENDED),
  NETWORK_AUTHENTICATION_REQUIRED(HttpResponseStatus.NETWORK_AUTHENTICATION_REQUIRED), UNKNOWN;

  private final String codeAsString;
  private final int codeAsInt;

  HttpResponseStatusEnum(HttpResponseStatus httpResponseStatus) {
    this.codeAsString = httpResponseStatus.codeAsText().toString();
    this.codeAsInt = httpResponseStatus.code();
  }

  HttpResponseStatusEnum() {
    this.codeAsString = "0";
    this.codeAsInt = 0;
  }

  private static final Map<Integer, HttpResponseStatusEnum> statusToEnumMap;

  static {
    statusToEnumMap = new HashMap<>();
    for (HttpResponseStatusEnum enumStatus: HttpResponseStatusEnum.values()) {
      statusToEnumMap.put(enumStatus.codeAsInt, enumStatus);
    }
  }

  public static HttpResponseStatusEnum transformHttpResponseStatusToHttpResponseStatusEnum(HttpResponseStatus status) {
    return statusToEnumMap.getOrDefault(status.code(), UNKNOWN);
  }

  /**
   * All the instances of this Enum should have the same dimension name.
   * Refer {@link VeniceDimensionInterface#getDimensionName()} for more details.
   */
  @Override
  public VeniceMetricsDimensions getDimensionName() {
    return VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
  }

  @Override
  public String getDimensionValue() {
    return codeAsString;
  }
}
