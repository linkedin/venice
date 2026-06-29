package com.linkedin.venice.exceptions;

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;
import org.apache.http.HttpStatus;


/**
 * {@link ExceptionType} enum did not have a default deserialization annotation and this makes it non-evolvable till all
 * clients upgrade to newer versions. This temporary enum is created to have an evolvable ExceptionType while mapping
 * new types to previously existing ExceptionTypes.
 *
 * Each error type also carries the HTTP status code that should be returned when it surfaces in an HTTP response. This
 * lets {@link VeniceException#getHttpStatusCode()} derive the status code from the error type instead of every exception
 * subclass overriding the method, so new error types are handled consistently without missing the mapping.
 */
@SuppressWarnings("deprecation")
public enum ErrorType {
  INCORRECT_CONTROLLER(ExceptionType.INCORRECT_CONTROLLER, HttpStatus.SC_INTERNAL_SERVER_ERROR),
  INVALID_SCHEMA(ExceptionType.INVALID_SCHEMA, HttpStatus.SC_BAD_REQUEST),
  INVALID_CONFIG(ExceptionType.INVALID_CONFIG, HttpStatus.SC_INTERNAL_SERVER_ERROR),
  STORE_NOT_FOUND(ExceptionType.STORE_NOT_FOUND, HttpStatus.SC_NOT_FOUND),
  SCHEMA_NOT_FOUND(ExceptionType.SCHEMA_NOT_FOUND, HttpStatus.SC_NOT_FOUND),
  CONNECTION_ERROR(ExceptionType.CONNECTION_ERROR, HttpStatus.SC_INTERNAL_SERVER_ERROR), @JsonEnumDefaultValue
  GENERAL_ERROR(ExceptionType.GENERAL_ERROR, HttpStatus.SC_INTERNAL_SERVER_ERROR),
  BAD_REQUEST(ExceptionType.BAD_REQUEST, HttpStatus.SC_BAD_REQUEST),
  CONCURRENT_BATCH_PUSH(ExceptionType.BAD_REQUEST, HttpStatus.SC_BAD_REQUEST),
  RESOURCE_STILL_EXISTS(ExceptionType.BAD_REQUEST, HttpStatus.SC_PRECONDITION_FAILED),
  PROTOCOL_ERROR(ExceptionType.BAD_REQUEST, HttpStatus.SC_INTERNAL_SERVER_ERROR),
  ACL_ERROR(ExceptionType.BAD_REQUEST, HttpStatus.SC_FORBIDDEN),
  STORE_DISABLED(ExceptionType.BAD_REQUEST, HttpStatus.SC_FORBIDDEN),;

  private final ExceptionType exceptionType;
  private final int httpStatusCode;

  ErrorType(ExceptionType type, int httpStatusCode) {
    this.exceptionType = type;
    this.httpStatusCode = httpStatusCode;
  }

  public ExceptionType getExceptionType() {
    return exceptionType;
  }

  public int getHttpStatusCode() {
    return httpStatusCode;
  }
}
