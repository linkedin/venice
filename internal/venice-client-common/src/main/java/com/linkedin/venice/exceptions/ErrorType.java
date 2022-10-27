package com.linkedin.venice.exceptions;

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;


/**
 * {@link ExceptionType} enum did not have a default deserialization annotation and this makes it non-evolvable till all
 * clients upgrade to newer versions. This temporary enum is created to have an evolvable ExceptionType while mapping
 * new types to previously existing ExceptionTypes.
 */
@SuppressWarnings("deprecation")
public enum ErrorType {
  INCORRECT_CONTROLLER(ExceptionType.INCORRECT_CONTROLLER), INVALID_SCHEMA(ExceptionType.INVALID_SCHEMA),
  INVALID_CONFIG(ExceptionType.INVALID_CONFIG), STORE_NOT_FOUND(ExceptionType.STORE_NOT_FOUND),
  SCHEMA_NOT_FOUND(ExceptionType.SCHEMA_NOT_FOUND), CONNECTION_ERROR(ExceptionType.CONNECTION_ERROR),
  @JsonEnumDefaultValue
  GENERAL_ERROR(ExceptionType.GENERAL_ERROR), BAD_REQUEST(ExceptionType.BAD_REQUEST),
  CONCURRENT_BATCH_PUSH(ExceptionType.BAD_REQUEST), RESOURCE_STILL_EXISTS(ExceptionType.BAD_REQUEST);

  private final ExceptionType exceptionType;

  ErrorType(ExceptionType type) {
    this.exceptionType = type;
  }

  public ExceptionType getExceptionType() {
    return exceptionType;
  }
}
