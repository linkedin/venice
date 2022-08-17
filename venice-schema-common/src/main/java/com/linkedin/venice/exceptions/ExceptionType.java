package com.linkedin.venice.exceptions;

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;


/**
 * A list of exception types/causes that can be added to error responses for clients to programmatically react.
 *
 * @deprecated since v0.2.620
 * Previously, this enum didn't have the {@literal @JsonEnumDefaultValue} annotation. This annotation helps Jackson
 * deserialize arbitrary values to a default value instead of throwing an Exception. Without this annotation, the enum
 * is essentially non-evolvable since there may be many clients that are already running older code.
 * Use {@link ErrorType} instead.
 */
@Deprecated
public enum ExceptionType {
  INCORRECT_CONTROLLER, INVALID_SCHEMA, INVALID_CONFIG, STORE_NOT_FOUND, SCHEMA_NOT_FOUND, CONNECTION_ERROR,
  @JsonEnumDefaultValue
  GENERAL_ERROR, BAD_REQUEST
}
