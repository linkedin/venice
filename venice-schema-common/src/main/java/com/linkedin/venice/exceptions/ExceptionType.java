package com.linkedin.venice.exceptions;

/**
 * A list of exception types/causes that can be added to error responses for clients to programmatically react.
 */
public enum ExceptionType {
    INCORRECT_CONTROLLER,
    INVALID_SCHEMA,
    INVALID_CONFIG,
    STORE_NOT_FOUND,
    SCHEMA_NOT_FOUND,
    CONNECTION_ERROR,
    GENERAL_ERROR,
    BAD_REQUEST
}
