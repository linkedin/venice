package com.linkedin.venice.hadoop.exceptions;

import com.linkedin.venice.exceptions.VeniceException;

/**
 * Customized exception for non-existing key/value field error in Avro schema
 * in {@link com.linkedin.venice.hadoop.KafkaPushJob}
 */
public class VeniceSchemaFieldNotFoundException extends VeniceException {
    private String fieldName;

    public VeniceSchemaFieldNotFoundException(String fieldName, String message) {
        super(message);
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }
}
