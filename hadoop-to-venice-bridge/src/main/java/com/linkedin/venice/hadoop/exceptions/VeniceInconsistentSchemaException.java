package com.linkedin.venice.hadoop.exceptions;

import com.linkedin.venice.exceptions.VeniceException;

/**
 * Customized exception for inconsistent Avro schema error of input directory
 * in {@link com.linkedin.venice.hadoop.KafkaPushJob}
 */
public class VeniceInconsistentSchemaException extends VeniceException {
    public VeniceInconsistentSchemaException(String message) {
        super(message);
    }

    public VeniceInconsistentSchemaException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
