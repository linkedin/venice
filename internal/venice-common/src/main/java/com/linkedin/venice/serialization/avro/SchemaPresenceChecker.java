package com.linkedin.venice.serialization.avro;

import static com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer.MAX_ATTEMPTS_FOR_SCHEMA_READER;
import static com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer.WAIT_TIME_BETWEEN_SCHEMA_READER_ATTEMPTS_IN_MS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.utils.Utils;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class helps checks presence of a schema version in ZK. This can be used to ensure forward compatibility when a
 * protocol gets upgraded.
 */
public class SchemaPresenceChecker {
  private static final Logger LOGGER = LogManager.getLogger(SchemaPresenceChecker.class);

  /** Used to fetch and verify a schema version is present in ZK. */
  private SchemaReader schemaReader = null;

  private final AvroProtocolDefinition avroProtocolDefinition;

  public SchemaPresenceChecker(SchemaReader schemaReader, AvroProtocolDefinition avroProtocolDefinition) {
    this.schemaReader = schemaReader;
    this.avroProtocolDefinition = avroProtocolDefinition;
  }

  private void verifySchemaIsPresent(int protocolVersion, boolean retry) {
    for (int attempt = 1; attempt <= MAX_ATTEMPTS_FOR_SCHEMA_READER; attempt++) {
      try {
        Schema newProtocolSchema = schemaReader.getValueSchema(protocolVersion);
        if (newProtocolSchema == null) {
          throw new VeniceMessageException(
              "Failed to retrieve protocol version '" + protocolVersion + "' with remote fetch using "
                  + SchemaReader.class.getSimpleName() + " for " + avroProtocolDefinition.name());
        }
        LOGGER.info(
            "Discovered new protocol version: {}, and successfully retrieved it for protocol: {}",
            protocolVersion,
            avroProtocolDefinition.name());
        LOGGER.debug("Schema: {}", newProtocolSchema.toString(true));
        break;
      } catch (Exception e) {
        if (attempt == MAX_ATTEMPTS_FOR_SCHEMA_READER || !retry) {
          throw new VeniceException(
              "Failed to retrieve new protocol schema version (" + protocolVersion + ") after "
                  + MAX_ATTEMPTS_FOR_SCHEMA_READER + " attempts.",
              e);
        }
        LOGGER.error(
            "Caught an exception while trying to fetch a new protocol schema version (protocol: {}, version: {})."
                + " Attempt #{}/{}. Will sleep {} ms and try again.",
            avroProtocolDefinition.name(),
            protocolVersion,
            attempt,
            MAX_ATTEMPTS_FOR_SCHEMA_READER,
            WAIT_TIME_BETWEEN_SCHEMA_READER_ATTEMPTS_IN_MS,
            e);
        Utils.sleep(WAIT_TIME_BETWEEN_SCHEMA_READER_ATTEMPTS_IN_MS);
      }
    }
  }

  /**
   * When schema is present this returns nothing, otherwise it throws an exception and exits the JVM.
   * This is added for testing purpose.
   * @param protocolVersion -- the protocol version to fetch the schema for, if not present then fetch the current version.
   * @param retry -- should retry fetching the schema or not
   */
  public void verifySchemaVersionPresentOrExit(Optional<Integer> protocolVersion, boolean retry) {
    int version =
        protocolVersion.isPresent() ? protocolVersion.get() : avroProtocolDefinition.getCurrentProtocolVersion();
    try {
      verifySchemaIsPresent(version, retry);
      LOGGER.info(
          "SchemaPresenceChecker: The schema {} current version {} is found",
          avroProtocolDefinition.name(),
          version);
    } catch (VeniceException e) {
      String errorMsg = "SchemaVersionNotFound: The schema " + avroProtocolDefinition.name() + " current version "
          + version + " is not present in ZK, exiting application";
      LOGGER.fatal(errorMsg);
      throw new VeniceException(errorMsg, e);
    }
  }

  public void verifySchemaVersionPresentOrExit() {
    verifySchemaVersionPresentOrExit(Optional.empty(), true);
  }
}
