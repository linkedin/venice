package com.linkedin.venice.etl.source;

import com.linkedin.venice.etl.extractor.VeniceKafkaExtractor;
import com.linkedin.venice.etl.schema.HDFSSchemaSource;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaSource;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source is responsible for getting the lastest version number and the key/value schemas of the store and
 * create a list of WorkUnits for ETL.
 */
public class VeniceKafkaSource extends KafkaSource<Schema, GenericRecord> {
  private static final Logger logger = LoggerFactory.getLogger(VeniceKafkaSource.class);

  public static final String VENICE_CONTROLLER_URLS = "venice.urls";
  public static final String VENICE_STORE_NAME = "venice.store.name";
  public static final String FABRIC_NAME = "fabric.name";
  public static final String KAFKA_BOOSTRAP_SERVERS = "kafka.bootstrap.servers";
  public static final String VENICE_STORE_NAME_SEPARATOR = ",";
  public static final String SCHEMA_DIRECTORY = "schema.directory";

  public static final String VENICE_ETL_KEY_FIELD = "key";
  public static final String VENICE_ETL_VALUE_FIELD = "value";
  public static final String VENICE_ETL_OFFSET_FIELD = "offset";
  public static final String VENICE_ETL_DELETED_TS_FIELD = "DELETED_TS";
  public static final String VENICE_ETL_METADATA_FIELD = "metadata";
  public static final String VENICE_ETL_SCHEMAID_FIELD = "schemaId";

  private String schemaDir;


  /**
   * The major method for VeniceSource; generate a list of work units.
   * @param state A container of all metadata including all configs from ETL job config files and the states from
   *              the last ETL run like the last offset of a Kafka partition consumed in the last ETL.
   * @return A list of WorkUnit; basically, one WorkUnit extracts message from one Kafka partition.
   */
  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = super.getWorkunits(state);

    try {
      schemaDir = state.getProp(SCHEMA_DIRECTORY);
      if (schemaDir.endsWith("/")) {
        schemaDir = schemaDir.substring(0, schemaDir.length() - 1);
      }
      HDFSSchemaSource hdfsSchemaSource = new HDFSSchemaSource(schemaDir);
      hdfsSchemaSource.load(state);
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }

    return workUnits;
  }

  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) {
    return new VeniceKafkaExtractor(state);
  }
}
