package com.linkedin.venice.hadoop.schema;

import com.google.common.base.Preconditions;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.schema.SchemaUtils;
import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.rmd.RmdVersionId;
import com.linkedin.venice.utils.Utils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * HDFSSchemaSource caches the Value schemes and RMD schemas for a given store on the HDFS and serve them during the Mapper
 * stage. It will make API calls on Venice controller to retrieve all RMD schemes and all value schemas.
 */
@NotThreadsafe
public class HDFSSchemaSource implements SchemaSource, AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(HDFSSchemaSource.class);
  private static final String UNDERSCORE = "_";
  private static final String RMD_SCHEMA_PREFIX = "RMD";
  private static final String VALUE_SCHEMA_PREFIX = "VALUE";

  private final String storeName;
  private final FileSystem fs;
  private final Path rmdSchemaDir;
  private final Path valueSchemaDir;

  // For non ETL flows (historical), the key schema is part of the push configuration and doesn't require caching
  private final boolean includeKeySchema;

  private final Path keySchemaDir;

  public HDFSSchemaSource(
      final Path valueSchemaDir,
      final Path rmdSchemaDir,
      final Path keySchemaDir,
      final String storeName) throws IOException {
    Configuration conf = new Configuration();
    this.rmdSchemaDir = rmdSchemaDir;
    // TODO: Consider either using global filesystem or remove per instance fs and infer filesystem from the path during
    // usage
    this.fs = this.rmdSchemaDir.getFileSystem(conf);
    this.valueSchemaDir = valueSchemaDir;
    this.keySchemaDir = keySchemaDir;
    this.includeKeySchema = keySchemaDir != null;
    this.storeName = storeName;

    initialize();
  }

  private void initialize() throws IOException {
    if (!fs.exists(this.rmdSchemaDir)) {
      fs.mkdirs(this.rmdSchemaDir);
    }

    if (!fs.exists(this.valueSchemaDir)) {
      fs.mkdirs(this.valueSchemaDir);
    }

    if (includeKeySchema && !fs.exists(this.keySchemaDir)) {
      fs.mkdirs(this.keySchemaDir);
    }
  }

  public HDFSSchemaSource(final Path valueSchemaDir, final Path rmdSchemaDir, final String storeName)
      throws IOException {
    this(valueSchemaDir, rmdSchemaDir, null, storeName);
  }

  public HDFSSchemaSource(final String valueSchemaDir, final String rmdSchemaDir, final String storeName)
      throws IOException {
    this(new Path(valueSchemaDir), new Path(rmdSchemaDir), storeName);
  }

  public HDFSSchemaSource(final String valueSchemaDir, final String rmdSchemaDir) throws IOException {
    this(new Path(valueSchemaDir), new Path(rmdSchemaDir), null);
  }

  public String getKeySchemaPath() {
    return keySchemaDir.toString();
  }

  public String getRmdSchemaPath() {
    return rmdSchemaDir.toString();
  }

  public String getValueSchemaPath() {
    return valueSchemaDir.toString();
  }

  /**
   * This method loads related configs, gets store value and RMD schemas from Venice controllers, and then write them on HDFS.
   * @throws IOException
   * @throws IllegalStateException
   */
  public void saveSchemasOnDisk(ControllerClient controllerClient) throws IOException, IllegalStateException {
    saveSchemaResponseToDisk(controllerClient.getAllReplicationMetadataSchemas(storeName).getSchemas(), true);
    saveSchemaResponseToDisk(controllerClient.getAllValueSchema(storeName).getSchemas(), false);

    if (includeKeySchema) {
      saveKeySchemaToDisk(controllerClient.getKeySchema(storeName));
    }
  }

  void saveSchemaResponseToDisk(MultiSchemaResponse.Schema[] schemas, boolean isRmdSchema)
      throws IOException, IllegalStateException {
    LOGGER.info(
        "Starting caching {} schemas for store: {} at HDFS path: {}",
        isRmdSchema ? RMD_SCHEMA_PREFIX : VALUE_SCHEMA_PREFIX,
        storeName,
        rmdSchemaDir.toString());
    for (MultiSchemaResponse.Schema schema: schemas) {
      // Path for RMD schema is /<rmdSchemaDir>/<valueSchemaId>_<rmdSchemaId>
      // Path for Value schema is /<valueSchemaDir>/<valueSchemaId>
      Path schemaPath = isRmdSchema
          ? new Path(rmdSchemaDir, schema.getRmdValueSchemaId() + UNDERSCORE + schema.getId())
          : new Path(valueSchemaDir, String.valueOf(schema.getId()));
      if (!fs.exists(schemaPath)) {
        try (FSDataOutputStream outputStream = fs.create(schemaPath);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
          Schema originalSchema = AvroSchemaParseUtils.parseSchemaFromJSON(schema.getSchemaStr(), false);
          Schema annotatedSchema = isRmdSchema
              ? SchemaUtils.annotateRmdSchema(originalSchema)
              : SchemaUtils.annotateValueSchema(originalSchema);
          outputStreamWriter.write(annotatedSchema.toString() + "\n");
          outputStreamWriter.flush();
        }
        LOGGER.info(
            "Finished writing schema {} onto disk",
            isRmdSchema ? schema.getRmdValueSchemaId() + "-" + schema.getId() : schema.getId());
      } else {
        throw new IllegalStateException(String.format("The schema path %s already exists.", schemaPath));
      }
    }
  }

  void saveKeySchemaToDisk(SchemaResponse schema) throws IOException, IllegalStateException {
    Preconditions.checkState(includeKeySchema, "Cannot be invoked with invalid key schema directory");
    Preconditions.checkState(!schema.isError(), "Cannot persist schema. Encountered error in schema response");
    LOGGER.info("Caching key schema for store: {} in {}", storeName, keySchemaDir.getName());

    Path schemaPath = new Path(keySchemaDir, String.valueOf(schema.getId()));
    if (!fs.exists(schemaPath)) {
      try (FSDataOutputStream outputStream = fs.create(schemaPath);
          OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
        outputStreamWriter.write(schema.getSchemaStr() + "\n");
        outputStreamWriter.flush();
      }
    } else {
      throw new IllegalStateException(String.format("The schema path %s already exists", schemaPath));
    }
  }

  /**
   * Fetch all rmd schemas under the {@link #rmdSchemaDir} if available.
   *
   * @return RMD schemas mapping
   * @throws IOException
   */
  @Override
  public Map<RmdVersionId, Schema> fetchRmdSchemas() throws IOException {
    Map<RmdVersionId, Schema> mapping = new HashMap<>();
    FileStatus[] fileStatus = fs.listStatus(rmdSchemaDir);
    LOGGER.info("Starting fetching RMD schemas at {}", rmdSchemaDir.toString());
    for (FileStatus status: fileStatus) {
      Path path = status.getPath();
      try (FSDataInputStream in = fs.open(path);
          BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        RmdVersionId pair = parseRmdSchemaIdsFromPath(path);
        String schemeStr = reader.readLine();
        if (schemeStr == null) {
          throw new RuntimeException(String.format("Failed to load RMD schema at the path %s", path));
        }
        mapping.put(pair, AvroCompatibilityHelper.parse(schemeStr));
      }
    }
    return mapping;
  }

  /**
   * Fetch all value schemas under the {@link #rmdSchemaDir} if available.
   *
   * @return RMD schemas mapping
   * @throws IOException
   */
  @Override
  public Map<Integer, Schema> fetchValueSchemas() throws IOException {
    Map<Integer, Schema> mapping = new HashMap<>();
    FileStatus[] fileStatus = fs.listStatus(valueSchemaDir);
    LOGGER.info("Starting fetching value schemas at {}", valueSchemaDir.toString());
    for (FileStatus status: fileStatus) {
      Path path = status.getPath();
      try (FSDataInputStream in = fs.open(path);
          BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        int valueSchemaId = parseValueSchemaIdsFromPath(path);
        String schemeStr = reader.readLine();
        if (schemeStr == null) {
          throw new RuntimeException(String.format("Failed to load value schema at the path %s", path));
        }
        mapping.put(valueSchemaId, AvroCompatibilityHelper.parse(schemeStr));
      }
    }
    return mapping;
  }

  @Override
  public Schema fetchKeySchema() throws IOException {
    Preconditions.checkState(includeKeySchema, "Cannot be invoked with invalid key schema directory");
    FileStatus[] fileStatus = fs.listStatus(keySchemaDir);
    LOGGER.info("Fetching key schemas from :{}", keySchemaDir);

    Optional<Schema> keySchema = Arrays.stream(fileStatus).map(status -> {
      Path path = status.getPath();
      Schema outputSchema = null;
      try (FSDataInputStream in = fs.open(path);
          BufferedReader reader = new BufferedReader((new InputStreamReader(in, StandardCharsets.UTF_8)))) {
        String schemaStr = reader.readLine();
        if (schemaStr != null) {
          outputSchema = Schema.parse(schemaStr.trim());
        }
      } catch (Exception e) {
        LOGGER.error("Failed to fetch key schema due to ", e);
      }

      return outputSchema;
    }).findFirst();

    return keySchema
        .orElseThrow(() -> new RuntimeException(String.format("Failed to load key schema from %s", keySchemaDir)));
  }

  @Override
  public void close() {
    Utils.closeQuietlyWithErrorLogged(fs);
  }

  /**
   * This method assert and parse the final component of this path should be <id>_<valueSchemaId>.
   * @param path
   * @return
   */
  private RmdVersionId parseRmdSchemaIdsFromPath(Path path) {
    String last = path.getName();
    String[] pair = last.split(UNDERSCORE);
    int valueSchemaId = Integer.parseInt(pair[0]);
    int rmdProtocolId = Integer.parseInt(pair[1]);
    return new RmdVersionId(valueSchemaId, rmdProtocolId);
  }

  private int parseValueSchemaIdsFromPath(Path path) {
    return Integer.parseInt(path.getName());
  }
}
