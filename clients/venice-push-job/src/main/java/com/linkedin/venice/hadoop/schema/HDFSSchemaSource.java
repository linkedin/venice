package com.linkedin.venice.hadoop.schema;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.schema.SchemaUtils;
import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.rmd.RmdVersionId;
import com.linkedin.venice.utils.Utils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
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
  private static final String SEPARATOR = "/";
  private static final String RMD_SCHEMA_PREFIX = "RMD";
  private static final String VALUE_SCHEMA_PREFIX = "VALUE";

  private final String storeName;
  private final FileSystem fs;
  private final Path rmdSchemaDir;
  private final Path valueSchemaDir;

  public HDFSSchemaSource(final String valueSchemaDir, final String rmdSchemaDir, final String storeName)
      throws IOException {
    Configuration conf = new Configuration();
    this.rmdSchemaDir = new Path(rmdSchemaDir);
    this.fs = this.rmdSchemaDir.getFileSystem(conf);
    if (!fs.exists(this.rmdSchemaDir)) {
      fs.mkdirs(this.rmdSchemaDir);
    }
    this.valueSchemaDir = new Path(valueSchemaDir);
    if (!fs.exists(this.valueSchemaDir)) {
      fs.mkdirs(this.valueSchemaDir);
    }

    this.storeName = storeName;
  }

  public HDFSSchemaSource(final String valueSchemaDir, final String rmdSchemaDir) throws IOException {
    this(valueSchemaDir, rmdSchemaDir, null);
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
      String schemaFileName = isRmdSchema
          ? rmdSchemaDir + SEPARATOR + schema.getRmdValueSchemaId() + UNDERSCORE + schema.getId()
          : valueSchemaDir + SEPARATOR + schema.getId();
      Path schemaPath = new Path(schemaFileName);
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
