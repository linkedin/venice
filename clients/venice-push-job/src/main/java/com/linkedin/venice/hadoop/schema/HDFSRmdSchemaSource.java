package com.linkedin.venice.hadoop.schema;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
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
 * HDFSRmdSchemaSource caches the RMD schemes for a given store on the HDFS and serve them during the Mapper stage
 * It will make API calls on Venice controller to retrieve all RMD schemes.
 * <p>
 * Borrowed some codes from {@link HDFSSchemaSource}. The difference is HDFSRmdSchemaSource is created for a single store.
 */
@NotThreadsafe
public class HDFSRmdSchemaSource implements RmdSchemaSource, AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(HDFSRmdSchemaSource.class);
  private static final String UNDERSCORE = "_";
  private static final String SEPARATOR = "/";

  private final String storeName;
  private final FileSystem fs;
  private final Path schemaDir;

  public HDFSRmdSchemaSource(final String schemaDir, final String storeName) throws IOException {
    Configuration conf = new Configuration();
    this.fs = FileSystem.get(conf);
    this.schemaDir = new Path(schemaDir);
    if (!fs.exists(this.schemaDir)) {
      fs.mkdirs(this.schemaDir);
    }
    this.storeName = storeName;
  }

  public HDFSRmdSchemaSource(final String schemaDirSuffix) throws IOException {
    this(schemaDirSuffix, null);
  }

  public String getPath() {
    return schemaDir.toString();
  }

  /**
   * This method loads related configs, gets store RMD schemas from Venice controllers, and then write them on HDFS.
   * @throws IOException
   * @throws IllegalStateException
   */
  public void loadRmdSchemasOnDisk(ControllerClient controllerClient) throws IOException, IllegalStateException {
    LOGGER.info("Starting caching RMD schemas for {} at {}", storeName, schemaDir.toString());
    MultiSchemaResponse.Schema[] schemas = controllerClient.getAllReplicationMetadataSchemas(storeName).getSchemas();
    for (MultiSchemaResponse.Schema schema: schemas) {
      // path for rmd schema is /<schemaDir>/<id>_<valueSchemaId>
      Path schemaPath = new Path(schemaDir + SEPARATOR + schema.getId() + UNDERSCORE + schema.getRmdValueSchemaId());
      if (!fs.exists(schemaPath)) {
        try (FSDataOutputStream outputStream = fs.create(schemaPath);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
          outputStreamWriter.write(schema.getSchemaStr() + "\n");
          outputStreamWriter.flush();
        }
        LOGGER.info(
            "Finished writing RMD schema with id {} and RMD value schema id {} onto disk",
            schema.getId(),
            schema.getRmdValueSchemaId());
      } else {
        throw new IllegalStateException(String.format("The schema path %s already exists.", schemaPath));
      }
    }
  }

  /**
   * Fetch all schemas under the {@link #schemaDir} if available.
   *
   * @return RMD schemas mapping
   * @throws IOException
   */
  @Override
  public Map<RmdVersionId, Schema> fetchSchemas() throws IOException {
    Map<RmdVersionId, Schema> mapping = new HashMap<>();
    FileStatus[] fileStatus = fs.listStatus(schemaDir);
    LOGGER.info("Starting fetching RMD schemas at {}", schemaDir.toString());
    for (FileStatus status: fileStatus) {
      Path path = status.getPath();
      try (FSDataInputStream in = fs.open(path);
          BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        RmdVersionId pair = parseIdsFromPath(path);
        String schemeStr = reader.readLine();
        if (schemeStr == null) {
          throw new RuntimeException(String.format("Failed to load RMD schema at the path %s", path));
        }
        mapping.put(pair, AvroCompatibilityHelper.parse(schemeStr));
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
  private RmdVersionId parseIdsFromPath(Path path) {
    String last = path.getName();
    String[] pair = last.split(UNDERSCORE);
    int id = Integer.parseInt(pair[0]), valueSchemaId = Integer.parseInt(pair[1]);
    return new RmdVersionId(valueSchemaId, id);
  }
}
