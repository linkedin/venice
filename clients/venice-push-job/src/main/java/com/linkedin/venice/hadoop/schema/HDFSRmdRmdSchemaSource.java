package com.linkedin.venice.hadoop.schema;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.hadoop.VenicePushJob.CONTROLLER_REQUEST_RETRY_ATTEMPTS;
import static com.linkedin.venice.hadoop.VenicePushJob.ENABLE_SSL;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_STORE_NAME_PROP;

import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.utils.ControllerUtils;
import com.linkedin.venice.schema.rmd.RmdVersionId;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
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
 * Borrowed some codes from HDFSSchemaSource. The difference is HDFSRmdSchemaSource is created for a single store.
 */
@NotThreadsafe
public class HDFSRmdRmdSchemaSource implements RmdSchemaSource, AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(HDFSRmdRmdSchemaSource.class);
  private static final String UNDERSCORE = "_";
  private static final String SEPARATOR = "/";
  private final VeniceProperties props;
  private final FileSystem fs;
  private final Path schemaDir;
  private final ControllerClient controllerClient;

  public HDFSRmdRmdSchemaSource(
      final String schemaDir,
      final VeniceProperties props,
      final ControllerClient controllerClient) throws IOException {
    Configuration conf = new Configuration();
    this.fs = FileSystem.get(conf);
    this.schemaDir = new Path(schemaDir);
    if (!fs.exists(this.schemaDir)) {
      fs.mkdirs(this.schemaDir);
    }
    this.props = props;
    this.controllerClient = controllerClient;
  }

  public HDFSRmdRmdSchemaSource(final String schemaDir, final VeniceProperties props) throws IOException {
    this(schemaDir, props, null);
  }

  /**
   * When no properties is provided, the schema source is intended for read-only.
   * @param schemaDirSuffix
   * @throws IOException
   */
  public HDFSRmdRmdSchemaSource(final String schemaDirSuffix) throws IOException {
    this(schemaDirSuffix, null);
  }

  public String getPath() {
    return schemaDir.getName();
  }

  /**
   * This method loads related configs, gets store RMD schemas from Venice controllers, and then write them on HDFS.
   * @throws IOException
   * @throws IllegalStateException
   */
  public void loadRmdSchemasOnDisk() throws IOException, IllegalStateException {
    if (this.props == null) {
      throw new IllegalStateException("The schema source has no VeniceProperties provided.");
    }
    // there's a risk of closing ControllerClient if passed via the contractor, as the client may be used somewhere else
    boolean selfClose = false;
    if (controllerClient == null) {
      selfClose = true;
    }
    ControllerClient client = initControllerClient();
    String storeName = props.getString(VENICE_STORE_NAME_PROP);
    LOGGER.info("Starting fetching and caching RMD schemas for {} at {}", storeName, schemaDir.toString());
    MultiSchemaResponse.Schema[] schemas = client.getAllReplicationMetadataSchemas(storeName).getSchemas();
    for (MultiSchemaResponse.Schema schema: schemas) {
      // path for rmd schema is /<schemaDir>/<id>_<valueSchemaId>
      Path schemaPath = new Path(schemaDir + SEPARATOR + schema.getId() + UNDERSCORE + schema.getValueSchemaId());
      if (!fs.exists(schemaPath)) {
        try (FSDataOutputStream outputStream = fs.create(schemaPath);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
          outputStreamWriter.write(schema.getSchemaStr() + "\n");
          outputStreamWriter.flush();
        }
        LOGGER.info(
            "Finished writing RMD schema with id {} and derived id {} onto disk",
            schema.getId(),
            schema.getDerivedSchemaId());
      }
    }

    if (selfClose) {
      client.close();
    }
  }

  private Optional<SSLFactory> initSslFactory() throws VeniceException {
    Lazy<Properties> sslProperties = Lazy.of(() -> {
      try {
        return ControllerUtils.getSslProperties(this.props);
      } catch (IOException e) {
        throw new VeniceException("Could not get user credential");
      }
    });

    return ControllerUtils.createSSlFactory(
        props.getBoolean(ENABLE_SSL, false),
        props.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME),
        sslProperties);
  }

  private ControllerClient initControllerClient() throws VeniceException {
    if (controllerClient != null) {
      return controllerClient;
    }
    return ControllerClient.discoverAndConstructControllerClient(
        props.getString(VENICE_STORE_NAME_PROP),
        ControllerUtils.getVeniceControllerUrl(props),
        initSslFactory(),
        props.getInt(CONTROLLER_REQUEST_RETRY_ATTEMPTS, 1));
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
    for (FileStatus status: fileStatus) {
      Path path = status.getPath();
      try (FSDataInputStream in = fs.open(path);
          BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        RmdVersionId pair = parseIdsFromPath(path);
        String schemeStr = reader.readLine();
        if (schemeStr == null) {
          throw new RuntimeException(String.format("Failed to load RMD schema at the path %s", path));
        }
        mapping.put(pair, Schema.parse(schemeStr));
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
