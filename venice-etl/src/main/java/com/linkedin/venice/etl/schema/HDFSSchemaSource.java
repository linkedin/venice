package com.linkedin.venice.etl.schema;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.etl.client.VeniceKafkaConsumerClient;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.SourceState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.etl.source.VeniceKafkaSource.*;


public class HDFSSchemaSource implements SchemaSource {
  private static final Logger logger = Logger.getLogger(HDFSSchemaSource.class);

  private Path schemaPath;
  private FileSystem fs;
  private Map<String, String> storeNameToKeySchemaStr;
  private Map<String, String[]> storeNameToAllValueSchemaStr;
  public HDFSSchemaSource(String schemaDir) throws IOException {
    Configuration conf = new Configuration();
    fs = FileSystem.get(conf);

    // create a schemas folder
    this.schemaPath = new Path(schemaDir);
    if (!fs.exists(schemaPath)) {
      fs.mkdirs(schemaPath);
    }
  }

  /**
   * This method loads Venice related configs, gets store schemas from Venice controllers and
   * writes the schemas to HDFS files.
   * @param state
   */
  public void load(SourceState state) {
    String veniceControllerUrls = state.getProp(VENICE_CHILD_CONTROLLER_URLS);
    loadSchemas(veniceControllerUrls, state.getProp(VENICE_STORE_NAME));
    loadSchemas(veniceControllerUrls,state.getProp(FUTURE_ETL_ENABLED_STORES));
  }

  private void getStoreSchemaFromVenice(String veniceControllerUrls, Set<String> veniceStoreNames) {

    Map<String, ControllerClient> storeToControllerClient = VeniceKafkaConsumerClient.getControllerClients(veniceStoreNames, veniceControllerUrls);
    getStoreSchema(storeToControllerClient, veniceStoreNames);
  }

  private void getStoreSchema(Map<String, ControllerClient> storeToControllerClient, Set<String> veniceStoreNames) {
    storeNameToKeySchemaStr = new HashMap<>();
    storeNameToAllValueSchemaStr = new HashMap<>();
    for (String storeName: veniceStoreNames) {
      SchemaResponse keySchemaResponse = storeToControllerClient.get(storeName).getKeySchema(storeName);
      String keySchemaStr = keySchemaResponse.getSchemaStr();
      storeNameToKeySchemaStr.put(storeName, keySchemaStr);
      logger.info("key schema for store " + storeName + ": " + keySchemaStr);

      MultiSchemaResponse allValueSchemaResponses = storeToControllerClient.get(storeName).getAllValueSchema(storeName);
      com.linkedin.venice.controllerapi.MultiSchemaResponse.Schema[] allValueSchemas = allValueSchemaResponses.getSchemas();
      int numOfValueSchemas = allValueSchemas.length;
      String[] allValueSchemaStr = new String[numOfValueSchemas];
      for (int j = 0; j < numOfValueSchemas; j++) {
        allValueSchemaStr[j] = allValueSchemas[j].getSchemaStr();
        logger.info("value schema #" + j + " for store " + storeName + ": " + allValueSchemaStr[j]);
      }
      storeNameToAllValueSchemaStr.put(storeName, allValueSchemaStr);
    }
  }

  @Override
  public Schema fetchKeySchema(String storeName) {
    Schema outputKeySchema = null;
    try {
      String storeSchemaDir = schemaPath + "/" + storeName;
      Path storeSchemaPath = new Path(storeSchemaDir);
      String keySchemaStr = storeSchemaPath + "/key";
      Path keySchemaPath = new Path(keySchemaStr);

      FSDataInputStream inputStream = fs.open(keySchemaPath);
      BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      String keySchema = br.readLine().trim();
      outputKeySchema = Schema.parse(keySchema);
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }
    return outputKeySchema;
  }

  @Override
  public Schema[] fetchValueSchemas(String storeName) {
    String storeSchemaDir = schemaPath + "/" + storeName;
    Path storeSchemaPath = new Path(storeSchemaDir);

    // build avro readers for all value schemas
    int valueSchemaNum = 0;
    Schema[] outputValueSchemas = null;
    try {
      FileStatus[] fileStatuses = fs.listStatus(storeSchemaPath);
      List<Path> valueSchemaPathsList = new ArrayList<>();
      for (FileStatus fileStatus : fileStatuses) {
        if (StringUtils.isNumeric(fileStatus.getPath().getName())) {
          valueSchemaNum++;
          valueSchemaPathsList.add(fileStatus.getPath());
        }
      }

      outputValueSchemas = new Schema[valueSchemaNum];
      for (Path valueSchemaPath : valueSchemaPathsList) {
        int schemaId = Integer.valueOf(valueSchemaPath.getName());
        FSDataInputStream in = fs.open(valueSchemaPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        String valueSchema = reader.readLine().trim();
        outputValueSchemas[schemaId] = Schema.parse(valueSchema);
      }
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }
    return outputValueSchemas;
  }

  private void setKeySchema(String storeName) {
    try {
      String storeSchemaDir = createStorePath(storeName);

      // path of the key schema is /working_path/store_name/key
      String keySchemaStr = storeSchemaDir + "/key";
      Path keySchemaPath = new Path(keySchemaStr);
      if (!fs.exists(keySchemaPath)) {
        logger.info("store " + storeName + " key schema path didn't exist");
        FSDataOutputStream outputStream = fs.create(keySchemaPath);
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, "UTF-8");
        outputStreamWriter.write(storeNameToKeySchemaStr.get(storeName));
        outputStreamWriter.flush();
        outputStreamWriter.close();
      }

    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

  private void putValueSchemas(String storeName) {
    try {
      String storeSchemaDir = createStorePath(storeName);

      String[] valueSchemas = storeNameToAllValueSchemaStr.get(storeName);
      for (int i = 0; i < valueSchemas.length; i++) {
        // path of the value schema is /working_path/topic_name/schemaId
        String valueSchemaStr = storeSchemaDir + "/" + i;
        Path valueSchemaPath = new Path(valueSchemaStr);
        if (!fs.exists(valueSchemaPath)) {
          logger.info("store " + storeName + " value schema " + i + " didn't exist");
          FSDataOutputStream outputStream = fs.create(valueSchemaPath);
          OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, "UTF-8");
          outputStreamWriter.write(valueSchemas[i] + "\n");
          outputStreamWriter.flush();
          outputStream.close();
        }
      }
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    }
  }

  private String createStorePath(String storeName) throws IOException {
    String storeSchemaDir = schemaPath + "/" + storeName;
    Path storeSchemaPath = new Path(storeSchemaDir);
    if (!fs.exists(storeSchemaPath)) {
      logger.info("store " + storeName + " schema path didn't exist");
      fs.mkdirs(storeSchemaPath);
    }
    return storeSchemaDir;
  }

  private void loadSchemas(String veniceChildControllerUrls, String storeNamesList) {
    // for current version etl
    Set<String> veniceStoreNames = new HashSet<>();
    String[] tokens = storeNamesList.split(VENICE_STORE_NAME_SEPARATOR);
    for (String token : tokens) {
      veniceStoreNames.add(token.trim());
    }
    getStoreSchemaFromVenice(veniceChildControllerUrls, veniceStoreNames);
    for (String storeName : veniceStoreNames) {
      setKeySchema(storeName);
      putValueSchemas(storeName);
    }
  }
}
