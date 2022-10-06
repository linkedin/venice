package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_STORE_NAME_PROP;

import java.io.IOException;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class provides a way to:
 * 1. Reuse the existing output directory and override existing files which throws an exception in
 *    the parent class: to keep the outfile path/Name deterministic
 * 2. set custom permissions to the output directory/files to allow only the push job owners can
 *    access the personally identifiable information (eg: compressionDictionary)
 * 3. sets {@link org.apache.hadoop.mapred.FileOutputFormat#setOutputPath}
 */
public class ValidateSchemaAndBuildDictOutputFormat extends AvroOutputFormat {
  private static final Logger LOGGER = LogManager.getLogger(ValidateSchemaAndBuildDictOutputFormat.class);

  private static void createAndSetFilePermission(FileSystem fs, Path path, String permission) throws IOException {
    createAndSetFilePermission(fs, path, permission, false);
  }

  private static void createAndSetFilePermission(FileSystem fs, Path path, String permission, boolean deleteIfExists)
      throws IOException {
    LOGGER.info("Trying to create path {} with permission {}", path.getName(), permission);
    boolean createPath = false;
    // check if the path needs to be created
    if (fs.exists(path)) {
      if (deleteIfExists) {
        LOGGER.info("path {} exists already, but will be deleted and recreated", path);
        fs.delete(path, true);
        createPath = true;
      } else {
        LOGGER.info("path {} exists already", path);
      }
    } else {
      createPath = true;
    }

    // create if needed
    if (createPath) {
      LOGGER.info("Creating path {}", path.getName());
      fs.mkdirs(path);
    }

    FsPermission perm = new FsPermission(permission);

    LOGGER.info("Setting permission {}", permission);
    fs.setPermission(path, perm);
  }

  /**
   * 1. The parent directory should be accessible by every user/group (777)
   * 2. specific output directory for this store should be accessible only by
   *    the user who triggers VPJ (700)
   * @param conf mapred config
   * @param storeName used for the specific output directory name
   * @throws IOException
   */
  protected static void setValidateSchemaAndBuildDictionaryOutputDirPath(JobConf conf, String storeName)
      throws IOException {
    Path outputPath = new Path(VenicePushJob.VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_PATH);
    FileSystem fs = outputPath.getFileSystem(conf);

    createAndSetFilePermission(fs, outputPath, "777");

    outputPath = new Path(
        VenicePushJob.VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_PATH + "/"
            + VenicePushJob.VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_PER_STORE_PATH_PREFIX + storeName);

    createAndSetFilePermission(fs, outputPath, "700");

    setOutputPath(conf, outputPath);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    try {
      String storeName = job.get(VENICE_STORE_NAME_PROP);
      setValidateSchemaAndBuildDictionaryOutputDirPath(job, storeName);
      super.checkOutputSpecs(ignored, job);
    } catch (FileAlreadyExistsException e) {
      // Ignore the exception
    }
  }
}
