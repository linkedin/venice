package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.JOB_EXEC_ID;
import static com.linkedin.venice.hadoop.VenicePushJob.MAPPER_OUTPUT_DIRECTORY;
import static com.linkedin.venice.hadoop.VenicePushJob.UNIQUE_STRING_FOR_MAPPER_OUTPUT_DIRECTORY;
import static com.linkedin.venice.hadoop.VenicePushJob.VENICE_STORE_NAME_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.getValidateSchemaAndBuildDictionaryOutputDir;
import static com.linkedin.venice.hadoop.VenicePushJob.getValidateSchemaAndBuildDictionaryOutputFileNameNoExtension;
import static org.apache.hadoop.mapreduce.MRJobConfig.ID;

import java.io.IOException;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class provides a way to:
 * 1. Reuse the existing output directory and override existing files which throws an exception in
 *    the parent class: to keep the outfile path/Name deterministic
 * 2. set custom permissions to the output directory/files to allow only the push job owners can
 *    access the personally identifiable information (eg: compressionDictionary)
 * 3. sets {@link FileOutputFormat#setOutputPath}
 */
public class ValidateSchemaAndBuildDictOutputFormat extends AvroOutputFormat {
  private static final Logger LOGGER = LogManager.getLogger(ValidateSchemaAndBuildDictOutputFormat.class);

  private static void createAndSetDirectoryPermission(FileSystem fs, Path path, String permission) throws IOException {
    createAndSetDirectoryPermission(fs, path, permission, false);
  }

  private static void createAndSetDirectoryPermission(
      FileSystem fs,
      Path path,
      String permission,
      boolean deleteIfExists) throws IOException {
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
   * 2. unique sub-directory for this VPJ should be accessible only by
   *    the user who triggers it (700) to protect unauthorized access to pii
   *    (eg: Zstd compression dictionary)
   *
   * @param job mapred config
   * @throws IOException
   */
  protected static void setValidateSchemaAndBuildDictionaryOutputDirPath(JobConf job) throws IOException {
    // parent directory
    String parentOutputDir = job.get(MAPPER_OUTPUT_DIRECTORY);
    Path outputPath = new Path(parentOutputDir);
    FileSystem fs = outputPath.getFileSystem(job);
    createAndSetDirectoryPermission(fs, outputPath, "777");

    // store specific directory under parent directory
    String storeName = job.get(VENICE_STORE_NAME_PROP);
    String jobExecId = job.get(JOB_EXEC_ID);
    String uniqueString = job.get(UNIQUE_STRING_FOR_MAPPER_OUTPUT_DIRECTORY);
    outputPath =
        new Path(getValidateSchemaAndBuildDictionaryOutputDir(parentOutputDir, storeName, jobExecId, uniqueString));
    createAndSetDirectoryPermission(fs, outputPath, "700");

    LOGGER.info(
        "{} Output will be stored in path: {}",
        ValidateSchemaAndBuildDictMapper.class.getSimpleName(),
        outputPath.toString());
    setOutputPath(job, outputPath);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    try {
      setValidateSchemaAndBuildDictionaryOutputDirPath(job);
      super.checkOutputSpecs(ignored, job);
    } catch (FileAlreadyExistsException e) {
      // Ignore the exception
    }
  }

  /**
   * Modify the output file name to be the MR job id to keep it unique.
   * No need to explicitly control the permissions for the output file
   * as its parent folder is restricted anyway.
   */
  @Override
  public RecordWriter getRecordWriter(FileSystem ignore, JobConf job, String name, Progressable prog)
      throws IOException {
    String newFileName = getValidateSchemaAndBuildDictionaryOutputFileNameNoExtension(job.get(ID));
    LOGGER.info(
        "{} Output will be stored in file: {}",
        ValidateSchemaAndBuildDictMapper.class.getSimpleName(),
        newFileName);
    return super.getRecordWriter(ignore, job, newFileName, prog);
  }
}
