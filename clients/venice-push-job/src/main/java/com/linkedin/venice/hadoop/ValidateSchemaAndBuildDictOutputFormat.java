package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.MAPPER_OUTPUT_DIRECTORY;
import static com.linkedin.venice.hadoop.VenicePushJob.VALIDATE_SCHEMA_AND_BUILD_DICT_MAPPER_OUTPUT_DIRECTORY;
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

  private static void createDirectoryWithPermission(FileSystem fs, Path path, String permission) throws IOException {
    createDirectoryWithPermission(fs, path, permission, false);
  }

  private static void createDirectoryWithPermission(FileSystem fs, Path path, String permission, boolean deleteIfExists)
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
      LOGGER.info("Creating path {} with permission {}", path.getName(), permission);
      fs.mkdirs(path);
      // mkdirs(path,permission) didn't set the right permission when
      // tested in hdfs, so splitting it like this, it works!
      fs.setPermission(path, new FsPermission(permission));
    }
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
    // parent directory: Common directory under which all the different push jobs
    // create their job specific directories.
    FileSystem fs = FileSystem.get(job);
    String parentOutputDir = job.get(MAPPER_OUTPUT_DIRECTORY);
    Path outputPath = new Path(parentOutputDir);
    createDirectoryWithPermission(fs, outputPath, "777");

    // store+job specific unique directory under parent directory: already derived in VPJ driver
    // and passed along with the format: {$storeName}-{$JOB_EXEC_ID}-{$randomUniqueString}
    // this job creates it and VPJ driver deletes it after consuming the data in this directory
    // in ValidateSchemaAndBuildDictMapperOutputReader. setting 700 permissions for pii.
    String fullOutputDir = job.get(VALIDATE_SCHEMA_AND_BUILD_DICT_MAPPER_OUTPUT_DIRECTORY);
    outputPath = new Path(fullOutputDir);
    createDirectoryWithPermission(fs, outputPath, "700");

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
