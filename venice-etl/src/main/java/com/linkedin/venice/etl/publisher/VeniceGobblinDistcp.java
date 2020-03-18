package com.linkedin.venice.etl.publisher;

import java.io.IOException;
import org.apache.gobblin.runtime.api.JobExecutionDriver;
import org.apache.gobblin.runtime.api.JobExecutionResult;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblinDistcp;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


public class VeniceGobblinDistcp implements Runnable {

  private static final Logger logger = Logger.getLogger(VeniceGobblinDistcp.class);
  private EmbeddedGobblinDistcp distcp;
  private Path destination;

  public VeniceGobblinDistcp(Path from, Path to) throws JobTemplate.TemplateException, IOException {
    this.distcp = new EmbeddedGobblinDistcp(from, to);
    this.destination = to;
  }

  /**
   * Blocks the call until the job finishes.
   */
  public void run() {
    try {
      JobExecutionResult jobResult = this.distcp.run();
      if (jobResult.isSuccessful()) {
        logger.info("Successfully published the delta to " + this.destination);
      } else if (jobResult.isFailed()) {
        logger.info("Distcp job failed publishing the delta to " + this.destination + jobResult.getErrorCause());
      } else if (jobResult.isCancelled()) {
        logger.info("Distcp job canceled publishing the delta to " + destination);
      }
    } catch (Exception e) {
      logger.warn("Error when running distributed copy", e);
    }
  }

  /**
   * Asynchronously runs a gobblin job, returns once the job is initialized.
   */
  public JobExecutionDriver runAsync() {
    try {
      return this.distcp.runAsync();
    } catch (Exception e) {
      logger.warn("Error when running distributed copy", e);
    }
    return null;
  }

  public void setDistcpConfigs(String key, String value) {
    this.distcp.setConfiguration(key, value);
  }
}
