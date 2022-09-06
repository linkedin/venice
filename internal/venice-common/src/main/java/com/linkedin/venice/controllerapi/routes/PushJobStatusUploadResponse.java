package com.linkedin.venice.controllerapi.routes;

import com.linkedin.venice.controllerapi.ControllerResponse;


/**
 * Response for uploading a job status record.
 */
public class PushJobStatusUploadResponse extends ControllerResponse {
  /**
   * The version number associated with the job.
   */
  private int version;

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }
}
