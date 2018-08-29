package com.linkedin.venice.controllerapi.routes;

import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.ControllerResponse;


public class AdminCommandExecutionResponse extends ControllerResponse {
  private AdminCommandExecution execution;

  public AdminCommandExecution getExecution() {
    return execution;
  }

  public void setExecution(AdminCommandExecution execution) {
    this.execution = execution;
  }
}
