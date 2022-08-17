package com.linkedin.venice.controllerapi;

import com.linkedin.venice.meta.StoreDataAudit;
import java.util.Map;


public class ClusterStaleDataAuditResponse extends ControllerResponse {
  private Map<String, StoreDataAudit> _auditMap;

  public Map<String, StoreDataAudit> getAuditMap() {
    return _auditMap;
  }

  public void setAuditMap(Map<String, StoreDataAudit> auditMap) {
    _auditMap = auditMap;
  }
}
