package com.linkedin.venice.controllerapi;

import com.linkedin.venice.persona.StoragePersona;


public class StoragePersonaResponse extends ControllerResponse {
  private StoragePersona storagePersona;

  public StoragePersona getStoragePersona() {
    return storagePersona;
  }

  public void setStoragePersona(StoragePersona storagePersona) {
    this.storagePersona = storagePersona;
  }

  @Override
  public String toString() {
    return StoragePersonaResponse.class.getSimpleName() + "(\n" + storagePersona.toString() + ",\n" + super.toString()
        + ")";
  }

}
