package com.linkedin.venice.controllerapi;

import com.linkedin.venice.persona.StoragePersona;
import java.util.ArrayList;
import java.util.List;


public class MultiStoragePersonaResponse extends ControllerResponse {
  private List<StoragePersona> storagePersonas;

  public List<StoragePersona> getStoragePersonas() {
    return storagePersonas;
  }

  public void setStoragePersonas(List<StoragePersona> newList) {
    if (storagePersonas == null) {
      storagePersonas = new ArrayList<>();
    }
    storagePersonas.clear();
    storagePersonas.addAll(newList);
  }

}
