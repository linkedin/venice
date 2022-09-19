package com.linkedin.venice.utils;

import com.linkedin.venice.persona.StoragePersona;
import java.util.HashSet;
import java.util.Set;


public class TestStoragePersonaUtils {
  public static final String QUOTA_FAILED_REGEX = "Invalid persona quota:.*";
  public static final String STORES_FAILED_REGEX = "Invalid store\\(s\\) provided: .*";
  public static final String PERSONA_DOES_NOT_EXIST_REGEX =
      "Update failed: persona with name .* does not exist in this cluster";
  public static final String OWNERS_DOES_NOT_EXIST_REGEX = "Invalid owner\\(s\\) provided";

  public static StoragePersona createDefaultPersona() {
    long quota = 100;
    String testPersonaName = Utils.getUniqueString("testPersona");
    Set<String> testStoreNames = new HashSet<>();
    Set<String> testOwnerNames = new HashSet<>();
    testOwnerNames.add("testOwner");
    return new StoragePersona(testPersonaName, quota, testStoreNames, testOwnerNames);
  }

}
