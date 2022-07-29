package com.linkedin.venice.utils;

import com.linkedin.venice.persona.StoragePersona;
import java.util.HashSet;
import java.util.Set;


public class TestStoragePersonaUtils {

  public final static String quotaFailedRegex = "Invalid persona quota:.*";
  public final static String storesFailedRegex =
      "Invalid store\\(s\\) provided: .*";
  public final static String personaDoesNotExistRegex =
      "Update failed: persona with name .* does not exist in this cluster";
  public final static String ownersDoesNotExistRegex = "Invalid owner\\(s\\) provided";

  public static StoragePersona createDefaultPersona() {
    long quota = 100;
    String testPersonaName = Utils.getUniqueString("testPersona");
    Set<String> testStoreNames = new HashSet<>();
    Set<String> testOwnerNames = new HashSet<>();
    testOwnerNames.add("testOwner");
    return new StoragePersona(testPersonaName, quota, testStoreNames, testOwnerNames);
  }



}
