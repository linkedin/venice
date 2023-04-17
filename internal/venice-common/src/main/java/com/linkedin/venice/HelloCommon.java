package com.linkedin.venice;

public class HelloCommon {
  public static boolean isValid(String s) {
    if (s.length() > 10) {
      return true;
    } else if (s.length() == 0) {
      return false;
    } else {
      return true;
    }
  }
}
