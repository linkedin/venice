package com.linkedin.venice.hadoop;

public class HelloWorld {
  public static String isHelloWorld(String s) {
    if (s == null || s.length() == 0) {
      return "false";
    } else if (s.equals("Hello World")) {
      return "true";
    } else {
      return "nothing";
    }
  }
}
