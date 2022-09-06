package com.linkedin.venice.testng;

import com.linkedin.venice.utils.TestUtils;
import org.testng.ISuite;
import org.testng.ISuiteListener;


public class VeniceSuiteListener implements ISuiteListener {
  @Override
  public void onStart(ISuite suite) {
    System.out.println("Start suite " + suite.getName());
    TestUtils.preventSystemExit();
  }

  @Override
  public void onFinish(ISuite suite) {
    System.out.println("Finish suite " + suite.getName());
    TestUtils.restoreSystemExit();
  }
}
