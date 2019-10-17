package com.linkedin.venice;

import com.linkedin.venice.controllerapi.MultiReplicaResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.commons.cli.CommandLine;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestAdminTool {
  @Test
  public void testPrintObject(){
    List<String> output = new ArrayList<>();
    Consumer<String> printCapture = (String s) -> output.add(s);

    MultiReplicaResponse multiReplicaResponse = new MultiReplicaResponse();
    AdminTool.printObject(multiReplicaResponse, printCapture);

    Assert.assertFalse(output.get(output.size()-1).contains("topic"), "Printed multi-replica response should not contain a topic field");
  }

  @Test(enabled = false) // disable until SSL config file becomes a mandatory config
  public void testAdminToolRequiresSSLConfigFile() {
    String[] args = {"--delete-store", "--url", "https://localhost:7036", "--cluster", "test-cluster", "--store", "testStore"};
    try {
      AdminTool.main(args);
    } catch (Exception e) {
      // AdminTool should enforce the rule that SSL config file must be included
      Assert.assertTrue(e.getMessage().contains("SSL config file path must be specified"));
    }
  }

  @Test
  public void testAdminUpdateStoreArg() {
    String[] args = {"--update-store", "--url", "http://localhost:7036", "--cluster", "test-cluster", "--store", "testStore"};

    try {
      CommandLine commandLine = AdminTool.getCommandLine(args);
      AdminTool.getUpdateStoreQueryParams(commandLine);
    } catch (Exception e) {
      Assert.fail(" All options are not added to update-store arg doc");
    }
  }
}
