package com.linkedin.venice;

import com.linkedin.venice.controllerapi.MultiReplicaResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
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
}
