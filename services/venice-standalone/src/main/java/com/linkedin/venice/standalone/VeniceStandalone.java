package com.linkedin.venice.standalone;

import com.linkedin.venice.controller.VeniceController;
import com.linkedin.venice.router.RouterServer;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceStandalone {
  private static final Logger LOGGER = LogManager.getLogger(VeniceStandalone.class);

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      Utils.exit("USAGE: java -jar venice-standalone.jar <config_directory>");
      return;
    }
    try {
      VeniceController.run(
          new File(args[0], "cluster.properties").getAbsolutePath(),
          new File(args[0], "controller.properties").getAbsolutePath(),
          false);

      VeniceServer.run(new File(args[0]).getAbsolutePath(), false);

      RouterServer.run(new File(args[0], "router.properties").getAbsolutePath(), false);

      Thread.sleep(Integer.MAX_VALUE);
    } catch (Throwable error) {
      LOGGER.error("Error starting Venice standalone", error);
      System.exit(-1);
      return;
    }

  }
}
