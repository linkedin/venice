package com.linkedin.alpini;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestCLI {
  @Test(groups = "unit")
  public void testNoParse() {
    ExampleCLI._args = null;
    ExampleCLI.main(new String[0]);
    Assert.assertNull(ExampleCLI._args);
    Assert.assertEquals(ExampleCLI._err.toString(), "Bad command line: Missing required option: path\n");
  }

  @Test(groups = "unit")
  public void testBasic() {
    ExampleCLI._args = null;
    ExampleCLI.main(new String[] { "--path", "foo", "extra" });
    Assert.assertNotNull(ExampleCLI._args);
    Assert.assertEquals(ExampleCLI._args.path, "foo");
    Assert.assertEquals(ExampleCLI._err.toString(), "");
    Assert.assertEquals(ExampleCLI._out.toString(), "");
    Assert.assertEquals(ExampleCLI._args.remain, new String[] { "extra" });
  }

  @Test(groups = "unit")
  public void testBasicBoolean() {
    ExampleCLI._args = null;
    ExampleCLI.main(new String[] { "--path", "foo", "-X" });
    Assert.assertNotNull(ExampleCLI._args);
    Assert.assertTrue(ExampleCLI._args.x);
    Assert.assertEquals(ExampleCLI._err.toString(), "");
    Assert.assertEquals(ExampleCLI._out.toString(), "");
  }

  @Test(groups = "unit")
  public void testBasicEnum1() {
    ExampleCLI._args = null;
    ExampleCLI.main(new String[] { "--path", "foo", "-d", "MODE1" });
    Assert.assertNotNull(ExampleCLI._args);
    Assert.assertSame(ExampleCLI._args.d, ExampleCLI.Mode.MODE1);
    Assert.assertEquals(ExampleCLI._err.toString(), "");
    Assert.assertEquals(ExampleCLI._out.toString(), "");
  }

  @Test(groups = "unit")
  public void testBasicEnumFail() {
    ExampleCLI._args = null;
    ExampleCLI.main(new String[] { "--path", "foo", "-d", "MODEX" });
    Assert.assertNull(ExampleCLI._args);
    Assert.assertEquals(ExampleCLI._err.toString(), "Bad value for d: \"MODEX\". Legal values are [MODE1, MODE2].\n");
  }

  @Test(groups = "unit")
  public void testBasicInteger() {
    ExampleCLI._args = null;
    ExampleCLI.main(new String[] { "--path", "foo", "-i", "100" });
    Assert.assertNotNull(ExampleCLI._args);
    Assert.assertEquals(ExampleCLI._args.i, 100);
    Assert.assertEquals(ExampleCLI._err.toString(), "");
    Assert.assertEquals(ExampleCLI._out.toString(), "");
  }

  @Test(groups = "unit")
  public void testFailInteger() {
    ExampleCLI._args = null;
    ExampleCLI.main(new String[] { "--path", "foo", "-i", "foo" });
    Assert.assertNull(ExampleCLI._args);
    Assert.assertEquals(ExampleCLI._err.toString(), "Bad value for i: \"foo\". Value must be an integer (int)\n");
  }

  @Test(groups = "unit")
  public void testBasicLong() {
    ExampleCLI._args = null;
    ExampleCLI.main(new String[] { "--path", "foo", "-l", "100" });
    Assert.assertNotNull(ExampleCLI._args);
    Assert.assertEquals(ExampleCLI._args.l, 100);
    Assert.assertEquals(ExampleCLI._err.toString(), "");
    Assert.assertEquals(ExampleCLI._out.toString(), "");
  }

  @Test(groups = "unit")
  public void testFailLong() {
    ExampleCLI._args = null;
    ExampleCLI.main(new String[] { "--path", "foo", "-l", "foo" });
    Assert.assertNull(ExampleCLI._args);
    Assert.assertEquals(ExampleCLI._err.toString(), "Bad value for l: \"foo\". Value must be a long integer (long)\n");
  }

  @Test(groups = "unit")
  public void testBasicFloat() {
    ExampleCLI._args = null;
    ExampleCLI.main(new String[] { "--path", "foo", "-r", "100.001" });
    Assert.assertNotNull(ExampleCLI._args);
    Assert.assertEquals(ExampleCLI._args.r, 100.001f);
    Assert.assertEquals(ExampleCLI._err.toString(), "");
    Assert.assertEquals(ExampleCLI._out.toString(), "");
  }

  @Test(groups = "unit")
  public void testFailFloat() {
    ExampleCLI._args = null;
    ExampleCLI.main(new String[] { "--path", "foo", "-r", "foo" });
    Assert.assertNull(ExampleCLI._args);
    Assert
        .assertEquals(ExampleCLI._err.toString(), "Bad value for r: \"foo\". Value must be a decimal number (float)\n");
  }

  @Test(groups = "unit")
  public void testBasicDouble() {
    ExampleCLI._args = null;
    ExampleCLI.main(new String[] { "--path", "foo", "-t", "100.001" });
    Assert.assertNotNull(ExampleCLI._args);
    Assert.assertEquals(ExampleCLI._args.t, 100.001);
    Assert.assertEquals(ExampleCLI._err.toString(), "");
    Assert.assertEquals(ExampleCLI._out.toString(), "");
  }

  @Test(groups = "unit")
  public void testFailDouble() {
    ExampleCLI._args = null;
    ExampleCLI.main(new String[] { "--path", "foo", "-t", "foo" });
    Assert.assertNull(ExampleCLI._args);
    Assert.assertEquals(
        ExampleCLI._err.toString(),
        "Bad value for t: \"foo\". Value must be a decimal number (double)\n");
  }

  @Test(groups = "unit")
  public void testHelp() {
    ExampleCLI._args = null;
    ExampleCLI.main(new String[] { "--path", "foo", "--help" });
    Assert.assertNull(ExampleCLI._args);
    Assert.assertEquals(ExampleCLI._err.toString(), "");
    Assert.assertEquals(
        ExampleCLI._out.toString(),
        "usage: example [-d <Mode>] [--fabrics <products>] [--help] [-i <int>] [-l\n"
            + "       <long>] [--outfile <filename>] --path <products> [--products\n"
            + "       <products>] [-r <Float>] [-t <double>] [-X]\n"
            + " -d <Mode>                  Mode (optional; options=[MODE1, MODE2])\n"
            + "    --fabrics <products>    Comma delimited list of fabrics to export\n"
            + "                            (optional; default=all fabrics)\n"
            + "    --help                  Show usage information\n"
            + " -i <int>                   integer (optional)\n" + " -l <long>                  long (optional)\n"
            + "    --outfile <filename>    File to export to, in csv format (optional;\n"
            + "                            default=stdout)\n"
            + "    --path <products>       Path to topology files (required)\n"
            + "    --products <products>   Comma delimited list of products to export\n"
            + "                            (optional; default=all products)\n"
            + " -r <Float>                 float (optional)\n" + " -t <double>                double (optional)\n"
            + " -X                         the X flag\n");
  }
}
