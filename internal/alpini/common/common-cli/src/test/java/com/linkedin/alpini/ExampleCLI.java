package com.linkedin.alpini;

import com.linkedin.alpini.cli.CLIArgument;
import com.linkedin.alpini.cli.CLIArgumentParser;
import com.linkedin.alpini.cli.CLIFlag;
import com.linkedin.alpini.cli.CLIUnknown;
import java.io.PrintWriter;
import java.io.StringWriter;


public class ExampleCLI {
  private static final String APPLICATION_NAME = "example";

  public static class Args {
    @CLIArgument(description = "Path to topology files", argumentName = "products")
    public String path;
    @CLIArgument(description = "Comma delimited list of fabrics to export", argumentName = "products", defaultValue = "all fabrics")
    public String fabrics;
    @CLIArgument(description = "Comma delimited list of products to export", argumentName = "products", defaultValue = "all products")
    public String products;
    @CLIArgument(description = "File to export to, in csv format", argumentName = "filename", defaultValue = "stdout")
    public String outfile;
    @CLIArgument(description = "Mode", shortOpt = 'd', optional = true)
    public Mode d;
    @CLIArgument(description = "integer", shortOpt = 'i', optional = true)
    public int i;
    @CLIArgument(description = "float", shortOpt = 'r', optional = true)
    public Float r;
    @CLIArgument(description = "long", shortOpt = 'l', optional = true)
    public long l;
    @CLIArgument(description = "double", shortOpt = 't', optional = true)
    public double t;
    @CLIFlag(shortOpt = 'X', description = "the X flag")
    public boolean x;
    @CLIUnknown
    public String[] remain;
  }

  public enum Mode {
    MODE1, MODE2
  }

  static Args _args;
  static StringWriter _out;
  static StringWriter _err;

  public static void main(String[] args) {
    _out = new StringWriter();
    _err = new StringWriter();
    _args = new CLIArgumentParser<>(APPLICATION_NAME, Args.class).redirect(new PrintWriter(_out), new PrintWriter(_err))
        .startup(args);
  }
}
