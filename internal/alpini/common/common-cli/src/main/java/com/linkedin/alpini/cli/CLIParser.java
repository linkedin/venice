package com.linkedin.alpini.cli;

public interface CLIParser<T> {
  void logConfiguredValues(T args);

  T parse(String[] args);

  void showUsage(boolean verbose);

  void showUsage(String error);
}
