package com.linkedin.venice.duckdb;

import static com.linkedin.venice.duckdb.Arg.COLUMNS_TO_PROJECT;
import static com.linkedin.venice.duckdb.Arg.DUCKDB_OUTPUT_DIRECTORY;
import static com.linkedin.venice.duckdb.Arg.STORE_NAME;
import static com.linkedin.venice.duckdb.Arg.ZK_HOST_URL;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;


/**
 * Contains all the possible commands for DuckVinciTool
 */
public enum Command {
  INGEST(
      "ingest", "Ingests all of the data from a Venice store into DuckDB", new Arg[] { STORE_NAME, ZK_HOST_URL },
      new Arg[] { DUCKDB_OUTPUT_DIRECTORY, COLUMNS_TO_PROJECT }
  );

  private final String commandName;
  private final String description;
  private final Arg[] requiredArgs;
  private final Arg[] optionalArgs;

  Command(String argName, String description, Arg[] requiredArgs) {
    this(argName, description, requiredArgs, new Arg[] {});
  }

  Command(String argName, String description, Arg[] requiredArgs, Arg[] optionalArgs) {
    this.commandName = argName;
    this.description = description;
    this.requiredArgs = requiredArgs;
    this.optionalArgs = optionalArgs;
  }

  @Override
  public String toString() {
    return commandName;
  }

  public Arg[] getRequiredArgs() {
    return requiredArgs;
  }

  public Arg[] getOptionalArgs() {
    return optionalArgs;
  }

  public String getDesc() {
    StringJoiner sj = new StringJoiner("");
    if (!description.isEmpty()) {
      sj.add(description);
      sj.add(". ");
    }

    StringJoiner requiredArgs = new StringJoiner(" ");
    for (Arg arg: getRequiredArgs()) {
      requiredArgs.add("--" + arg.toString());
    }

    sj.add("\nRequires: " + requiredArgs);

    StringJoiner optionalArgs = new StringJoiner(" ");
    for (Arg arg: getOptionalArgs()) {
      optionalArgs.add("--" + arg.toString());
    }

    if (getOptionalArgs().length > 0) {
      sj.add("\nOptional args: " + optionalArgs.toString());
    }

    return sj.toString();
  }

  public static final Comparator<Command> commandComparator = new Comparator<Command>() {
    public int compare(Command c1, Command c2) {
      return c1.commandName.compareTo(c2.commandName);
    }
  };

  public static Command getCommand(String name, CommandLine cmdLine) {
    for (Command cmd: values()) {
      if (cmd.commandName.equals(name)) {
        return cmd;
      }
    }
    if (name == null) {
      List<String> candidateCommands = Arrays.stream(Command.values())
          .filter(
              command -> Arrays.stream(command.getRequiredArgs()).allMatch(arg -> cmdLine.hasOption(arg.toString())))
          .map(command -> "--" + command)
          .collect(Collectors.toList());
      if (!candidateCommands.isEmpty()) {
        throw new VeniceException(
            "No command found, potential commands compatible with the provided parameters include: "
                + Arrays.toString(candidateCommands.toArray()));
      }
    }
    String message = name == null
        ? " No command found, Please specify a command, eg [--get] "
        : "No Command found with name: " + name;
    throw new VeniceException(message);
  }
}
