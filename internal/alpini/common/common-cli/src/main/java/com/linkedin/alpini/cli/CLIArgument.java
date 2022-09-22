/*
 * $Id$
 */
package com.linkedin.alpini.cli;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Annotation for a command line option with an argument, like "--file <filename>".
 *
 * @see CLIArgumentParser
 * @author Jemiah Westerman <jwesterman@linkedin.com>
 * @version $Revision$
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CLIArgument {
  /** A short sentence description of the option. Shown in help / usage screens. */
  String description();

  /** Argument name. For example, if you want --file <filename>, then argumentName is the "filename" part.
   * If not provided, one will be auto-generated from the field name. */
  String argumentName() default CLIArgumentParser.NO_DEFAULT_STRING;

  /** Long name for the option. If not provided, one will be auto-generated from the field name. */
  String longOpt() default CLIArgumentParser.NO_DEFAULT_STRING;

  /** Short name for the option. If not provided, the option will not have a short name. */
  char shortOpt() default CLIArgumentParser.NO_DEFAULT_CHAR;

  /** Default value for the option. If no default is provided, the argument is required. */
  String defaultValue() default CLIArgumentParser.NO_DEFAULT_STRING;

  /** Set to true if the default action is inaction. */
  boolean optional() default false;
}
