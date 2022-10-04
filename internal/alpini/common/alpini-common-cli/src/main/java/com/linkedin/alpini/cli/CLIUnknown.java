package com.linkedin.alpini.cli;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Annotation for a String[] property which will receive all the command line elements which were not processed
 * by the command line parser.
 *
 * @see CLIArgumentParser
 * @author Antony T Curtis <acurtis@linkedin.com>
 * @version $Revision$
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface CLIUnknown {
}
