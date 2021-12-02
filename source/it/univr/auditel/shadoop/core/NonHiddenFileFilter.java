package it.univr.auditel.shadoop.core;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class NonHiddenFileFilter implements PathFilter {

  @Override
  public boolean accept( Path path ) {
    String name = path.getName();
    return !name.startsWith("_") && !name.startsWith(".");
  }
}
