package it.univr.veronacard.services;

import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class UnreachableServiceException extends Exception {

  // === Properties ============================================================

  private int statusCode;

  // === Methods ===============================================================

  public UnreachableServiceException() {
  }

  public UnreachableServiceException( String message ) {
    super( message );
  }

  public UnreachableServiceException( String message, Throwable cause ) {
    super( message, cause );
  }

  public UnreachableServiceException( Throwable cause ) {
    super( cause );
  }

  public UnreachableServiceException
    ( String message,
      Throwable cause,
      boolean enableSuppression,
      boolean writableStackTrace ) {
    super( message, cause, enableSuppression, writableStackTrace );
  }

  public UnreachableServiceException( int statusCode ) {
    super( format( "Failed with status code %s", statusCode ) );
    this.statusCode = statusCode;
  }
}
