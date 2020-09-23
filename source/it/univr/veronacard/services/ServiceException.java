package it.univr.veronacard.services;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class ServiceException extends Exception {

  public ServiceException() {
  }

  public ServiceException( String message ) {
    super( message );
  }

  public ServiceException( String message, Throwable cause ) {
    super( message, cause );
  }

  public ServiceException( Throwable cause ) {
    super( cause );
  }

  public ServiceException( String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace ) {
    super( message, cause, enableSuppression, writableStackTrace );
  }
}
