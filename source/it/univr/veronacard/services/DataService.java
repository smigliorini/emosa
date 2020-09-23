package it.univr.veronacard.services;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class DataService {

  // === Properties ============================================================

  private static String driverName = "org.postgresql.Driver";
  private static String hostname = "localhost";
  private static String port = "5432";
  private static String dbname = "veronacard";
  private String connectionString = format
    ( "jdbc:postgresql://%s:%s/%s", hostname, port, dbname );
  ;
  private String username = "tourism";
  private String password = "andr01d3";


  // === Methods ===============================================================

  /**
   * Default constructor
   */
  public DataService() {
  }

  /**
   * This method starts a new connection with the database
   *
   * @return
   * @throws ServiceException
   */

  protected Connection beginConnection()
    throws ServiceException {

    try {
      Class.forName( driverName ).newInstance();
      Connection con = DriverManager.getConnection
        ( connectionString, username, password );
      con.setAutoCommit( false );
      return con;

    } catch( InstantiationException e ) {
      final String message =
        format( "A connection with the database cannot be established: %s.",
                e.getMessage() );
      throw new ServiceException( message, e );
    } catch( IllegalAccessException e ) {
      final String message =
        format( "A connection with the database cannot be established: %s.",
                e.getMessage() );
      throw new ServiceException( message, e );
    } catch( ClassNotFoundException e ) {
      final String message =
        format( "A connection with the database cannot be established: %s.",
                e.getMessage() );
      throw new ServiceException( message, e );
    } catch( SQLException e ) {
      final String message =
        format( "A connection with the database cannot be established: %s.",
                e.getMessage() );
      throw new ServiceException( message, e );
    }
  }

  /**
   * This method terminates a database connection and makes effective the
   * performed changes.
   *
   * @param conn
   * @throws ServiceException
   */

  protected void commit( Connection conn )
    throws ServiceException {
    try {
      // end transaction
      conn.setAutoCommit( true );
      conn.close();
    } catch( SQLException e ) {
      abort( conn );
      throw new ServiceException( "commit error", e );
    }
  }

  /**
   * This method terminates a database connection and discards all the performed
   * changes.
   *
   * @param conn connessione da terminare.
   */

  protected void abort( Connection conn ) {
    try {
      if( conn != null ) {
        conn.rollback();
        conn.close();
      }
    } catch( SQLException e ) {
      // TODO logging this error.
    }
  }

  /**
   * MOD 2004-12-04 questo metodo e' stato aggiunto per forzare la chiusura
   * delle connessioni tramite una clausola finally NEL CASO esista ancora
   * qualche eccezione non gestita che non richiama abort.
   *
   * @param conn
   */

  protected void finalize( Connection conn ) {
    try {
      if( conn != null && conn.isClosed() == false ) {
        conn.close();
      }
    } catch( SQLException e ) {
      // TODO logging this error.
    }
  }
}
