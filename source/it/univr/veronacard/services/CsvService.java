package it.univr.veronacard.services;

import it.univr.veronacard.entities.VCard;
import it.univr.veronacard.entities.VcTicket;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class CsvService {

  // === Attributes ============================================================

  private static int TICKET_DATE = 0;
  private static int TICKET_TIME = 1;
  private static int SITE_NAME = 2;
  private static int DEVICE = 3;
  private static int VC_SERIAL = 4;
  private static int VC_ACTIVATION_DATE = 5;
  private static int VC_ACTIVE = 6;
  private static int VC_ACTIVATION_TYPE = 7;
  private static int VC_PROFILE = 8;

  // === Methods ===============================================================

  /**
   * MISSING_COMMENT
   *
   * @param path
   * @param numColumns
   * @return
   */
  public static List<Map<Integer, String>> readCsvFile
  ( String path, int numColumns ) {

    if( path == null ) {
      throw new NullPointerException();
    }

    final Reader in;
    try {
      in = new FileReader( new File( path ) );
    } catch( FileNotFoundException e ) {
      System.out.printf( "File \"%s\" not found", path );
      return Collections.emptyList();
    }

    final Iterable<CSVRecord> records;
    final List<Map<Integer, String>> result = new ArrayList<>();
    try {
      records = CSVFormat.EXCEL.parse( in );
      for( CSVRecord r : records ) {
        final Map<Integer, String> value = new HashMap<>();
        for( int i = 0; i < numColumns; i++ ) {
          r.get( i );
          value.put( i, r.get( i ) );
        }
        result.add( value );
      }
      return result;

    } catch( IOException e ) {
      System.out.printf( "Unable to parse records in \"%s\".", path );
      return Collections.emptyList();
    }
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param records
   * @return
   */
  public static List<VCard> buildVcRecord
  ( List<Map<Integer, String>> records ) {

    if( records == null ) {
      throw new NullPointerException();
    }

    final Map<String, VCard> map = new HashMap<>();

    for( Map<Integer, String> tuple : records ) {
      final VcTicket ticket = new VcTicket();
      ticket.setDate( tuple.get( TICKET_DATE ), tuple.get( TICKET_TIME ) );
      ticket.setSiteName( tuple.get( SITE_NAME ) );
      ticket.setDevice( tuple.get( DEVICE ) );

      VCard card = map.get( tuple.get( VC_SERIAL ) );
      if( card == null ) {
        card = new VCard();
        card.setSerial( tuple.get( VC_SERIAL ) );
        card.setActivationDate( tuple.get( VC_ACTIVATION_DATE ) );
        card.setActive( tuple.get( VC_ACTIVE ) );
        card.setActivationType( tuple.get( VC_ACTIVATION_TYPE ) );
        card.setProfile( tuple.get( VC_PROFILE ) );
      }
      card.addTicket( ticket );

      map.put( card.getSerial(), card );
    }
    return new ArrayList<>( map.values() );
  }
}
