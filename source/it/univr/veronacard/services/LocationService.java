package it.univr.veronacard.services;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class LocationService {

  // === Attributes ============================================================

  private static final String geocodeApi =
    "https://maps.googleapis.com/maps/api/geocode/json?"
    + "address=%s&"
    + "key=%s";


  private static final String directionApi =
    "https://maps.googleapis.com/maps/api/directions/json?"
    + "origin=%s+Verona&"
    + "destination=%s+Verona&"
    + "key=%s&"
    + "mode=%s";
  //=Universal+Studios+Hollywood4

  private static final String apiKey =
    "AIzaSyA7wETLqjfWblfp36orff4e0_clEFHX8UQ";


  private static final String[] modes = new String[]{
    "driving",
    "walking",
  };

  // === Methods ===============================================================

  private LocationService() {
    // nothing here
  }


  /**
   * MISSING_COMMENT
   *
   * @param origin
   * @param destination
   */

  public static void retrievePath
  ( String origin,
    String destination,
    String origId,
    String destId,
    String directory ) {

    try {
      Thread.sleep( 500 ); // wait a half second for google api restrictions

      final String originEnc = URLEncoder.encode( origin, "UTF-8" );
      final String destEnc = URLEncoder.encode( destination, "UTF-8" );

      for( String m : modes ) {
        String address = format( directionApi, originEnc, destEnc, apiKey, m );
        address = encodeAddress( address );

        final URL url = new URL( address );
        final InputStream is = url.openStream();

        // save the JSON file
        final List<String> lines = new ArrayList<>();
        final BufferedReader reader = new BufferedReader
          ( new InputStreamReader( is ) );
        String line;
        while( ( line = reader.readLine() ) != null ) {
          lines.add( line );
        }

        final Path file = Paths.get
          ( format
              ( "%s/orig_%s_dest_%s_mode_%s.json",
                directory,
                origId,
                destId,
                m ) );
        Files.write( file, lines, Charset.forName( "UTF-8" ) );
      }

    } catch( InterruptedException e ) {
      e.printStackTrace();
    } catch( UnsupportedEncodingException e ) {
      e.printStackTrace();
    } catch( MalformedURLException e ) {
      e.printStackTrace();
    } catch( IOException e ) {
      e.printStackTrace();
    }
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param address
   * @return
   */

  private static String encodeAddress( String address ) {
    if( address != null ) {
      address = address.replace( " ", "%20" );
      address = address.replace( "+", "%20" );
      address = address.replace( "\n", "%20" );
    }
    return address;
  }


  // ===========================================================================

  /*public static Location retrieveLocation
  ( Logger logger,
    String where,
    String objectType,
    String objectId ) {

    try {
      Thread.sleep( 500 ); // wait a half second for google api restrictions
      final String whereEnc = URLEncoder.encode( where, "UTF-8" );
      String address = format( geocodeApi, whereEnc, apiKey );
      address = address.replace( " ", "%20" );
      address = address.replace( "+", "%20" );
      address = address.replace( "\n", "%20" );

      final URL url = new URL( address );
      final InputStream is = url.openStream();

      // parse the JSON file
      final StringBuilder b = new StringBuilder();
      final BufferedReader reader = new BufferedReader
        ( new InputStreamReader( is ) );
      String line;
      while( ( line = reader.readLine() ) != null ) {
        b.append( line );
      }

      // create JsonReader from Json.
      final JsonReader jsonReader =
        createReader( new StringReader( b.toString() ) );
      // get the JsonObject structure from JsonReader.
      final JsonObject root = jsonReader.readObject();
      // we are done with the reader, let's close it.
      jsonReader.close();

      final JsonArray results = root.getJsonArray( "results" );
      if( results != null && results.size() > 0 ) {
        final JsonObject geometry =
          ( (JsonObject) results.get( 0 ) )
            .getJsonObject( "geometry" );

        final JsonObject location = geometry.getJsonObject( "location" );
        final String locationType = geometry.getString( "location_type" );

        if( locationType.equals( LocationType.ROOFTOP.toString() ) ) {
          logger.info
            ( "[LOCATION] Precise location found for address \"%s\" of %s %s.",
              where,
              objectType,
              objectId );
        } else {
          if( results.size() > 1 ) {
            logger.severe
              ( "[LOCATION] Ambiguous location for address \"%s\" of %s %s.",
                where,
                objectType,
                objectId );
          } else {
            logger.info
              ( "[LOCATION] Approximated location found for address \"%s\" "
                + "of %s %s. Kind of approximation: %s",
                where,
                objectType,
                objectId,
                locationType );
          }
        }

        final BigDecimal lat = location.getJsonNumber( "lat" ).bigDecimalValue();
        final BigDecimal lng = location.getJsonNumber( "lng" ).bigDecimalValue();

        if( lat.compareTo( minLat ) < 0 ||
            lat.compareTo( maxLat ) > 0 ||
            lng.compareTo( minLng ) < 0 ||
            lng.compareTo( maxLng ) > 0 ) {
          logger.severe
            ( "[LOCATION] No location found inside Verona for "
              + "address \"%s\" of event %s.",
              where, objectId );
          return null;
        }

        final Location result = new Location();
        result.setLatitude
          ( location.getJsonNumber( "lat" ).bigDecimalValue() );
        result.setLongitude
          ( location.getJsonNumber( "lng" ).bigDecimalValue() );
        result.setAddress( where );
        return result;
      } else {
        return null;
      }
    } catch( MalformedURLException e ) {
      logger.severe
        ( "[LOCATION] No location found for address \"%s\" of event %s. "
          + "API not available: %s",
          where,
          objectId,
          e.getMessage() );
      return null;
    } catch( IOException e ) {
      logger.severe
        ( "[LOCATION] No location found for address \"%s\" of event %s. "
          + "API not available: %s",
          where,
          objectId,
          e.getMessage() );
      return null;
    } catch( InterruptedException e ) {
      Thread.currentThread().interrupt();
      return null;
    }
  }//*/

}
