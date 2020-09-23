package it.univr.veronacard.services;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import it.univr.veronacard.entities.TravelPath;
import it.univr.veronacard.entities.TravelStep;
import it.univr.veronacard.entities.VcProfile;
import it.univr.veronacard.entities.VcSite;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import static com.vividsolutions.jts.geom.PrecisionModel.FLOATING;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class JsonService {

  /**
   * The method returns a string representing the response obtained by the WFS
   * request {@code url}.
   *
   * @param url
   * @return
   */
  public static String readJsonFile( String url )
    throws UnreachableServiceException {

    final StringBuilder builder = new StringBuilder();

    try {
      final URL u = new URL( url );
      final HttpURLConnection connection = (HttpURLConnection) u.openConnection();
      connection.setRequestProperty( "User-Agent", "" );
      connection.setRequestMethod( "POST" );
      connection.setDoInput( true );
      connection.connect();
      final int statusCode = connection.getResponseCode();

      if( statusCode == 302 ) { // redirect
        final String location = connection.getHeaderField( "Location" );
        return readJsonFile( location );
      }

      if( statusCode == 200 ) {
        final InputStream content = connection.getInputStream();
        final BufferedReader reader = new BufferedReader
          ( new InputStreamReader( content ) );
        String line;
        while( ( line = reader.readLine() ) != null ) {
          builder.append( line );
        }
      } else {
        throw new UnreachableServiceException( statusCode );
      }
    } catch( IOException e ) {
      throw new UnreachableServiceException();
    }
    return builder.toString();
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param json
   * @return
   */
  public static List<VcSite> buildVcSites( String json ) {
    if( json == null ) {
      throw new NullPointerException();
    }

    try {
      final List<VcSite> result = new ArrayList<>();

      final JsonReader reader = Json.createReader( new StringReader( json ) );
      final JsonArray sites = reader.readArray();

      if( sites != null ) {
        for( int i = 0; i < sites.size(); i++ ) {
          final JsonObject site = sites.getJsonObject( i );

          final VcSite vcSite = new VcSite();
          vcSite.setId( getStringProperty( site, "site_id" ) );
          vcSite.setName( getStringProperty( site, "name" ) );
          vcSite.setNameShort( getStringProperty( site, "name_short" ) );

          final String enabled = getStringProperty( site, "enabled" );
          if( enabled != null && enabled.equals( "true" ) ) {
            vcSite.setEnabled( true );
          } else if( enabled != null && enabled.equals( "false" ) ) {
            vcSite.setEnabled( false );
          }

          vcSite.setLatitude( getDoubleProperty( site, "latitude" ) );
          vcSite.setLongitude( getDoubleProperty( site, "longitude" ) );

          if( vcSite.getId() != null ) {
            result.add( vcSite );
          }
        }
      }
      return result;
    } catch( Exception e ) {
      return Collections.emptyList();
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param json
   * @return
   */
  public static List<VcProfile> buildVcProfiles( String json ) {
    if( json == null ) {
      throw new NullPointerException();
    }

    try {
      final List<VcProfile> result = new ArrayList<>();

      final JsonReader reader = Json.createReader( new StringReader( json ) );
      final JsonArray profiles = reader.readArray();

      if( profiles != null ) {
        for( int i = 0; i < profiles.size(); i++ ) {
          final JsonObject profile = profiles.getJsonObject( i );

          final VcProfile vcProfile = new VcProfile();
          vcProfile.setId( getStringProperty( profile, "profile_id" ) );
          vcProfile.setName( getStringProperty( profile, "name" ) );
          vcProfile.setDescription( getStringProperty( profile, "description" ) );

          final String enabled = getStringProperty( profile, "enabled" );
          if( enabled != null && enabled.equals( "true" ) ) {
            vcProfile.setEnabled( true );
          } else if( enabled != null && enabled.equals( "false" ) ) {
            vcProfile.setEnabled( false );
          }

          final String date = getStringProperty( profile, "date" );
          if( date != null ) {
            try {
              final Long l = Long.parseLong( date );
              vcProfile.setDate( new Date( l * 1000 ) );
            } catch( NumberFormatException e ) {
              // nothing here
            }
          }

          final String days = getStringProperty( profile, "days" );
          if( days != null ) {
            try {
              final Integer d = Integer.parseInt( days );
              vcProfile.setDays( d );
            } catch( NumberFormatException e ) {
              // nothing here
            }
          }

          if( vcProfile.getId() != null ) {
            result.add( vcProfile );
          }
        }
      }
      return result;
    } catch( Exception e ) {
      return Collections.emptyList();
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param file
   * @return
   */

  public static TravelPath processPath( File file ) {
    if( file == null ) {
      throw new NullPointerException();
    }

    final GeometryFactory f = new GeometryFactory
      ( new PrecisionModel( FLOATING ), 4326 );
    final Reader in;
    try {
      in = new FileReader( file );

      final TravelPath path = new TravelPath();
      final List<Coordinate> pathCoords = new ArrayList<>();

      final String name = file.getName();
      final StringTokenizer tok = new StringTokenizer( name, "_" );
      while( tok.hasMoreElements() ) {
        final String c = tok.nextToken();
        if( c.equals( "orig" ) && tok.hasMoreTokens() ) {
          path.setOriginId( tok.nextToken() );
        } else if( c.equals( "dest" ) && tok.hasMoreTokens() ) {
          path.setDestinationId( tok.nextToken() );
        } else if( c.equals( "mode" ) && tok.hasMoreTokens() ) {
          String mode = tok.nextToken();
          if( mode.indexOf( "." ) > -1 ) {
            mode = mode.substring( 0, mode.indexOf( "." ) );
            mode = mode.toUpperCase();
          }
          path.setTravelMode( mode );
        }
      }

      final JsonReader reader = Json.createReader( in );
      final JsonObject mainObject = reader.readObject();

      final List<TravelStep> pathSteps = new ArrayList<>();
      final JsonArray routes = mainObject.getJsonArray( "routes" );
      if( routes != null ) {
        // process the various routes from the orig to the dest
        for( int i = 0; i < routes.size(); i++ ) {
          final JsonObject route = routes.getJsonObject( i );

          // each route may consist of one or more legs depending on whether
          // any waypoints were specified: if no waypoints are specified there
          // will be only one leg
          final JsonArray legs = route.getJsonArray( "legs" );
          if( legs.size() > 0 ) {
            // we consider only one leg: no waypoints!
            final JsonObject leg = legs.getJsonObject( 0 );
            final JsonArray steps = leg.getJsonArray( "steps" );

            for( int s = 0; s < steps.size(); s++ ) {
              final JsonObject cstep = steps.getJsonObject( s );

              final TravelStep ts = new TravelStep();

              final JsonObject distance = cstep.getJsonObject( "distance" );
              ts.setDistance( getDoubleProperty( distance, "value" ) );
              ts.setDistanceText( getStringProperty( distance, "text" ) );

              final JsonObject duration = cstep.getJsonObject( "duration" );
              ts.setDuration( getDoubleProperty( duration, "value" ) );
              ts.setDurationText( getStringProperty( duration, "text" ) );

              final JsonObject endLoc = cstep.getJsonObject( "end_location" );
              ts.setEndLocationLat( getDoubleProperty( endLoc, "lat" ) );
              ts.setEndLocationLng( getDoubleProperty( endLoc, "lng" ) );

              final JsonObject startLoc = cstep.getJsonObject( "start_location" );
              ts.setStartLocationLat( getDoubleProperty( startLoc, "lat" ) );
              ts.setStartLocationLng( getDoubleProperty( startLoc, "lng" ) );

              ts.setTravelMode( getStringProperty( cstep, "travel_mode" ) );

              // --- path ------------------------------------------------------
              final Coordinate start = new Coordinate
                ( ts.getStartLocationLng(), ts.getStartLocationLat() );
              final Coordinate end = new Coordinate
                ( ts.getEndLocationLng(), ts.getEndLocationLat() );
              ts.setPolyline( f.createLineString
                ( new Coordinate[]{start, end} ) );
              ts.getPolyline().setSRID( 4326 );

              if( pathCoords.size() == 0 ||
                  !pathCoords.get( pathCoords.size() - 1 ).equals( start ) ) {
                pathCoords.add( start );
              }
              pathCoords.add( end );
              // ---------------------------------------------------------------

              ts.setIndex( s );
              pathSteps.add( ts );
            }
          }
        }
      }
      path.setSteps( pathSteps );
      // --- path --------------------------------------------------------------
      path.setPolyline( f.createLineString
        ( pathCoords.toArray
          ( new Coordinate[pathCoords.size()] ) ) );
      path.getPolyline().setSRID( 4326 );
      // -----------------------------------------------------------------------

      return path;
    } catch( FileNotFoundException e ) {
      System.out.printf( "File \"%s\" not found", file.getAbsolutePath() );
      return null;
    }
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param properties
   * @param id
   * @return
   */
  private static String getStringProperty( JsonObject properties, String id ) {
    if( properties != null ) {
      try {
        String p = properties.get( id ).toString().trim();
        if( p.startsWith( "\"" ) ) {
          p = p.substring( 1, p.length() );
        }
        if( p.endsWith( "\"" ) ) {
          p = p.substring( 0, p.length() - 1 );
        }
        p = p.trim();

        if( p.equals( "null" ) ) {
          return null;
        } else {
          return p;
        }
      } catch( Throwable e ) {
        return null;
      }
    } else {
      return null;
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param properties
   * @param id
   * @return
   */
  private static Date getDateProperty( JsonObject properties, String id ) {
    if( properties != null ) {
      final SimpleDateFormat f = new SimpleDateFormat( "yyyy-MM-dd" );
      try {
        final String d = getStringProperty( properties, id );
        return f.parse( d );
      } catch( Throwable e ) {
        return null;
      }
    } else {
      return null;
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param properties
   * @param id
   * @return
   */
  private static Double getDoubleProperty( JsonObject properties, String id ) {
    if( properties != null ) {
      try {
        final String p = getStringProperty( properties, id );
        if( p.equals( "null" ) ) {
          return null;
        } else {
          final Double v = Double.parseDouble( p );
          return v;
        }
      } catch( Throwable e ) {
        return null;
      }
    } else {
      return null;
    }
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param encoded
   * @return
   */
  private static Coordinate[] decodePolyline( String encoded ) {
    if( encoded == null ) {
      throw new NullPointerException();
    }

    try {
      final List<Coordinate> poly = new ArrayList<>();
      int index = 0, len = encoded.length();
      int lat = 0, lng = 0;

      while( index < len ) {
        int b, shift = 0, result = 0;
        do {
          b = encoded.charAt( index++ ) - 63;
          result |= ( b & 0x1f ) << shift;
          shift += 5;
        } while( b >= 0x20 );
        int dlat = ( ( result & 1 ) != 0 ? ~( result >> 1 ) : ( result >> 1 ) );
        lat += dlat;

        shift = 0;
        result = 0;
        do {
          b = encoded.charAt( index++ ) - 63;
          result |= ( b & 0x1f ) << shift;
          shift += 5;
        } while( b >= 0x20 );
        int dlng = ( ( result & 1 ) != 0 ? ~( result >> 1 ) : ( result >> 1 ) );
        lng += dlng;

        final Coordinate p = new Coordinate( ( (double) lat / 1E5 ),
                                             ( (double) lng / 1E5 ) );
        poly.add( p );
      }
      return poly.toArray( new Coordinate[poly.size()] );
    } catch( StringIndexOutOfBoundsException e ) {
      System.out.printf( "Encoded: %s%n", encoded );
      return null;
    }
  }

}
