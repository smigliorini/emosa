package it.univr.veronacard.services;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import it.univr.veronacard.entities.TravelPath;
import it.univr.veronacard.entities.TravelStep;
import it.univr.veronacard.entities.VCard;
import it.univr.veronacard.entities.VcProfile;
import it.univr.veronacard.entities.VcSite;
import it.univr.veronacard.entities.VcTicket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import static java.sql.PreparedStatement.RETURN_GENERATED_KEYS;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class VcDataService extends DataService {

  // === Properties ============================================================

  public static String createVcSiteTable =
    "CREATE TABLE IF NOT EXISTS vc_site ("
    + "id varchar(80) PRIMARY KEY,"
    + "name varchar(512),"
    + "name_short varchar(256),"
    + "enabled boolean,"
    + "latitude numeric(10,4),"
    + "longitude numeric(10,4)"
    + ")";

  public static String addVcSitePointColumn =
    "SELECT AddGeometryColumn"
    + "( 'vc_site', 'geom', 4326, 'POINT', 2)";

  public static String createVcProfileTable =
    "CREATE TABLE IF NOT EXISTS vc_profile("
    + "id varchar(80) PRIMARY KEY,"
    + "name varchar(512),"
    + "enabled boolean,"
    + "days integer,"
    + "date timestamp,"
    + "description text"
    + ")";

  public static String createVcTable =
    "CREATE TABLE IF NOT EXISTS verona_card("
    + "serial varchar(128) PRIMARY KEY,"
    + "activation_date timestamp,"
    + "active varchar(16),"
    + "activation_type varchar(16),"
    + "profile varchar(80)"
    + ")";

  public static String createVcTicketTable =
    "CREATE TABLE IF NOT EXISTS vc_ticket("
    + "id serial PRIMARY KEY,"
    + "arriving_date timestamp,"
    + "device varchar(80),"
    + "site_name varchar(512),"
    + "vc_serial varchar(128)"
    + ")";

  public static String addVcTicketPointColumn =
    "SELECT AddGeometryColumn"
    + "( 'vc_ticket', 'geom', 4326, 'POINT', 4)";

  public static String createTrajSerialTable =
    "CREATE TABLE IF NOT EXISTS vc_traj_serial ("
    + "id serial PRIMARY KEY,"
    + "vc_serial varchar(128)"
    + ")";

  public static String addTrajSerialLinestringColumn =
    "SELECT AddGeometryColumn"
    + "( 'vc_traj_serial', 'geom', 4326, 'LINESTRING', 2)";

  public static String createPathTable =
    "CREATE TABLE IF NOT EXISTS g_travel_path ("
    + "id serial PRIMARY KEY,"
    + "origin_id varchar(80),"
    + "destination_id varchar(80),"
    + "travel_mode varchar(256)"
    + ")";

  public static String addPathLineColumn =
    "SELECT AddGeometryColumn"
    + "( 'g_travel_path', 'path', 4326, 'LINESTRING', 2)";

  public static String createPathStepTable =
    "CREATE TABLE IF NOT EXISTS g_travel_path_step ("
    + "id serial PRIMARY KEY,"
    + "path_id integer,"
    + "travel_mode varchar(256),"
    + "duration numeric(10,2),"
    + "duration_text varchar(256),"
    + "distance numeric(10,2),"
    + "distance_text varchar(256),"
    + "index integer"
    + ")";

  public static String addPathStepStartLocColumn =
    "SELECT AddGeometryColumn"
    + "( 'g_travel_path_step', 'start_location', 4326, 'POINT', 2)";
  public static String addPathStepEndLocColumn =
    "SELECT AddGeometryColumn"
    + "( 'g_travel_path_step', 'end_location', 4326, 'POINT', 2)";
  public static String addPathStepLineColumn =
    "SELECT AddGeometryColumn"
    + "( 'g_travel_path_step', 'path', 4326, 'LINESTRING', 2)";

  public static String createTrajGoogleTable =
    "CREATE TABLE IF NOT EXISTS vc_traj_google ("
    + "id serial PRIMARY KEY,"
    + "vc_serial varchar(128),"
    + "travel_mode varchar(256)"
    + ")";

  public static String addTrajGoogleLinestringColumn =
    "SELECT AddGeometryColumn"
    + "( 'vc_traj_google', 'geom', 4326, 'LINESTRING', 2)";

  public static String buildVcSitePoint =
    "UPDATE vc_site s "
    + "SET geom ="
    + "ST_SetSRID("
    + "ST_MakePoint( longitude, latitude ),"
    + "4326)";

  public static String buildVcTicketPoint =
    "UPDATE vc_ticket t "
    + "SET geom ="
    + "( SELECT ST_SetSRID("
    + "ST_MakePoint( longitude, latitude, 0.0,"
    + "EXTRACT( EPOCH FROM t.arriving_date)),"
    + "4326)"
    + "FROM vc_site "
    + "WHERE t.site_name = name_short"
    + ")";

  public static String getVcTickets =
    "SELECT s.id "
    + "FROM vc_ticket t join vc_site s on (t.site_name = s.name_short) "
    + "WHERE vc_serial = ? "
    + "ORDER BY arriving_date";

  public static String getVcGeometry =
    "SELECT ST_AsText(geom) "
    + "FROM verona_card "
    + "WHERE serial = ? ";

  public static String buildVcTrajSerialLinestring =
    "INSERT INTO vc_traj_serial (vc_serial, geom) VALUES "
    + "( ?, ST_SetSRID(ST_GeomFromText(?),4326) )";

  public static String insertVcSite =
    "INSERT INTO vc_site( id, name, name_short, enabled, latitude, longitude )"
    + "VALUES ( ?, ?, ?, ?, ?, ? )";

  public static String insertVcProfile =
    "INSERT INTO vc_profile( id, name, enabled, days, date, description )"
    + "VALUES( ?, ?, ?, ?, ?, ? )";

  public static String insertVeronaCard =
    "INSERT INTO verona_card( serial, activation_date, active, activation_type, profile ) "
    + "VALUES( ?, ?, ?, ?, ? )";

  public static String insertVcTicket =
    "INSERT INTO vc_ticket( arriving_date, site_name, site_id, vc_serial ) "
    + "VALUES( ?, ?, ?, ? )";

  public static String insertTravelPath =
    "INSERT INTO g_travel_path( origin_id, destination_id, travel_mode, path ) "
    + "VALUES (?, ?, ?, ST_SetSRID(ST_GeomFromText(?),4326) )";

  public static String insertTravelPathStep =
    "INSERT INTO g_travel_path_step"
    + "( path_id, travel_mode, "
    + "duration, duration_text, "
    + "distance, distance_text, "
    + "start_location, end_location, path, index ) VALUES"
    + "( ?, ?, ?, ?, ?, ?, "
    + "ST_SetSRID(ST_MakePoint(?,?),4326), "
    + "ST_SetSRID(ST_MakePoint(?,?),4326), "
    + "ST_SetSRID(ST_GeomFromText(?),4326), "
    + "? )";

  public static String selectVcSites =
    "SELECT id, name_short from vc_site";

  public static String selectVcTickets =
    "SELECT t.vc_serial, s.id "
    + "FROM vc_ticket t JOIN vc_site s ON ( t.site_name = s.name_short) "
    + "ORDER BY t.vc_serial, t.arriving_date";

  public static String selectVcTicketGeoms =
    "SELECT vc_serial, ST_AsText( geom ) "
    + "FROM vc_ticket t "
    + "ORDER BY arriving_date";

  public static String selectGTravelPath =
    "SELECT ST_AsText( path ) "
    + "FROM g_travel_path "
    + "WHERE origin_id = ? AND destination_id = ? AND travel_mode = ? ";

  public static String buildVcTrajGoogleLinestring =
    "INSERT INTO vc_traj_google (vc_serial, travel_mode, geom ) VALUES "
    + "( ?, ?, ST_SetSRID(ST_GeomFromText(?),4326) )";

  // === Methods ===============================================================

  /**
   * MISSING_COMMENT
   *
   * @param query
   * @throws ServiceException
   */

  public void executeQuery( String query )
    throws ServiceException {

    Connection con = null;
    try {
      con = beginConnection();
      executeQuery( query, con );
      commit( con );

    } catch( Exception e ) {
      abort( con );
      throw new ServiceException( e );
    } finally {
      finalize( con );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param query
   * @param con
   * @throws ServiceException
   */

  private void executeQuery( String query, Connection con )
    throws ServiceException {

    if( con == null ) {
      throw new NullPointerException();
    }

    try {
      final PreparedStatement ps = con.prepareStatement( query );
      ps.execute();

    } catch( SQLException e ) {
      throw new ServiceException( e );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param query
   * @throws ServiceException
   */

  public List<Map<String, Object>> executeSelectQuery
  ( String query,
    Map<String, Class> attributes,
    List<String> parameters )
    throws ServiceException {

    Connection con = null;
    List<Map<String, Object>> result = null;
    try {
      con = beginConnection();
      result = executeSelectQuery( query, attributes, parameters, con );
      commit( con );

    } catch( Exception e ) {
      abort( con );
      throw new ServiceException( e );
    } finally {
      finalize( con );
    }
    return result;
  }


  /**
   * MISSING_COMMENT
   *
   * @param query
   * @param con
   * @throws ServiceException
   */

  public List<Map<String, Object>> executeSelectQuery
  ( String query,
    Map<String, Class> attributes,
    List<String> parameters,
    Connection con )
    throws ServiceException {

    if( con == null ) {
      throw new NullPointerException();
    }
    if( attributes == null ) {
      throw new NullPointerException();
    }
    if( query == null ) {
      throw new NullPointerException();
    }

    try {
      final PreparedStatement ps = con.prepareStatement( query );

      if( parameters != null ){
        for( int i = 0; i < parameters.size(); i++ ){
          // fast fix!!!!
          ps.setString( i + 1, parameters.get( i ) );
        }
      }

      final ResultSet rs = ps.executeQuery();

      final List<Map<String, Object>> result = new ArrayList<>();
      while( rs.next() ) {
        final Map<String, Object> tuple = new HashMap<>();
        for( Map.Entry<String, Class> a : attributes.entrySet() ) {
          final Object value = rs.getObject( a.getKey() );
          tuple.put( a.getKey(), value );
        }
        result.add( tuple );
      }
      return result;
    } catch( SQLException e ) {
      throw new ServiceException( e );
    }
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param sites
   * @throws ServiceException
   */

  public void insertVcSites( List<VcSite> sites )
    throws ServiceException {

    if( sites == null ) {
      throw new NullPointerException();
    }

    Connection con = null;
    try {
      con = beginConnection();
      for( VcSite s : sites ) {
        insertVcSite( s, con );
      }
      commit( con );

    } catch( Exception e ) {
      abort( con );
      throw new ServiceException( e );
    } finally {
      finalize( con );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param site
   * @param con
   * @throws ServiceException
   */

  private void insertVcSite( VcSite site, Connection con )
    throws ServiceException {

    if( site == null ) {
      throw new NullPointerException();
    }
    if( con == null ) {
      throw new NullPointerException();
    }

    try {
      final PreparedStatement ps = con.prepareStatement( insertVcSite );
      ps.setString( 1, site.getId() );
      ps.setString( 2, site.getName() );
      ps.setString( 3, site.getNameShort() );
      ps.setBoolean( 4, site.getEnabled() );
      ps.setDouble( 5, site.getLatitude() );
      ps.setDouble( 6, site.getLongitude() );

      ps.execute();

    } catch( SQLException e ) {
      throw new ServiceException( e );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param profiles
   * @throws ServiceException
   */

  public void insertVcProfiles( List<VcProfile> profiles )
    throws ServiceException {

    if( profiles == null ) {
      throw new NullPointerException();
    }

    Connection con = null;
    try {
      con = beginConnection();
      for( VcProfile p : profiles ) {
        insertVcProfile( p, con );
      }
      commit( con );

    } catch( Exception e ) {
      abort( con );
      throw new ServiceException( e );
    } finally {
      finalize( con );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param profile
   * @param con
   * @throws ServiceException
   */

  private void insertVcProfile( VcProfile profile, Connection con )
    throws ServiceException {

    if( profile == null ) {
      throw new NullPointerException();
    }
    if( con == null ) {
      throw new NullPointerException();
    }

    try {
      final PreparedStatement ps = con.prepareStatement( insertVcProfile );
      ps.setString( 1, profile.getId() );
      ps.setString( 2, profile.getName() );
      ps.setBoolean( 3, profile.getEnabled() );
      ps.setInt( 4, profile.getDays() );
      ps.setDate( 5, new java.sql.Date( profile.getDate().getTime() ) );
      ps.setString( 6, profile.getDescription() );

      ps.execute();

    } catch( SQLException e ) {
      throw new ServiceException( e );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param cards
   * @throws ServiceException
   */

  public void insertVeronaCardRecords( List<VCard> cards )
    throws ServiceException {

    if( cards == null ) {
      throw new NullPointerException();
    }

    Connection con = null;
    try {
      con = beginConnection();
      for( VCard c : cards ) {
        insertVeronaCardRecords( c, con );
      }
      commit( con );

    } catch( Exception e ) {
      abort( con );
      throw new ServiceException( e );
    } finally {
      finalize( con );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param card
   * @param con
   * @throws ServiceException
   */

  private void insertVeronaCardRecords( VCard card, Connection con )
    throws ServiceException {

    if( card == null ) {
      throw new NullPointerException();
    }
    if( con == null ) {
      throw new NullPointerException();
    }

    try {
      final PreparedStatement ps = con.prepareStatement( insertVeronaCard );
      ps.setString( 1, card.getSerial() );
      if( card.getActivationDate() != null ) {
        ps.setDate( 2, new java.sql.Date( card.getActivationDate().getTime() ) );
      } else {
        ps.setDate( 2, null );
      }
      ps.setString( 3, card.getActive() );
      ps.setString( 4, card.getActivationType() );
      ps.setString( 5, card.getProfile() );
      ps.execute();

      if( card.getTickets() != null ) {
        for( VcTicket t : card.getTickets() ) {
          final PreparedStatement inps = con.prepareStatement( insertVcTicket );
          if( t.getDate() != null ) {
            inps.setTimestamp( 1, new Timestamp( t.getDate().getTime() ) );
          } else {
            inps.setDate( 1, null );
          }
          inps.setString( 2, t.getSiteName() );
          inps.setString( 3, t.getDevice() );
          inps.setString( 4, card.getSerial() );
          inps.execute();
        }
      }

    } catch( SQLException e ) {
      throw new ServiceException( e );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param paths
   * @throws ServiceException
   */

  public void insertTravelPaths( List<TravelPath> paths )
    throws ServiceException {

    if( paths == null ) {
      throw new NullPointerException();
    }

    Connection con = null;
    try {
      con = beginConnection();
      for( TravelPath p : paths ) {
        insertTravelPath( p, con );
      }
      commit( con );

    } catch( Exception e ) {
      abort( con );
      throw new ServiceException( e );
    } finally {
      finalize( con );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param path
   * @param con
   * @throws ServiceException
   */

  private void insertTravelPath( TravelPath path, Connection con )
    throws ServiceException {

    if( path == null ) {
      throw new NullPointerException();
    }
    if( con == null ) {
      throw new NullPointerException();
    }

    try {
      final PreparedStatement ps = con.prepareStatement
        ( insertTravelPath, RETURN_GENERATED_KEYS );
      ps.setString( 1, path.getOriginId() );
      ps.setString( 2, path.getDestinationId() );
      ps.setString( 3, path.getTravelMode() );
      ps.setString( 4, path.getPolyline().toString() );
      ps.executeUpdate();

      final ResultSet key = ps.getGeneratedKeys();
      Long pathId = null;
      if( key.next() ) {
        pathId = key.getLong( 1 );
      }

      if( pathId != null &&
          path.getSteps() != null && !path.getSteps().isEmpty() ) {
        for( TravelStep s : path.getSteps() ) {
          final PreparedStatement inps =
            con.prepareStatement( insertTravelPathStep );
          inps.setLong( 1, pathId );
          inps.setString( 2, s.getTravelMode() );
          inps.setDouble( 3, s.getDuration() );
          inps.setString( 4, s.getDurationText() );
          inps.setDouble( 5, s.getDistance() );
          inps.setString( 6, s.getDistanceText() );
          inps.setDouble( 7, s.getStartLocationLng() );
          inps.setDouble( 8, s.getStartLocationLat() );
          inps.setDouble( 9, s.getEndLocationLng() );
          inps.setDouble( 10, s.getEndLocationLat() );
          inps.setString( 11, s.getPolyline().toString() );
          inps.setInt( 12, s.getIndex() );
          inps.execute();
        }
      }
    } catch( SQLException e ) {
      throw new ServiceException( e );
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @throws ServiceException
   */

  public void buildVcTrajSerial()
    throws ServiceException {

    Connection con = null;
    try {
      con = beginConnection();
      buildVcTrajSerial( con );
      commit( con );

    } catch( Exception e ) {
      abort( con );
      throw new ServiceException( e );
    } finally {
      finalize( con );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param con
   * @throws ServiceException
   */

  public void buildVcTrajSerial( Connection con )
    throws ServiceException {

    if( con == null ) {
      throw new NullPointerException();
    }

    try {
      final PreparedStatement ps = con.prepareStatement( selectVcTicketGeoms );
      final ResultSet rs = ps.executeQuery();

      // path for verona_card serial: for each serial, the list of tickets
      final Map<String, List<String>> paths = new HashMap<>();
      while( rs.next() ) {
        final String serial = rs.getString( 1 );
        final String geom = rs.getString( 2 );

        List<String> p = paths.get( serial );
        if( p == null ) {
          p = new ArrayList<>();
        }
        p.add( geom );
        paths.put( serial, p );
      }

      final GeometryFactory f = new GeometryFactory();

      for( Map.Entry<String, List<String>> gp : paths.entrySet() ) {
        final String vcSerial = gp.getKey();
        final List<String> value = gp.getValue();
        final List<Coordinate> cs = new ArrayList<>();

        for( String v : value ) {
          try {
            final Point p = readPointZM( v );
            if( p != null ) {
              for( Coordinate c : p.getCoordinates() ) {
                cs.add( c );
              }
            }
          } catch( ParseException e ) {
            // nothing here
          }
        }
        if( cs != null && cs.size() > 1 ) {
          final LineString ls =
            f.createLineString( cs.toArray( new Coordinate[cs.size()] ) );

          final PreparedStatement inPs =
            con.prepareStatement( buildVcTrajSerialLinestring );
          inPs.setString( 1, vcSerial );
          inPs.setString( 2, ls.toString() );
          inPs.execute();
        }
      }

    } catch( SQLException e ) {
      throw new ServiceException( e );
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @throws ServiceException
   */

  public void buildVcTrajGoogle()
    throws ServiceException {

    Connection con = null;
    try {
      con = beginConnection();
      buildVcTrajGoogle( con );
      commit( con );

    } catch( Exception e ) {
      abort( con );
      throw new ServiceException( e );
    } finally {
      finalize( con );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param con
   * @throws ServiceException
   */

  public void buildVcTrajGoogle( Connection con )
    throws ServiceException {

    if( con == null ) {
      throw new NullPointerException();
    }

    try {
      final PreparedStatement ps = con.prepareStatement( selectVcTickets );
      final ResultSet rs = ps.executeQuery();

      // path for verona_card serial: for each serial, the list of tickets
      final Map<String, List<String>> paths = new HashMap<>();
      while( rs.next() ) {
        final String serial = rs.getString( 1 );
        final String siteId = rs.getString( 2 );

        List<String> p = paths.get( serial );
        if( p == null ) {
          p = new ArrayList<>();
        }
        p.add( siteId );
        paths.put( serial, p );
      }

      final String[] travelMode = new String[]{
        "DRIVING",
        "WALKING"
      };

      final Map<String, List<String>> gpaths = new HashMap<>();
      final GeometryFactory f = new GeometryFactory();
      final WKTReader reader = new WKTReader();

      for( String tm : travelMode ) {
        for( Map.Entry<String, List<String>> p : paths.entrySet() ) {
          final List<String> value = p.getValue();
          if( value != null && value.size() > 1 ) {
            for( int i = 0; i < value.size() - 1; i++ ) {
              final String orig = value.get( i );
              final String dest = value.get( i + 1 );

              if( !orig.equals( dest ) ) {
                final PreparedStatement inPs =
                  con.prepareStatement( selectGTravelPath );
                inPs.setString( 1, orig );
                inPs.setString( 2, dest );
                inPs.setString( 3, tm );
                final ResultSet inRs = inPs.executeQuery();

                if( inRs.next() ) {
                  List<String> gp = gpaths.get( p.getKey() );
                  if( gp == null ) {
                    gp = new ArrayList<>();
                  }
                  gp.add( inRs.getString( 1 ) );
                  gpaths.put( p.getKey(), gp );
                } else {
                  System.out.printf
                    ( "Not found path from %s to %s%n",
                      orig, dest );
                }
              }
            }
          }
        }

        for( Map.Entry<String, List<String>> gp : gpaths.entrySet() ) {
          final String vcSerial = gp.getKey();
          final List<String> value = gp.getValue();
          final List<Coordinate> cs = new ArrayList<>();

          for( String v : value ) {
            try {
              final Geometry g = reader.read( v );
              if( g.getGeometryType().equals( "LineString" ) ) {
                for( Coordinate c : g.getCoordinates() ) {
                  cs.add( c );
                }
              }
            } catch( ParseException e ) {
              // nothing here
            }
          }
          if( cs != null && !cs.isEmpty() ) {
            final LineString ls =
              f.createLineString( cs.toArray( new Coordinate[cs.size()] ) );

            final PreparedStatement inPs =
              con.prepareStatement( buildVcTrajGoogleLinestring );
            inPs.setString( 1, vcSerial );
            inPs.setString( 2, tm );
            inPs.setString( 3, ls.toString() );
            inPs.execute();
          }
        }
      }

    } catch( SQLException e ) {
      throw new ServiceException( e );
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param vcSerial
   * @return
   * @throws ServiceException
   */
  public List<String> getVcTickets( String vcSerial )
    throws ServiceException {

    List<String> result;
    Connection con = null;
    try {
      con = beginConnection();
      result = getVcTickets( vcSerial, con );
      commit( con );

    } catch( Exception e ) {
      abort( con );
      throw new ServiceException( e );
    } finally {
      finalize( con );
    }
    return result;
  }

  /**
   * MISSING_COMMENT
   *
   * @param vcSerial
   * @param conn
   * @return
   * @throws ServiceException
   */
  public List<String> getVcTickets( String vcSerial, Connection conn )
    throws ServiceException, SQLException {

    if( conn == null ) {
      throw new NullPointerException();
    }

    if( vcSerial == null ) {
      throw new NullPointerException();
    }

    final PreparedStatement ps = conn.prepareStatement( getVcTickets );
    ps.setString( 1, vcSerial );
    final ResultSet rs = ps.executeQuery();

    final List<String> result = new ArrayList<>();
    while( rs.next() ) {
      final String site = rs.getString( 1 );
      result.add( site );
    }
    return result;
  }


  /**
   * MISSING_COMMENT
   *
   * @param vcSerial
   * @return
   * @throws ServiceException
   */
  public Geometry getVcGeometry( String vcSerial )
    throws ServiceException {

    Geometry result;
    Connection con = null;
    try {
      con = beginConnection();
      result = getVcGeometry( vcSerial, con );
      commit( con );

    } catch( Exception e ) {
      abort( con );
      throw new ServiceException( e );
    } finally {
      finalize( con );
    }
    return result;
  }

  /**
   * MISSING_COMMENT
   *
   * @param vcSerial
   * @param conn
   * @return
   * @throws ServiceException
   */
  public Geometry getVcGeometry( String vcSerial, Connection conn )
    throws ServiceException, SQLException {

    if( conn == null ) {
      throw new NullPointerException();
    }

    if( vcSerial == null ) {
      throw new NullPointerException();
    }

    final PreparedStatement ps = conn.prepareStatement( getVcGeometry );
    ps.setString( 1, vcSerial );
    final ResultSet rs = ps.executeQuery();

    Geometry result = null;
    final WKTReader reader = new WKTReader();

    if( rs.next() ) {
      final String wktGeom = rs.getString( 1 );
      final Geometry g;
      try {
        result = reader.read( wktGeom );
      } catch( ParseException e ) {
        // nothing here
      }
    }
    return result;
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param s
   * @return
   */

  private Point readPointZM( String s ) throws ParseException {
    if( s == null || s.isEmpty() ) {
      return null;
    }

    if( s.startsWith( "POINT ZM" ) ) {
      // POINT ZM (10.9997 45.4451 0 56)
      s = s.substring( 10 );
      final StringTokenizer tok = new StringTokenizer( s, " " );

      double x = 0.0, y = 0.0, z = 0.0;
      if( tok.hasMoreTokens() ) {
        try {
          x = Double.parseDouble( tok.nextToken() );
        } catch( NumberFormatException e ) {
          throw new ParseException( e );
        }
      }
      if( tok.hasMoreTokens() ) {
        try {
          y = Double.parseDouble( tok.nextToken() );
        } catch( NumberFormatException e ) {
          throw new ParseException( e );
        }
      }
      if( tok.hasMoreTokens() ) {
        try {
          z = Double.parseDouble( tok.nextToken() );
        } catch( NumberFormatException e ) {
          throw new ParseException( e );
        }
      }
      // discard the M value
      final GeometryFactory f = new GeometryFactory();
      return f.createPoint( new Coordinate( x, y, z ) );
    } else {
      return null;
    }
  }
}
