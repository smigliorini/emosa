package it.univr.veronacard;

import it.univr.veronacard.entities.TravelPath;
import it.univr.veronacard.entities.VCard;
import it.univr.veronacard.entities.VcProfile;
import it.univr.veronacard.entities.VcSite;
import it.univr.veronacard.services.ServiceException;
import it.univr.veronacard.services.UnreachableServiceException;
import it.univr.veronacard.services.VcDataService;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static it.univr.veronacard.services.CsvService.buildVcRecord;
import static it.univr.veronacard.services.CsvService.readCsvFile;
import static it.univr.veronacard.services.JsonService.*;
import static it.univr.veronacard.services.LocationService.retrievePath;
import static it.univr.veronacard.services.VcDataService.*;
import static java.lang.String.format;

/**
 * This class executes the programs that populates the database with the data
 * contained in the VeronaCard log files and the google maps json files.
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class LoadVcToDatabase {

  // === Properties ============================================================

  private static String vcServerAddress = "http://vrcard.csgalileo.org/api";
  private static String vcSites = "%s/sites";
  private static String vcProfiles = "%s/profiles";

  private static String[] vcRecords = new String[]{
    "data/dati_2014.csv",
    "data/dati_2015.csv",
    "data/dati_2016.csv",
    "data/dati_2017.csv",
  };

  // === Methods ===============================================================

  public static void main( String[] args ) {
    final VcDataService ds = new VcDataService();

    createTables( ds );
    addGeometryColumn( ds );
    final List<VcSite> sites = readVcSite();
    insertVcSites( ds, sites );
    final List<VcProfile> profiles = readVcProfile();
    insertVcProfiles( ds, profiles );


    final List<VCard> records = readVcRecords( vcRecords, 9 );
    insertVcRecords( ds, records );

    populateGeometryColumn( ds );
    buildVcTrajSerial( ds );

    downloadPaths( ds );
    downloadAdditionalPaths( ds );

    final List<TravelPath> paths = processPaths();
    savePaths( ds, paths );
    final List<TravelPath> scenicRoutes = processAdditionalPaths();
    savePaths( ds, scenicRoutes );

    buildVcTrajGoogle( ds );


    /*downloadAdditionalPaths();
    final List<TravelPath> scenicRoutes = processAdditionalPaths();
    for( TravelPath p : scenicRoutes ){
      int duration = 0;
      int distance = 0;
      for( TravelStep s : p.getSteps() ){
        duration += s.getDuration();
        distance += s.getDistance();
      }

      System.out.printf( "%s\t%s\t%s\t%s\t%s\t%s%n",
                         p.getOriginId(),
                         p.getDestinationId(),
                         p.getTravelMode(),
                         p.getPolyline().toText(),
                         duration,
                         distance );
    }//*/

  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param ds
   */

  private static void createTables( VcDataService ds ) {
    if( ds == null ) {
      throw new NullPointerException();
    }

    try {
      System.out.printf( "Start creating table \"vc_site\"...%n" );
      ds.executeQuery( createVcSiteTable );
      System.out.printf( "Successfully created table \"vc_site\"%n" );

      System.out.printf( "Start creating table \"vc_profile\"...%n" );
      ds.executeQuery( createVcProfileTable );
      System.out.printf( "Successfully created table \"vc_profile\"%n" );

      System.out.printf( "Start creating table \"verona_card\"...%n" );
      ds.executeQuery( createVcTable );
      System.out.printf( "Successfully created table \"verona_card\"%n" );

      System.out.printf( "Start creating table \"vc_ticket\"...%n" );
      ds.executeQuery( createVcTicketTable );
      System.out.printf( "Successfully created table \"vc_ticket\"%n" );

      System.out.printf( "Start creating table \"vc_traj_serial\"...%n" );
      ds.executeQuery( createTrajSerialTable );
      System.out.printf( "Successfully created table \"vc_traj_serial\"%n" );

      System.out.printf( "Start creating table \"g_travel_path\"...%n" );
      ds.executeQuery( createPathTable );
      System.out.printf( "Successfully created table \"g_travel_path\"%n" );

      System.out.printf( "Start creating table \"g_travel_path_step\"...%n" );
      ds.executeQuery( createPathStepTable );
      System.out.printf( "Successfully created table \"g_travel_path_step\"%n" );

      System.out.printf( "Start creating table \"vc_traj_google\"...%n" );
      ds.executeQuery( createTrajGoogleTable );
      System.out.printf( "Successfully created table \"vc_traj_google\"%n" );


    } catch( ServiceException e ) {
      System.out.printf
        ( "Unable to create database tables: %s",
          e.getMessage() );
      System.exit( 1 );
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param ds
   */

  private static void addGeometryColumn( VcDataService ds ) {
    if( ds == null ) {
      throw new NullPointerException();
    }

    try {
      System.out.printf
        ( "Start adding geometric column to table \"vc_site\"...%n" );
      ds.executeQuery( addVcSitePointColumn );
      System.out.printf
        ( "Successfully added geometric column to table \"vc_site\"%n" );

      System.out.printf
        ( "Start adding geometric columns to table \"vc_ticket\"...%n" );
      ds.executeQuery( addVcTicketPointColumn );
      System.out.printf
        ( "Successfully added geometric column to table \"vc_ticket\"%n" );

      System.out.printf
        ( "Start adding geometric columns to table \"vc_traj_serial\"...%n" );
      ds.executeQuery( addTrajSerialLinestringColumn );
      System.out.printf
        ( "Successfully added geometric column to table \"vc_traj_serial\"%n" );

      System.out.printf
        ( "Start adding geometric columns to table \"g_travel_path_step\"...%n" );
      ds.executeQuery( addPathStepStartLocColumn );
      ds.executeQuery( addPathStepEndLocColumn );
      ds.executeQuery( addPathStepLineColumn );
      System.out.printf
        ( "Successfully added geometric columns to table \"g_travel_path_step\"%n" );

      System.out.printf
        ( "Start adding geometric column to table \"g_travel_path\"...%n" );
      ds.executeQuery( addPathLineColumn );
      System.out.printf
        ( "Successfully added geometric column to table \"g_travel_path\"%n" );

      System.out.printf
        ( "Start adding geometric column to table \"vc_traj_google\"...%n" );
      ds.executeQuery( addTrajGoogleLinestringColumn );
      System.out.printf
        ( "Successfully added geometric column to table \"vc_traj_google\"%n" );

    } catch( ServiceException e ) {
      System.out.printf
        ( "Unable to add geometric columns to database tables: %s",
          e.getMessage() );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @return
   */

  private static List<VcSite> readVcSite() {
    final String sitesUrl = format( vcSites, vcServerAddress );
    try {
      System.out.printf( "Start retrieving vc_sites from web...%n" );
      final String sitesJson = readJsonFile( sitesUrl );
      final List<VcSite> siteList = buildVcSites( sitesJson );
      System.out.printf( "Successfully retrieved vc_sites from web%n" );
      return siteList;

    } catch( UnreachableServiceException e ) {
      System.out.printf( "Service \"%s\" is unreachable", sitesUrl );
      return Collections.emptyList();
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @return
   */

  private static List<VcProfile> readVcProfile() {
    final String profilesUrl = format( vcProfiles, vcServerAddress );
    try {
      System.out.printf( "Start retrieving vc_profiles from web...%n" );
      final String profilesJson = readJsonFile( profilesUrl );
      final List<VcProfile> profileList = buildVcProfiles( profilesJson );
      System.out.printf( "Successfully retrieved vc_sites from web%n" );
      return profileList;

    } catch( UnreachableServiceException e ) {
      System.out.printf( "Service \"%s\" is unreachable", profilesUrl );
      return Collections.emptyList();
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param ds
   * @param sites
   */

  private static void insertVcSites( VcDataService ds, List<VcSite> sites ) {
    if( ds == null ) {
      throw new NullPointerException();
    }
    if( sites == null ) {
      throw new NullPointerException();
    }

    if( sites != null && !sites.isEmpty() ) {
      try {
        System.out.printf( "Start saving vc_sites into the database...%n" );
        ds.insertVcSites( sites );
        System.out.printf( "Successfully saved vc_sites into the database%n" );

      } catch( ServiceException e ) {
        System.out.printf( "Unable to save vc_sites into the database%n" );
      }
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param ds
   * @param profiles
   */

  private static void insertVcProfiles( VcDataService ds, List<VcProfile> profiles ) {
    if( ds == null ) {
      throw new NullPointerException();
    }
    if( profiles == null ) {
      throw new NullPointerException();
    }

    if( profiles != null && !profiles.isEmpty() ) {
      try {
        System.out.printf( "Start saving vc_profiles into the database...%n" );
        ds.insertVcProfiles( profiles );
        System.out.printf( "Successfully saved vc_profiles into the database%n" );

      } catch( ServiceException e ) {
        System.out.printf( "Unable to save vc_profiles into the database%n" );
      }
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param paths
   * @return
   */

  private static List<VCard> readVcRecords( String[] paths, int numColumns ) {
    if( paths == null ) {
      throw new NullPointerException();
    }

    final List<Map<Integer, String>> tuples = new ArrayList<>();
    for( String p : paths ) {
      System.out.printf( "Start reading file \"%s\"...%n", p );
      tuples.addAll( readCsvFile( p, numColumns ) );
      System.out.printf( "End reading file \"%s\": found %s tuples%n", p, tuples.size() );
    }

    System.out.printf( "Start building verona_cards...%n" );
    final List<VCard> cards = buildVcRecord( tuples );
    System.out.printf( "End building verona_cards: built %s objects%n", cards.size() );
    return cards;
  }

  /**
   * MISSING_COMMENT
   *
   * @param ds
   * @param cards
   */

  private static void insertVcRecords( VcDataService ds, List<VCard> cards ) {
    if( ds == null ) {
      throw new NullPointerException();
    }
    if( cards == null ) {
      throw new NullPointerException();
    }

    if( cards != null && !cards.isEmpty() ) {
      try {
        System.out.printf( "Start saving vc_card records into the database...%n" );
        ds.insertVeronaCardRecords( cards );
        System.out.printf( "Successfully saved vc_card records into the database%n" );

      } catch( ServiceException e ) {
        System.out.printf( "Unable to save vc_card records into the database: %s%n", e.getMessage() );
      }
    }

  }

  /**
   * MISSING_COMMENT
   *
   * @param ds
   */
  private static void populateGeometryColumn( VcDataService ds ) {
    if( ds == null ) {
      throw new NullPointerException();
    }

    try {
      System.out.printf( "Start populating \"vc_site\" geometry...%n" );
      ds.executeQuery( buildVcSitePoint );
      System.out.printf( "Successfully populated \"vc_site\" geometry%n" );
    } catch( ServiceException e ) {
      System.out.printf( "Unable to populate \"vc_site\" table: %s%n", e.getMessage() );
    }

    try {
      System.out.printf( "Start populating \"vc_ticket\" geometry...%n" );
      ds.executeQuery( buildVcTicketPoint );
      System.out.printf( "Successfully populated \"vc_ticket\" geometry%n" );
    } catch( ServiceException e ) {
      System.out.printf( "Unable to populate \"vc_ticket\" table: %s%n", e.getMessage() );
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param ds
   */
  private static void buildVcTrajSerial( VcDataService ds ) {
    if( ds == null ) {
      throw new NullPointerException();
    }

    try {
      System.out.printf( "Start populating \"vc_traj_serial\" geometry...%n" );
      ds.buildVcTrajSerial();
      System.out.printf( "Successfully populated \"vc_traj_serial\" geometry%n" );
    } catch( ServiceException e ) {
      System.out.printf( "Unable to make trajectories into the database: %s%n", e.getMessage() );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param ds
   */

  private static void downloadPaths( VcDataService ds ) {
    if( ds == null ) {
      throw new NullPointerException();
    }

    try {
      final Map<String, Class> attributes = new HashMap<>( 1 );
      attributes.put( "name_short", String.class );
      attributes.put( "id", String.class );

      final List<Map<String, Object>> sites =
        ds.executeSelectQuery( selectVcSites, attributes, null );

      if( sites.size() > 1 ) {
        for( int i = 0; i < sites.size(); i++ ) {
          for( int j = 0; j < sites.size(); j++ ) {
            if( i != j ) {
              final String orig =
                sites.get( i ).get( "name_short" ).toString();
              final String origId =
                sites.get( i ).get( "id" ).toString();
              final String dest =
                sites.get( j ).get( "name_short" ).toString();
              final String destId =
                sites.get( j ).get( "id" ).toString();
              final String directory = "data/paths";

              retrievePath( orig, dest, origId, destId, directory );
              System.out.printf( "Compute path from %s to %s%n", orig, dest );
            }
          }
        }
      }

    } catch( ServiceException e ) {
      System.out.printf( "Unable to compute trajectories with Google API: %s%n", e.getMessage() );
    }
  }


  /**
   * MISSING_COMMENT
   */

  private static void downloadAdditionalPaths( VcDataService ds ) {
    final String directory = "data/scenic_routes";

    retrievePath( "Piazza Bra", "Piazza delle Erbe", "PB", "PE", directory );
    retrievePath( "Piazza delle Erbe", "Piazza Bra", "PE", "PB", directory );
    retrievePath( "Piazza Bra", "Porta Borsari", "PB", "PTB", directory );
    retrievePath( "Porta Borsari", "Piazza Bra", "PTB", "PB", directory );
    retrievePath( "Piazza delle Erbe", "Porta Borsari", "PE", "PTB", directory );
    retrievePath( "Porta Borsari", "Piazza delle Erbe", "PTB", "PE", directory );

    final Map<String, Class> attributes = new HashMap<>( 1 );
    attributes.put( "name_short", String.class );
    attributes.put( "id", String.class );

    final List<Map<String, Object>> sites;
    try {
      sites = ds.executeSelectQuery( selectVcSites, attributes, null );


      if( sites.size() > 1 ) {
        for( int i = 0; i < sites.size(); i++ ) {
          retrievePath( "Piazza Bra",
                        sites.get( i ).get( "name_short" ).toString(),
                        "PB",
                        sites.get( i ).get( "id" ).toString(),
                        directory );
          retrievePath( "Piazza delle Erbe",
                        sites.get( i ).get( "name_short" ).toString(),
                        "PE",
                        sites.get( i ).get( "id" ).toString(),
                        directory );
          retrievePath( "Porta Borsari",
                        sites.get( i ).get( "name_short" ).toString(),
                        "PTB",
                        sites.get( i ).get( "id" ).toString(),
                        directory );
          retrievePath( sites.get( i ).get( "name_short" ).toString(),
                        "Piazza Bra",
                        sites.get( i ).get( "id" ).toString(),
                        "PB",
                        directory );
          retrievePath( sites.get( i ).get( "name_short" ).toString(),
                        "Piazza delle Erbe",
                        sites.get( i ).get( "id" ).toString(),
                        "PE",
                        directory );
          retrievePath( sites.get( i ).get( "name_short" ).toString(),
                        "Porta Borsari",
                        sites.get( i ).get( "id" ).toString(),
                        "PTB",
                        directory );
        }
      }

    } catch( ServiceException e ) {
      System.out.printf( "Unable to compute trajectories with Google API: %s%n", e.getMessage() );
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @return
   */

  private static List<TravelPath> processPaths() {
    final File folder = new File( "data/paths" );
    final File[] files = folder.listFiles();

    final List<TravelPath> paths = new ArrayList<>();
    for( File f : files ) {
      System.out.printf
        ( "Start processing path file \"%s\"%n", f.getName() );
      paths.add( processPath( f ) );
      System.out.printf
        ( "Successfully complete processing of file \"%s\"%n", f.getName() );
    }
    return paths;
  }


  /**
   * MISSING_COMMENT
   *
   * @return
   */

  private static List<TravelPath> processAdditionalPaths() {
    final File folder = new File( "data/scenic_routes" );
    final File[] files = folder.listFiles();

    final List<TravelPath> paths = new ArrayList<>();
    for( File f : files ) {
      System.out.printf
        ( "Start processing path file \"%s\"%n", f.getName() );
      paths.add( processPath( f ) );
      System.out.printf
        ( "Successfully complete processing of file \"%s\"%n", f.getName() );
    }
    return paths;
  }


  /**
   * MISSING_COMMENT
   *
   * @param ds
   */

  private static void savePaths( VcDataService ds, List<TravelPath> paths ) {
    if( ds == null ) {
      throw new NullPointerException();
    }
    if( paths == null ) {
      throw new NullPointerException();
    }

    try {
      System.out.printf( "Start populating \"g_travel_path\" and "
                         + "\"g_travel_path_step\" tables...%n" );
      ds.insertTravelPaths( paths );
      System.out.printf( "Successfully populated \"g_travel_path\" and"
                         + "\"g_travel_path_step\" tables%n" );
    } catch( ServiceException e ) {
      System.out.printf( "Unable to insert paths into the database: %s%n",
                         e.getMessage() );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param ds
   */

  private static void buildVcTrajGoogle( VcDataService ds ) {
    if( ds == null ) {
      throw new NullPointerException();
    }

    System.out.printf
      ( "Start populating \"vc_traj_google\"...%n" );
    try {
      ds.buildVcTrajGoogle();
      System.out.printf
        ( "Successfully complete population of \"vc_traj_google\"%n" );
    } catch( ServiceException e ) {
      System.out.printf
        ( "Unable to insert trajectories into the table \"vc_traj_google\": %s%n",
          e.getMessage() );
    }


  }
}
