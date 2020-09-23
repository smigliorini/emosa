package it.univr.veronacard;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import it.univr.veronacard.services.ServiceException;
import it.univr.veronacard.services.VcDataService;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import static it.univr.veronacard.ProcessTrsaStats.maxDuration;
import static it.univr.veronacard.shadoop.core.TripValue.computesAngularAttributes;
import static it.univr.utils.Statistics.computeMean;
import static it.univr.utils.Statistics.computeStandardDeviation;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class ProcessTrsaStats {

  public static final String input =
    //"C:\\workspace\\projects\\veronacard_analysis\\out\\"
    //+ "output_castelvecchio\\1511331858722_trsa\\part-r-00000";
    "D:\\workspace\\projects\\veronacard_analysis\\test\\trsa_output\\"
    + "run_100_castelvecchio\\dynamic_pert_100_temp_10_perc_50\\output\\"
    + "1531494266871_trsa\\part-r-00000";

  public static final String outDir =
    "C:\\workspace\\projects\\veronacard_analysis\\test\\trsa_output\\run_100_castelvecchio\\";

  public static final String outFileName = "castelvecchio_perc_50.csv";

  public static final int maxDuration = 360;

  // ===========================================================================

  private static final int serialIndex = 1;
  private static final int stepsIndex = 2;
  private static final int stepsGeomIndex = 3;
  private static final int travelModeIndex = 4;
  private static final int profileIndex = 5;
  private static final int numStepsIndex = 6;
  private static final int durationIndex = 7;
  private static final int travelTimeIndex = 8;
  private static final int distanceIndex = 9;
  private static final int numScenicRoutesIndex = 10;
  private static final int smoothnessMeanIndex = 11;
  private static final int smoothnessStnDevIndex = 12;
  private static final int arrivalHoursIndex = 13;

  // ===========================================================================

  public static void main( String[] args )
    throws ServiceException, FileNotFoundException, ParseException {

    final List<String> lines = readLines( new File( input ), false );
    final Map<String, List<Record>> records = new HashMap<>();
    for( String l : lines ) {
      processLine( l, records );
    }//*/

    readDbData( records );

    final StringBuilder b = new StringBuilder();
    b.append( Record.getHeader() );

    for( List<Record> l : records.values() ) {
      for( Record r : l ) {
        b.append( r.toString() );
      }
    }

    final String filepath = outDir + outFileName;
    final PrintWriter indexWriter = new PrintWriter( filepath );
    indexWriter.write( b.toString() );
    indexWriter.close();
  }


  // ===========================================================================

  private static void readDbData( Map<String, List<Record>> records )
    throws ServiceException, ParseException {

    if( records == null ) {
      throw new NullPointerException();
    }

    final VcDataService ds = new VcDataService();
    final WKTReader reader = new WKTReader();

    int i = 0;

    for( String key : records.keySet() ) {
      final String query =
        "select id, vc_serial, next_time_walk, next_dist_walk, arriving_date "
        + "from vc_ticket "
        + "where vc_serial = ? "
        + "order by arriving_date";


      final Map<String, Class> attributes = new HashMap<>();
      attributes.put( "id", Integer.class );
      attributes.put( "vc_serial", String.class );
      attributes.put( "next_time_walk", Double.class );
      attributes.put( "next_dist_walk", Double.class );
      attributes.put( "arriving_date", java.sql.Date.class );


      final List<String> parameters = new ArrayList<>();
      parameters.add( key );

      final List<Map<String, Object>> result =
        ds.executeSelectQuery( query, attributes, parameters );

      final int numPois = result.size();

      double distance = 0;
      for( Map<String, Object> v : result ) {
        distance += v.get( "next_dist_walk" ) != null ? Double.parseDouble( v.get( "next_dist_walk" ).toString() ) : 0;
      }

      double travelTime = 0;
      for( Map<String, Object> v : result ) {
        travelTime += v.get( "next_time_walk" ) != null ? Double.parseDouble( v.get( "next_time_walk" ).toString() ) : 0;
      }

      Date firstDate = null, lastDate = null;
      for( Map<String, Object> v : result ) {
        final Date d = (Date) v.get( "arriving_date" );
        if( firstDate == null || firstDate.after( d ) ) {
          firstDate = d;
        }

        if( lastDate == null || lastDate.before( d ) ) {
          lastDate = d;
        }
      }

      final String gquery =
        "select vc_serial, ST_AsText( geom ) as geom "
        + "from vc_traj_serial "
        + "where vc_serial = ? ";

      final Map<String, Class> gattributes = new HashMap<>();
      gattributes.put( "vc_serial", String.class );
      gattributes.put( "geom", String.class );

      final List<String> gparameters = new ArrayList<>();
      gparameters.add( key );

      final List<Map<String, Object>> gresult =
        ds.executeSelectQuery( gquery, gattributes, gparameters );

      final List<Geometry> gl = new ArrayList<>();
      for( Map<String, Object> v : gresult ) {
        final String g = (String) v.get( "geom" );
        gl.add( reader.read( g ) );
      }

      final List<Double> angleList = computesAngularAttributes( gl );
      final double mean = computeMean( angleList );
      final double stnDev = computeStandardDeviation( angleList );


      for( Record r : records.get( key ) ) {
        r.setOriginalPois( numPois );
        r.setOriginalTravelTime( travelTime / 60 );
        r.setOriginalDuration( ( lastDate.getTime() - firstDate.getTime() ) / 60000 );
        r.setOriginalTravelDistance( distance );
        r.setOriginalScenicRoutes( 0 );
        r.setOriginalSmoothMean( mean );
        r.setOriginalSmoothStnDev( stnDev );
      }

      System.out.printf( "Processed key %s of %s%n", i + 1, records.keySet().size() );
      i++;
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param line
   * @return
   */

  private static void processLine
  ( String line,
    Map<String, List<Record>> records ) {

    if( line == null ) {
      throw new NullPointerException();
    }

    final StringTokenizer tk = new StringTokenizer( line, "\t" );

    int currIndex = 0;
    String serial = null;
    String numPois = null;
    String duration = null;
    String travelTime = null;
    String travelDistance = null;
    String numScenicRoutes = null;
    String smoothnessMean = null;
    String smoothnessStnDev = null;

    while( tk.hasMoreTokens() ) {
      String current = tk.nextToken();
      if( currIndex == serialIndex ) {
        serial = current;
      } else if( currIndex == numStepsIndex ) {
        numPois = current + 1;
      } else if( currIndex == durationIndex ) {
        duration = current;
      } else if( currIndex == travelTimeIndex ) {
        travelTime = current;
      } else if( currIndex == distanceIndex ) {
        travelDistance = current;
      } else if( currIndex == numScenicRoutesIndex ) {
        numScenicRoutes = current;
      } else if( currIndex == smoothnessMeanIndex ) {
        smoothnessMean = current;
      } else if( currIndex == smoothnessStnDevIndex ) {
        smoothnessStnDev = current;
      }

      currIndex++;
    }

    if( serial != null ) {
      List<Record> list = records.get( serial );
      if( list == null ) {
        list = new ArrayList<>();
      }

      final Record r = new Record( serial );
      r.setTrsaPois( parseInt( numPois ) );
      r.setTrsaDuration( parseDouble( duration ) );
      r.setTrsaTravelTime( parseDouble( travelTime ) );
      r.setTrsaTravelDistance( parseDouble( travelDistance ) );
      r.setTrsaScenicRoutes( parseDouble( numScenicRoutes ) );
      r.setTrsaSmoothMean( parseDouble( smoothnessMean ) );
      r.setTrsaSmoothDev( parseDouble( smoothnessStnDev ) );

      list.add( r );
      records.put( serial, list );
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param file
   * @param header
   * @return
   */

  private static List<String> readLines( File file, boolean header ) {
    final List<String> lines = new ArrayList<>();

    try( BufferedReader br = new BufferedReader( new FileReader( file ) ) ) {
      String line;
      int lineCount = 0;

      while( ( line = br.readLine() ) != null ) {
        if( ( !header || lineCount > 0 ) && line.trim().length() > 0 ) {
          lines.add( line );
        }
        lineCount++;
      }
    } catch( FileNotFoundException e ) {
      System.out.printf( "File \"%s\" not found.%n", file );
    } catch( IOException e ) {
      System.out.printf( "Unable to read file \"%s\".%n", file );
    }

    return lines;
  }
}

// ===========================================================================

class Record {

  final static String delimiter = ",";

  private String serial;

  private int originalPois;
  private int trsaPois;

  private double originalDuration;
  private double trsaDuration;

  private double originalTravelTime;
  private double trsaTravelTime;

  private double originalTravelDistance;
  private double trsaTravelDistance;

  private double originalScenicRoutes;
  private double trsaScenicRoutes;

  private double originalSmoothMean;
  private double trsaSmoothMean;

  private double originalSmoothStnDev;
  private double trsaSmoothDev;

  public Record( String serial ) {
    this.serial = serial;
  }

  public String getSerial() {
    return serial;
  }

  public void setSerial( String serial ) {
    this.serial = serial;
  }

  public int getOriginalPois() {
    return originalPois;
  }

  public void setOriginalPois( int originalPois ) {
    this.originalPois = originalPois;
  }

  public int getTrsaPois() {
    return trsaPois;
  }

  public void setTrsaPois( int trsaPois ) {
    this.trsaPois = trsaPois;
  }

  public double getOriginalDuration() {
    return originalDuration;
  }

  public void setOriginalDuration( double originalDuration ) {
    this.originalDuration = originalDuration;
  }

  public double getTrsaDuration() {
    return trsaDuration;
  }

  public void setTrsaDuration( double trsaDuration ) {
    this.trsaDuration = trsaDuration;
  }

  public double getOriginalTravelDistance() {
    return originalTravelDistance;
  }

  public void setOriginalTravelDistance( double originalTravelDistance ) {
    this.originalTravelDistance = originalTravelDistance;
  }

  public double getTrsaTravelDistance() {
    return trsaTravelDistance;
  }

  public void setTrsaTravelDistance( double trsaTravelDistance ) {
    this.trsaTravelDistance = trsaTravelDistance;
  }

  public double getOriginalTravelTime() {
    return originalTravelTime;
  }

  public void setOriginalTravelTime( double originalTravelTime ) {
    this.originalTravelTime = originalTravelTime;
  }

  public double getTrsaTravelTime() {
    return trsaTravelTime;
  }

  public void setTrsaTravelTime( double trsaTravelTime ) {
    this.trsaTravelTime = trsaTravelTime;
  }

  public double getOriginalScenicRoutes() {
    return originalScenicRoutes;
  }

  public void setOriginalScenicRoutes( double originalScenicRoutes ) {
    this.originalScenicRoutes = originalScenicRoutes;
  }

  public double getTrsaScenicRoutes() {
    return trsaScenicRoutes;
  }

  public void setTrsaScenicRoutes( double trsaScenicRoutes ) {
    this.trsaScenicRoutes = trsaScenicRoutes;
  }

  public double getOriginalSmoothMean() {
    return originalSmoothMean;
  }

  public void setOriginalSmoothMean( double originalSmoothMean ) {
    this.originalSmoothMean = originalSmoothMean;
  }

  public double getTrsaSmoothMean() {
    return trsaSmoothMean;
  }

  public void setTrsaSmoothMean( double trsaSmoothMean ) {
    this.trsaSmoothMean = trsaSmoothMean;
  }

  public double getOriginalSmoothStnDev() {
    return originalSmoothStnDev;
  }

  public void setOriginalSmoothStnDev( double originalSmoothStnDev ) {
    this.originalSmoothStnDev = originalSmoothStnDev;
  }

  public double getTrsaSmoothDev() {
    return trsaSmoothDev;
  }

  public void setTrsaSmoothDev( double trsaSmoothDev ) {
    this.trsaSmoothDev = trsaSmoothDev;
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder();
    b.append( serial );
    b.append( delimiter );

    b.append( originalPois );
    b.append( delimiter );
    b.append( trsaPois );
    b.append( delimiter );
    b.append( trsaPois - originalPois > 0 ? 1 : 0 );
    b.append( delimiter );

    b.append( originalDuration );
    b.append( delimiter );
    b.append( trsaDuration );
    b.append( delimiter );
    final int bd =
      trsaDuration <= maxDuration && originalDuration > maxDuration ? 1 :
        trsaDuration > maxDuration && originalDuration <= maxDuration ? 0 :
          trsaDuration > maxDuration && originalDuration > maxDuration && trsaDuration < originalDuration ? 1 :
            trsaDuration <= maxDuration && originalDuration <= maxDuration && trsaDuration > originalDuration ? 1 : 0;
    b.append( bd );
    b.append( delimiter );

    b.append( originalTravelTime );
    b.append( delimiter );
    b.append( trsaTravelTime );
    b.append( delimiter );
    b.append( trsaTravelTime < originalTravelTime ? 1 : 0 );
    b.append( delimiter );

    b.append( originalTravelDistance );
    b.append( delimiter );
    b.append( trsaTravelDistance );
    b.append( delimiter );
    b.append( trsaTravelDistance < originalTravelDistance ? 1 : 0 );
    b.append( delimiter );

    b.append( originalScenicRoutes );
    b.append( delimiter );
    b.append( trsaScenicRoutes );
    b.append( delimiter );
    b.append( originalScenicRoutes < trsaScenicRoutes ? 1 : 0 );
    b.append( delimiter );

    b.append( originalSmoothMean );
    b.append( delimiter );
    b.append( originalSmoothStnDev );
    b.append( delimiter );
    b.append( trsaSmoothMean );
    b.append( delimiter );
    b.append( trsaSmoothDev );
    b.append( delimiter );
    b.append( trsaSmoothMean > originalSmoothMean &&
              trsaSmoothDev < originalSmoothStnDev ? 1 : 0 );
    b.append( delimiter );

    b.append( String.format( "%n" ) );

    return b.toString();
  }

  /**
   * MISSING_COMMENT
   *
   * @return
   */

  public static String getHeader() {
    final StringBuilder b = new StringBuilder();

    b.append( "serial" );
    b.append( delimiter );

    b.append( "originalPois" );
    b.append( delimiter );
    b.append( "trsaPois" );
    b.append( delimiter );
    b.append( "improved steps" );
    b.append( delimiter );

    b.append( "originalDuration" );
    b.append( delimiter );
    b.append( "trsaDuration" );
    b.append( delimiter );
    b.append( "improved duration" );
    b.append( delimiter );

    b.append( "originalTravelTime" );
    b.append( delimiter );
    b.append( "trsaTravelTime" );
    b.append( delimiter );
    b.append( "improved travel time" );
    b.append( delimiter );

    b.append( "originalTravelDistance" );
    b.append( delimiter );
    b.append( "trsaTravelDistance" );
    b.append( delimiter );
    b.append( "improved travel distance" );
    b.append( delimiter );

    b.append( "originalScenicRoutes" );
    b.append( delimiter );
    b.append( "trsaScenicRoutes" );
    b.append( delimiter );
    b.append( "improved scenic routes" );
    b.append( delimiter );

    b.append( "originalSmoothMean" );
    b.append( delimiter );
    b.append( "originalSmoothStnDev" );
    b.append( delimiter );
    b.append( "trsaSmoothMean" );
    b.append( delimiter );
    b.append( "trsaSmoothDev" );
    b.append( delimiter );
    b.append( "improved smoothness" );
    b.append( delimiter );

    b.append( String.format( "%n" ) );

    return b.toString();
  }

}
