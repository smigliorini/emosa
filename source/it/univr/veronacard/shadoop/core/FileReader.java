package it.univr.veronacard.shadoop.core;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import static it.univr.veronacard.entities.VcSiteInfo.*;
import static it.univr.veronacard.shadoop.core.Step.Attributes.*;
import static java.lang.Double.*;
import static java.lang.Integer.*;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class FileReader {

  /**
   * MISSING_COMMENT
   *
   * @param hdfs
   * @param stepFile
   * @return
   */

  public static Map<String, Map<String, List<Step>>> readStepsFromCacheFile
  ( FileSystem hdfs,
    URI stepFile ) {

    if( hdfs == null ) {
      throw new NullPointerException();
    }
    if( stepFile == null ) {
      throw new NullPointerException();
    }

    final WKTReader reader = new WKTReader();
    final Map<String, Map<String, List<Step>>> result = new HashMap<>();

    try {
      try( BufferedReader br = new BufferedReader
        ( new InputStreamReader( hdfs.open( new Path( stepFile ) ) ) ) ) {
        String line;

        while( ( line = br.readLine() ) != null ) {
          processTripStepLine( line, reader, result );
        }
      }
    } catch( IOException e ) {
      // nothing here
    }
    return result;
  }


  /**
   * MISSING_COMMENT
   *
   * @param file
   * @return
   */

  public static Map<String, Map<String, List<Step>>> readStepsFromLocalFile
  ( File file ) {

    if( file == null ) {
      throw new NullPointerException();
    }

    final WKTReader reader = new WKTReader();
    final Map<String, Map<String, List<Step>>> result = new HashMap<>();

    try {
      try( BufferedReader br = new BufferedReader
        ( new InputStreamReader( new FileInputStream( file ) ) ) ) {
        String line;

        while( ( line = br.readLine() ) != null ) {
          processTripStepLine( line, reader, result );
        }
      }
    } catch( IOException e ) {
      // nothing here
    }
    return result;
  }


  /**
   * MISSING_COMMENT
   *
   * @param fs
   * @param outPath
   * @return
   */

  public static Set<TripValue> readParetoFrontFromHdfs
  ( FileSystem fs, URI outPath ) {

    if( fs == null ) {
      throw new NullPointerException();
    }
    if( outPath == null ) {
      throw new NullPointerException();
    }

    final Set<TripValue> paretoFront = new HashSet<>();

    try {
      final Path path = fs.resolvePath( new Path( outPath ) );
      final FileStatus[] fileStatus =
        fs.listStatus( path, SpatialSite.NonHiddenFileFilter );

      for( FileStatus file : fileStatus ) {
        if( file.isDirectory() ) {
          continue;
        }

        try( BufferedReader br = new BufferedReader
          ( new InputStreamReader( fs.open( file.getPath() ) ) ) ) {
          String line;
          while( ( line = br.readLine() ) != null ) {
            paretoFront.add( processTripValueLine( line ) );
          }
        } catch( IOException e ) {
          // nothing here
        }

        file.getPath();
      }
    } catch( IOException e ) {
      // nothing here
    }

    return paretoFront;
  }


  /**
   * MISSING_COMMENT
   *
   * @param fs
   * @param outPath
   * @return
   */

  public static Map<String, Map<Integer, Integer>> readStayTimes
  ( FileSystem fs, URI outPath ) {

    if( fs == null ) {
      throw new NullPointerException();
    }
    if( outPath == null ) {
      throw new NullPointerException();
    }

    final Map<String, Map<Integer, Integer>> result = new HashMap<>();

    try {
      final Path path = fs.resolvePath( new Path( outPath ) );
      final FileStatus[] fileStatus =
        fs.listStatus( path, SpatialSite.NonHiddenFileFilter );

      for( FileStatus file : fileStatus ) {
        if( file.isDirectory() ) {
          continue;
        }

        try( BufferedReader br = new BufferedReader
          ( new InputStreamReader( fs.open( file.getPath() ) ) ) ) {
          String line;
          while( ( line = br.readLine() ) != null ) {
            processStayTimeLine( line, result );
          }
        } catch( IOException e ) {
          // nothing here
        }

        file.getPath();
      }
    } catch( IOException e ) {
      // nothing here
    }
    return result;
  }


  /**
   * MISSING_COMMENT
   *
   * @param fs
   * @param outPath
   * @return
   */

  public static Map<String, Map<String, Integer>> readNumVisits
  ( FileSystem fs, URI outPath ) {

    if( fs == null ) {
      throw new NullPointerException();
    }
    if( outPath == null ) {
      throw new NullPointerException();
    }

    final Map<String, Map<String, Integer>> result = new HashMap<>();

    try {
      final Path path = fs.resolvePath( new Path( outPath ) );
      final FileStatus[] fileStatus =
        fs.listStatus( path, SpatialSite.NonHiddenFileFilter );

      for( FileStatus file : fileStatus ) {
        if( file.isDirectory() ) {
          continue;
        }

        try( BufferedReader br = new BufferedReader
          ( new InputStreamReader( fs.open( file.getPath() ) ) ) ) {
          String line;
          while( ( line = br.readLine() ) != null ) {
            processNumVisitsLine( line, result );
          }
        } catch( IOException e ) {
          // nothing here
        }

        file.getPath();
      }
    } catch( IOException e ) {
      // nothing here
    }
    return result;
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param line
   * @param reader
   * @param result
   */
  private static void processTripStepLine
  ( String line,
    WKTReader reader,
    Map<String, Map<String, List<Step>>> result ) {

    if( line == null ) {
      throw new NullPointerException();
    }

    if( reader == null ) {
      throw new NullPointerException();
    }


    final Step step = new Step();

    final StringTokenizer tk = new StringTokenizer( line, ";" );
    for( int i = 0; i < values().length && tk.hasMoreTokens(); i++ ) {
      String s = tk.nextToken();
      if( s.startsWith( "\"" ) ) {
        s = s.substring( 1 );
      }
      if( s.endsWith( "\"" ) ) {
        s = s.substring( 0, s.length() - 1 );
      }

      if( i == ORIGIN.ordinal() ) {
        step.setOrigin( s );
      } else if( i == DESTINATION.ordinal() ) {
        step.setDestination( s );
      } else if( i == TRAVEL_MODE.ordinal() ) {
        step.setTravelMode( s );
      } else if( i == PATH.ordinal() ) {
        try {
          step.setPath( reader.read( s ) );
        } catch( ParseException e ) {
          // nothing here
        }
      } else if( i == DURATION.ordinal() ) {
        try {
          final Double d = parseDouble( s );
          // value is in seconds, convert into minutes!!!
          step.setDuration( Math.round( d.intValue() / 60 ));
        } catch( NumberFormatException e ) {
          // nothing here
        }
      } else if( i == DISTANCE.ordinal() ) {
        try {
          final Double d = parseDouble( s );
          step.setDistance( d.intValue() );
        } catch( NumberFormatException e ) {
          // nothing here
        }
      }
    }

    Map<String, List<Step>> tmValue = result.get( step.getTravelMode() );
    if( tmValue == null ) {
      tmValue = new HashMap<>();
    }

    List<Step> value = tmValue.get( step.getOrigin() );
    if( value == null ) {
      value = new ArrayList<>();
    }
    value.add( step );
    tmValue.put( step.getOrigin(), value );

    result.put( step.getTravelMode(), tmValue );
  }


  /**
   * MISSING_COMMENT
   *
   * @param line
   */

  public static TripValue processTripValueLine( String line ) {
    if( line == null ) {
      throw new NullPointerException();
    }

    final TripValue value = new TripValue();
    final StringTokenizer tk = new StringTokenizer( line, "\t" );
    int i = 0;
    while( tk.hasMoreTokens() ) {
      final String curr = tk.nextToken();

      switch( i ) {
        case 0:
          // curr is the line key
          break;
        case 1:
          int numSteps;
          try {
            numSteps = parseInt( curr );
          } catch( NumberFormatException e ) {
            numSteps = 0;
          }
          value.setNumSteps( numSteps );
          break;
        case 2:
          int duration;
          try {
            // duration is in minutes!!!
            duration = parseInt( curr );
          } catch( NumberFormatException e ) {
            duration = 0;
          }
          value.setDuration( duration );
          break;
        case 3:
          int travelTime;
          try {
            travelTime = parseInt( curr );
          } catch( NumberFormatException e ) {
            travelTime = 0;
          }
          value.setTravelTime( travelTime );
          break;
        case 4:
          int waitingTime;
          try {
            waitingTime = parseInt( curr );
          } catch( NumberFormatException e ) {
            waitingTime = 0;
          }
          value.setWaitingTime( waitingTime );
          break;
        case 5:
          int distance;
          try {
            distance = parseInt( curr );
          } catch( NumberFormatException e ) {
            distance = 0;
          }
          value.setDistance( distance );
          break;
        case 6:
          final StringTokenizer ltk = new StringTokenizer( curr, "," );
          while( ltk.hasMoreTokens() ) {
            final String icurr = ltk.nextToken();
            value.addStepLabel( icurr );
          }
          break;
        case 7:
          final StringTokenizer gtk = new StringTokenizer( curr, "," );
          final WKTReader reader = new WKTReader();
          while( gtk.hasMoreTokens() ) {
            final String icurr = gtk.nextToken();
            try {
              final Geometry g = reader.read( icurr );
              value.addGeometry( g );
            } catch( ParseException e ) {
              // nothing here
            }
          }
          break;
      }
      i++;
    }
    return value;
  }


  /**
   * Stay time is returned in minutes!!!
   *
   * @param line
   */

  public static void processStayTimeLine
  ( String line,
    Map<String, Map<Integer, Integer>> collection ) {

    if( line == null ) {
      throw new NullPointerException();
    }
    if( collection == null ) {
      throw new NullPointerException();
    }


    final StringTokenizer tk = new StringTokenizer( line, "," );
    int index = 0;

    String poi = null;
    Integer numVisits = null;
    Integer stayTime = null;

    while( tk.hasMoreTokens() ) {
      switch( index ) {
        case 0:
          poi = tk.nextToken();
          index++;
          break;
        case 1:
          numVisits = parseInt( tk.nextToken() );
          index++;
          break;
        case 2:
          stayTime = parseInt( tk.nextToken() );
          index++;
          break;
        default:
          throw new IllegalStateException( "Invalid number of tokens" );

      }
    }

    Map<Integer, Integer> values = collection.get( getSiteCode( poi ) );
    if( values == null ) {
      values = new HashMap<>();
    }
    values.put( numVisits, stayTime );
    collection.put( getSiteCode( poi ), values );
  }


  /**
   * MISSING_COMMENT
   *
   * @param line
   */

  public static void processNumVisitsLine
  ( String line,
    Map<String, Map<String, Integer>> collection ) {

    if( line == null ) {
      throw new NullPointerException();
    }
    if( collection == null ) {
      throw new NullPointerException();
    }


    final StringTokenizer tk = new StringTokenizer( line, "," );
    int index = 0;

    String poi = null;
    Integer month = null;
    Integer dow = null;
    Integer hour = null;
    Integer numVisits = null;

    while( tk.hasMoreTokens() ) {
      switch( index ) {
        case 0:
          poi = tk.nextToken();
          index++;
          break;
        case 1:
          month = parseInt( tk.nextToken() );
          index++;
          break;
        case 2:
          dow = parseInt( tk.nextToken() );
          index++;
          break;
        case 3:
          hour = parseInt( tk.nextToken() );
          index++;
          break;
        case 4:
          numVisits = parseInt( tk.nextToken() );
          index++;
          break;
        default:
          throw new IllegalStateException( "Invalid number of tokens" );

      }
    }

    Map<String, Integer> values = collection.get( getSiteCode( poi ) );
    if( values == null ) {
      values = new HashMap<>();
    }
    final String key = String.format( "%s_%s_%s", month, dow, hour );
    values.put( key, numVisits );
    collection.put( getSiteCode( poi ), values );
  }
}
