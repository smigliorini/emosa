package it.univr.auditel.shadoop.core;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import it.univr.auditel.entities.GContext;
import it.univr.auditel.entities.ProgramRecord;
import it.univr.auditel.entities.UserPreference;
//new
import it.univr.auditel.entities.ChannelTransition;
//endnew
import it.univr.veronacard.shadoop.core.TripValue;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import static java.lang.Integer.parseInt;
import static java.util.Calendar.DATE;
import static java.util.Calendar.MONTH;

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
   * @param evoFile
   * @return
   */

  public static Map<GContext, Map<GContext, Double>> readGroupTypeEvolution
  ( FileSystem hdfs, URI evoFile ) {

    if( hdfs == null ) {
      throw new NullPointerException();
    }
    if( evoFile == null ) {
      throw new NullPointerException();
    }

    final Map<GContext, Map<GContext, Double>> result = new HashMap<>();

    try {
      try( BufferedReader br = new BufferedReader
        ( new InputStreamReader( hdfs.open( new Path( evoFile ) ) ) ) ) {
        String line;

        while( ( line = br.readLine() ) != null ) {
          processGroupTypeEvolutionLine( line, result );
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
   * @param hdfs
   * @param preferenceFile
   * @return
   */

  public static Map<String, List<UserPreference>> readUserPreferences
  ( FileSystem hdfs, URI preferenceFile ) {

    if( hdfs == null ) {
      throw new NullPointerException();
    }
    if( preferenceFile == null ) {
      throw new NullPointerException();
    }

    final Map<String, List<UserPreference>> result = new HashMap<>();

    try {
      try( BufferedReader br = new BufferedReader
        ( new InputStreamReader( hdfs.open( new Path( preferenceFile ) ) ) ) ) {
        String line;

        while( ( line = br.readLine() ) != null ) {
          processUserPreferenceLine( line, result );
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
   * @param hdfs
   * @param schedulingFile
   * @return
   */

  public static Map<Date, Map<String, List<ProgramRecord>>> readScheduling
  ( FileSystem hdfs, URI schedulingFile ) {

    if( hdfs == null ) {
      throw new NullPointerException();
    }

    if( schedulingFile == null ) {
      throw new NullPointerException();
    }

    final Map<Date, Map<String, List<ProgramRecord>>> result = new HashMap<>();

    try {
      try( BufferedReader br = new BufferedReader
        ( new InputStreamReader( hdfs.open( new Path( schedulingFile ) ) ) ) ) {
        String line;

        while( ( line = br.readLine() ) != null ) {
          processSchedulingLine( line, result );
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

  public static Set<ViewSequenceValue> readParetoFrontFromHdfs
  ( FileSystem fs, URI outPath ) {

    if( fs == null ) {
      throw new NullPointerException();
    }
    if( outPath == null ) {
      throw new NullPointerException();
    }

    final Set<ViewSequenceValue> paretoFront = new HashSet<>();

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
            paretoFront.add( processViewSequenceValue( line ) );
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

  //new
  /**
   * MISSING_COMMENT
   *
   * @param hdfs
   * @param channelTransitionFile
   * @return
   */

  public static Map<String, List<ChannelTransition>> readChannelTransition
  ( FileSystem hdfs, URI channelTransitionFile ) {

    if( hdfs == null ) {
      throw new NullPointerException();
    }
    if( channelTransitionFile == null ) {
      throw new NullPointerException();
    }

    final Map<String, List<ChannelTransition>> result = new HashMap<>();

    try {
      try( BufferedReader br = new BufferedReader
              ( new InputStreamReader( hdfs.open( new Path( channelTransitionFile ) ) ) ) ) {
        String line;

        while( ( line = br.readLine() ) != null ) {
          processChannelTransitionLine( line, result );
        }
      }
    } catch( IOException e ) {
      // nothing here
    }
    return result;

  }
  //newend


  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param line
   * @param result
   */

  private static void processGroupTypeEvolutionLine
  ( String line, Map<GContext, Map<GContext, Double>> result ) {

    if( line == null ) {
      throw new NullPointerException();
    }

    if( result == null ) {
      throw new NullPointerException();
    }

    final GContext source = new GContext();
    final GContext target = new GContext();
    Double probability = 0.0;

    final StringTokenizer tk = new StringTokenizer( line, "," );

    int i = 0;
    while( tk.hasMoreTokens() ) {
      if( i == 0 ) {
        final String users = tk.nextToken();
        final StringTokenizer itk = new StringTokenizer( users, "-" );
        while( itk.hasMoreTokens() ) {
          source.addAgeClass( itk.nextToken() );
        }
        i++;

      } else if( i == 1 ) {
        source.setTimeSlot( tk.nextToken() );
        i++;

      } else if( i == 2 ) {
        final String users = tk.nextToken();
        final StringTokenizer itk = new StringTokenizer( users, "-" );
        while( itk.hasMoreTokens() ) {
          target.addAgeClass( itk.nextToken() );
        }
        i++;

      } else if( i == 3 ) {
        target.setTimeSlot( tk.nextToken() );
        i++;

      } else if( i == 4 ) {
        final NumberFormat f = NumberFormat.getInstance( Locale.US );
        final String current = tk.nextToken();
        try {
          probability = f.parse( current ).doubleValue();
        } catch( ParseException e ) {
          System.out.printf( "Unable to convert number: \"%s\".%n", current );
        }
      }
    }

    Map<GContext, Double> t = result.get( source );
    if( t == null ) {
      t = new HashMap<>();
    }
    t.put( target, probability );
  }


  /**
   * MISSING_COMMENT
   *
   * @param line
   * @param preferences
   */

  private static void processUserPreferenceLine
  ( String line, Map<String, List<UserPreference>> preferences ) {

    if( line == null ) {
      throw new NullPointerException();
    }
    if( preferences == null ) {
      throw new NullPointerException();
    }

    final StringTokenizer tk = new StringTokenizer( line, "," );
    final UserPreference pref = new UserPreference();

    int i = 0;
    while( tk.hasMoreTokens() ) {
      final String current = tk.nextToken().replace( "\"", "" );

      if( i == 0 ) {
        pref.setUser( current );
        i++;

      } else if( i == 1 ) {
        pref.setFamilyId( current );
        i++;

      } else if( i == 2 ) {
        pref.setTimeSlot( current );
        i++;

      } else if( i == 3 ) {
        pref.setDayOfWeek( current );
        i++;

      } else if( i == 4 ) {
        pref.setChannelId( current );
        i++;

      } else if( i == 5 ) {
        // seconds
        i++;

      } else if( i == 6 ) {
        try {
          if( current.equals( "NaN" ) ) {
            pref.setPreference( 0.0 );
          } else {
            final NumberFormat f = NumberFormat.getInstance( Locale.US );
            final Double value = f.parse( current ).doubleValue();
            pref.setPreference( value );
          }
        } catch( ParseException e ) {
          System.out.printf
            ( "Unable to convert number \"%s\" in line \"%s\".%n",
              current, line );
        }
        i++;
      }
    }

    List<UserPreference> plist = preferences.get( pref.getUser() );
    if( plist == null ) {
      plist = new ArrayList<>();
    }
    plist.add( pref );
    preferences.put( pref.getUser(), plist );
  }


  /**
   * MISSING_COMMENT
   *
   * @param line
   * @param scheduling
   */

  private static void processSchedulingLine
  ( String line,
    Map<Date, Map<String, List<ProgramRecord>>> scheduling ) {

    if( line == null ) {
      throw new NullPointerException();
    }
    if( scheduling == null ) {
      throw new NullPointerException();
    }

    final StringTokenizer tk = new StringTokenizer( line, "," );
    final ProgramRecord record = new ProgramRecord();
    final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd hh:mm" );

    int i = 0;
    while( tk.hasMoreTokens() ) {
      final String current = tk.nextToken().replace( "\"", "" );

      if( i == 0 ) {
        // ID
        i++;

      } else if( i == 1 ) {
        record.setProgramId( current );
        i++;

      } else if( i == 2 ) {
        record.setChannelId( current );
        i++;

      } else if( i == 3 ) {
        try {
          record.setStartTime( f.parse( current ) );
        } catch( ParseException e ) {
          System.out.printf( "Unable to parse date \"%s\" in line \"%s\".", current, line );
        }
        i++;

      } else if( i == 4 ) {
        try {
          record.setEndTime( f.parse( current ) );
        } catch( ParseException e ) {
          System.out.printf( "Unable to parse date \"%s\" in line \"%s\".", current, line );
        }
        i++;
      }
    }

    Map<String, List<ProgramRecord>> map =
      scheduling.get( DateUtils.truncate( record.getStartTime(), DATE ) );
    List<ProgramRecord> list;
    if( map == null ) {
      map = new HashMap<>();
      list = new ArrayList<>();
    } else {
      list = map.get( record.getChannelId() );
      if( list == null ){
        list = new ArrayList<>();
      }
    }
    list.add( record );
    map.put( record.getChannelId(), list );
    scheduling.put( record.getStartTime(), map );
  }


  /**
   * MISSING_COMMENT
   *
   * @param line
   */

  public static ViewSequenceValue processViewSequenceValue( String line ) {
    if( line == null ) {
      throw new NullPointerException();
    }

    final ViewSequenceValue value = new ViewSequenceValue();

    final StringTokenizer tk = new StringTokenizer( line, "\t" );
    int i = 0;
    while( tk.hasMoreTokens() ) {
      final String current = tk.nextToken();

      switch( i ) {
        case 0:
          // current is the line key
          break;
        case 1:
          int duration;
          try {
            // duration is in minutes!!!
            duration = parseInt( current );
          } catch( NumberFormatException e ) {
            duration = 0;
          }
          value.setDuration( duration );
          break;
        case 2:
          final StringTokenizer ctk = new StringTokenizer( current, "," );
          while( ctk.hasMoreTokens() ) {
            final String icurr = ctk.nextToken();
            value.addChannel( icurr );
          }
          break;
        case 3:
          final StringTokenizer ptk = new StringTokenizer( current, "," );
          while( ptk.hasMoreTokens() ) {
            final String icurr = ptk.nextToken();
            final int index = icurr.indexOf( "-" );
            final String u = icurr.substring( 0, index );
            final Double p = Double.parseDouble( icurr.substring( index + 1 ) );
            value.addUserPreference( u, p );
          }
          break;
        case 4:
          final StringTokenizer mtk = new StringTokenizer( current, "," );
          while( mtk.hasMoreTokens() ) {
            final String icurr = mtk.nextToken();
            final int index = icurr.indexOf( "-" );
            final String u = icurr.substring( 0, index );
            final Integer m = Integer.parseInt( icurr.substring( index + 1 ) );
            value.addMissedProgramSeconds( u, m );
          }
          break;

      }
      i++;
    }
    return value;
  }

  //new
  /**
   * MISSING_COMMENT
   *
   * @param line
   * @param transitions
   */

  private static void processChannelTransitionLine
  ( String line, Map<String, List<ChannelTransition>> transitions ) {

    if( line == null ) {
      throw new NullPointerException();
    }
    if( transitions == null ) {
      throw new NullPointerException();
    }

    final StringTokenizer tk = new StringTokenizer( line, "," );
    final ChannelTransition trans = new ChannelTransition();

    int i = 0;
    while( tk.hasMoreTokens() ) {
      final String current = tk.nextToken().replace( "\"", "" );

      if( i == 0 ) {
        trans.setChannelId1( current );
        i++;

      } else if( i == 1 ) {
        trans.setChannelId2( current );
        i++;

      } else if( i == 2 ) {
        try {
          if( current.equals( "NaN" ) ) {
            trans.setPreferenceTransition( 0.0 );
          } else {
            final NumberFormat f = NumberFormat.getInstance( Locale.US );
            final Double value = f.parse( current ).doubleValue();
            trans.setPreferenceTransition( value );
          }
        } catch( ParseException e ) {
          System.out.printf
                  ( "Unable to convert number \"%s\" in line \"%s\".%n",
                          current, line );
        }
        i++;
      }
    }

    List<ChannelTransition> ptlist = transitions.get( trans.getChannelId1() );
    if( ptlist == null ) {
      ptlist = new ArrayList<>();
    }
    ptlist.add( trans );
    transitions.put( trans.getChannelId1(), ptlist );
  }
  //newend

}
