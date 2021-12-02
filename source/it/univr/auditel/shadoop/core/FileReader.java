package it.univr.auditel.shadoop.core;

import it.univr.auditel.entities.GContext;
import it.univr.auditel.entities.ProgramRecord;
import it.univr.auditel.entities.UserPreference;
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
   * @param preferenceFile
   * @return
   */

  public static Map<String,Map<String,Double>> readGenreSequencePreferences
  ( FileSystem hdfs, URI preferenceFile ){

    if( hdfs == null ) {
      throw new NullPointerException();
    }
    if( preferenceFile == null ) {
      throw new NullPointerException();
    }

    final Map<String,Map<String,Double>> result = new HashMap<>();

    try {
      try( BufferedReader br = new BufferedReader
        ( new InputStreamReader( hdfs.open( new Path( preferenceFile ) ) ) ) ) {
        String line;

        while( ( line = br.readLine() ) != null ) {
          processGenreSequencePreferenceLine( line, result );
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

  public static Map<Long, Map<String, List<ProgramRecord>>> readScheduling
  ( FileSystem hdfs, URI schedulingFile ) {

    if( hdfs == null ) {
      throw new NullPointerException();
    }

    if( schedulingFile == null ) {
      throw new NullPointerException();
    }

    final Map<Long, Map<String, List<ProgramRecord>>> result = new HashMap<>();

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
   * @param hdfs
   * @param schedulingFile
   * @return
   */

  public static Map<Long, Map<String, List<ProgramRecord>>> readVisitingTime
  ( FileSystem hdfs, URI schedulingFile ) {

    if( hdfs == null ) {
      throw new NullPointerException();
    }

    if( schedulingFile == null ) {
      throw new NullPointerException();
    }

    final Map<Long, Map<String, List<ProgramRecord>>> result = new HashMap<>();

    try {
      try( BufferedReader br = new BufferedReader
        ( new InputStreamReader( hdfs.open( new Path( schedulingFile ) ) ) ) ) {
        String line;

        while( ( line = br.readLine() ) != null ) {
          processVisitingTime( line, result );
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
        fs.listStatus( path, new NonHiddenFileFilter() );

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
    result.put( source, t );
  }


  /**
   * MISSING_COMMENT
   *
   * @param line
   * @param preferences
   */

  private static void processUserPreferenceLineOld
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
        pref.setChannelId( current );
        i++;

      } else if( i == 2 ) {
        pref.setTimeSlot( current );
        i++;

      } else if( i == 3 ) {
        pref.setDayOfWeek( current );
        i++;

      } else if( i == 4 ) {
        pref.setGroupTypeList( current );
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
   * @param preferences
   */

  private static void processGenreSequencePreferenceLine
    ( String line, Map<String,Map<String,Double>> preferences ){
    if( line == null ) {
      throw new NullPointerException();
    }
    if( preferences == null ) {
      throw new NullPointerException();
    }

    final StringTokenizer tk = new StringTokenizer( line, "," );
    String source = null;
    String target = null;
    Double preference = null;

    int i = 0;
    while( tk.hasMoreTokens() ) {
      final String current = tk.nextToken().replace( "\"", "" );

      if( i == 0 ) {
        source = current;
        i++;

      } else if( i == 1 ) {
        target = current;
        i++;

      } else if( i == 2 ) {
        // skip seconds
        i++;
      } else if( i == 3 ) {
        try {
          preference = Double.parseDouble( current );
        } catch(  NumberFormatException e ){
          preference = 0.0;
        }
        i++;
      }
    }

    if( source != null && target != null && preference != null ) {
      Map<String, Double> p = preferences.get( source );
      if( p == null ) {
        p = new HashMap<>();
      }
      p.put( target, preference );
      preferences.put( source, p );
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param line
   * @param scheduling
   */

  private static void processSchedulingLine
  ( String line,
    Map<Long, Map<String, List<ProgramRecord>>> scheduling ) {

    if( line == null ) {
      throw new NullPointerException();
    }
    if( scheduling == null ) {
      throw new NullPointerException();
    }

    final StringTokenizer tk = new StringTokenizer( line, "," );
    final ProgramRecord record = new ProgramRecord();
    final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd HH:mm" );

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

    if( record.getEndTime().getTime() < record.getStartTime().getTime() ){
      final SimpleDateFormat df = new SimpleDateFormat( "dd-MM-yyyy HH:mm:ss" );
      throw new IllegalArgumentException
        ( String.format
          ("Program \"%s\" has an invalid duration: %s - %s",
          df.format( record.getProgramId() ),
          df.format( record.getStartTime() ),
          record.getEndTime() ));
    }

    final Long key = DateUtils.truncate( record.getStartTime(), DATE ).getTime();
    Map<String, List<ProgramRecord>> map = scheduling.get( key );
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
    scheduling.put( key, map );
  }

  /**
   * MISSING_COMMENT
   *
   * @param line
   * @param scheduling
   */

  private static void processVisitingTime
  ( String line,
    Map<Long, Map<String, List<ProgramRecord>>> scheduling ) {

    if( line == null ) {
      throw new NullPointerException();
    }
    if( scheduling == null ) {
      throw new NullPointerException();
    }

    final StringTokenizer tk = new StringTokenizer( line, "," );
    final ProgramRecord record = new ProgramRecord();

    int i = 0;
    while( tk.hasMoreTokens() ) {
      final String current = tk.nextToken().replace( "\"", "" );

      if( i == 0 ) {
        // venue_id
        record.setProgramId( current.trim() );
        i++;

      } else if( i == 1 ) {
        // venue_category
        record.setChannelId( current.trim() );
        i++;

      } else if( i == 2 ) {
        try {
          record.setDuration( Long.parseLong( current.trim() ) );
        } catch( NumberFormatException e ) {
          System.out.printf( "Unable to parse duration \"%s\" in line \"%s\".", current, line );
        }
        i++;

      }
    }


    final Long key = new Long( 0 );
    Map<String, List<ProgramRecord>> map = scheduling.get( key );
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
    scheduling.put( key, map );
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
          if( duration < 0 ){
            System.out.printf( "Negative duration!!!%n" );
          }
          value.setDuration( duration );
          break;
        case 2:
          // sequence
          final StringTokenizer ctk = new StringTokenizer( current, "," );
          while( ctk.hasMoreTokens() ) {
            final String icurr = ctk.nextToken();
            value.addChannel( icurr );
          }
          break;
        case 3:
          int missedSeconds;
          try {
            missedSeconds = parseInt( current );
          } catch( NumberFormatException e ) {
            missedSeconds = 0;
          }
          if( missedSeconds < 0 ){
            System.out.printf( "Negative missed seconds!!!%n" );
          }
          value.setMissedSeconds( missedSeconds );
          break;
        case 4:
          double groupPreference;
          try {
            groupPreference = Double.parseDouble( current );
          } catch( NumberFormatException e ) {
            groupPreference = 0;
          }
          value.setGroupPreference( groupPreference );
          break;
        case 5:
          double minMaxFairness;
          try {
            minMaxFairness = Double.parseDouble( current );
          } catch( NumberFormatException e ) {
            minMaxFairness = 0;
          }
          value.setMinMaxFairness( minMaxFairness );
          break;
        case 6:
          double jainFairness;
          try {
            jainFairness = Double.parseDouble( current );
          } catch( NumberFormatException e ) {
            jainFairness = 0;
          }
          value.setMinMaxFairness( jainFairness );
          break;
      }
      i++;
    }
    return value;
  }
}
