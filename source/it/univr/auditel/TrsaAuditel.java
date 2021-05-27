package it.univr.auditel;

import it.univr.auditel.mapred.EstParetoFrontMapper;
import it.univr.auditel.mapred.EstParetoFrontReducer;
import it.univr.auditel.mapred.SpatialMapper;
import it.univr.auditel.mapred.SpatialReducer;
import it.univr.auditel.shadoop.core.ViewSequenceValue;
import it.univr.auditel.shadoop.core.ViewSequenceWritable;
import it.univr.utils.LogUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Date;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class TrsaAuditel {

  // logger
  private static final Log logger = LogFactory.getLog( TrsaAuditel.class );

  private static final Integer defaultDurationOffset = 0; // in minutes
  private static final Integer defaultDuration = 60; // in minutes
  private static long defaultMaxPerturbations = 100L;
  private static int defaultInitialTemperature = 8;

  public static final String ageClassesLabel = "ageClasses";
  public static final String timeSlotLabel = "timeSlot";
  //new
  public static final String transitionValueLabel = "transitionValue";
  //newend
  public static final String durationLabel = "maxDuration";
  public static final String durationOffsetLabel = "durationOffset";
  public static final String maxPerturbationsLabel = "maxPerturbations";
  public static final String initialTemperatureLabel = "initialTemperature";
  //public static final String dynamicLabel = "dynamic";
  //public static final String historicalPercentageLabel = "historicalPercentage";
  //public static final String deltaPerVisitorLabel = "deltaPerVisitor";

  public static final String groupTypeEvolutionFile = "group_type_evolution.csv";
  public static final String sequenceFile = "sequences.csv";
  public static final String preferenceFile = "user_channel_timeslot_wdwe_seconds_preference.csv";
  public static final String schedulingFile = "epg_program_scheduling.csv";
  //new
  public static final String channelTransitionFile = "channel_transition.csv";
  //endnew

  public static final String paretoFileSuffix = "epf";

  public static final String paretoFileLabel = "est_pareto_file";
  public static final String groupTypeEvoFileLabel = "group_type_evolution";
  public static final String sequenceFileLabel = "sequence";
  public static final String userPreferenceFileLabel = "user_preference";
  public static final String schedulingFileLabel = "scheduling";
  //new
  public static final String channelTransitionFileLabel = "channel_transition";
  //endnew

  // TODO: FAST FIX, assume maximum POI id = 30
  // substitute with list of POI ids
  // public static final int maxPoiId = 30;

  /**
   * MISSING_COMMENT
   *
   * @param args
   * @throws java.io.IOException
   */
  public static void main( String[] args )
    throws IOException,
    ClassNotFoundException,
    InterruptedException {

    final TrsaArguments ts = parseArguments( args );

    final Path inPath = new Path
      ( format( "%s/%s", ts.mainDirectory, ts.inputDirectory ) );
    final long timestamp = new Date().getTime();
    final Path epfOutPath = new Path
      ( format( "%s/%s/%s_%s",
                ts.mainDirectory,
                ts.outputDirectory,
                timestamp,
                paretoFileSuffix ) );

    final Path groupTypeEvolutionCachePath = new Path
      ( format( "%s/%s", ts.mainDirectory, groupTypeEvolutionFile ) );
    final Path preferenceCachePath = new Path
      ( format( "%s/%s", ts.mainDirectory, preferenceFile ) );
    final Path schedulingCachePath = new Path
      ( format( "%s/%s", ts.mainDirectory, schedulingFile ) );
    //new
    final Path transitionCachePath = new Path
            ( format( "%s/%s", ts.mainDirectory, channelTransitionFile ) );
    //newend

    LogUtils.suppressDebugLogs( true );

    // running the job
    logger.info( "running job...." );

    // -------------------------------------------------------------------------
    final Job job = preliminaryJob
      ( inPath,
        epfOutPath,
        preferenceCachePath,
        groupTypeEvolutionCachePath,
        schedulingCachePath,
        transitionCachePath,  //new
        ts);
    final int result = job.waitForCompletion( true ) ? 0 : 1;
    if( result != 0 ) {
      System.exit( result );
    }
    logger.info( "EstParetoFront completed." );//*/

    // -------------------------------------------------------------------------
    final Path trsaOutPath = new Path
      ( format
          ( "%s/%s/%s_trsa",
            ts.mainDirectory,
            ts.outputDirectory,
            timestamp ) );

    final Job mainJob = mainJob
      ( inPath,
        trsaOutPath,
        preferenceCachePath,
        groupTypeEvolutionCachePath,
        schedulingCachePath,
        transitionCachePath, //new
        epfOutPath,
        ts);

    final int mainResult = mainJob.waitForCompletion( true ) ? 0 : 1;
    if( mainResult != 0 ) {
      System.exit( mainResult );
    }//*/
    logger.info( "Trsa completed." );
  }


  /**
   * The method parses the program arguments and accordingly initializes an
   * instance of {@code TrsaArguments} with them.
   *
   * @param args
   * @return
   */

  private static TrsaArguments parseArguments( String[] args ) {
    if( args.length < 6 ) {  //new: erano 5
      printUsage();
      System.exit( 1 );
    }

    final TrsaArguments ts = new TrsaArguments();
    ts.mainDirectory = args[0];
    ts.inputDirectory = args[1];
    ts.outputDirectory = args[2];
    ts.ageClasses = args[3].substring( format( "%s=", ageClassesLabel ).length() );
    ts.timeSlot = args[4].substring( format( "%s=", timeSlotLabel ).length() );
    //new
    ts.transitionValue = args[5].substring( format( "%s=", transitionValueLabel).length() );
    //newend

    if( args.length > 6 ) { //new: era 5
      final boolean[] initialized = {false, false, false, false};

      for( String arg : args ) {
        // --- duration --------------------------------------------------------
        if( arg.startsWith( durationLabel ) ) {
          if( initialized[0] ) {
            System.out.printf
              ( "Parameter \"%s\" initialized twice.%n",
                durationLabel );
            printUsage();
            System.exit( 1 );
          } else {
            try {
              ts.duration = parseInt
                ( arg.substring( format( "%s=", durationLabel ).length() ) );
              if( ts.duration < 0 ) {
                throw new NumberFormatException();
              }
            } catch( NumberFormatException e ) {
              System.out.printf
                ( "Value \"%s\" for parameter \"%s\" is not valid.%n",
                  arg, durationLabel );
              System.exit( 1 );
            }
          }
          // --- maxDuration -----------------------------------------------------
        } else if( arg.startsWith( durationOffsetLabel ) ) {
          if( initialized[1] ) {
            System.out.printf
              ( "Parameter \"%s\" initialized twice.%n",
                durationOffsetLabel );
            printUsage();
            System.exit( 1 );

          } else {
            try {
              ts.durationOffset = parseInt
                ( arg.substring( format( "%s=", durationOffsetLabel ).length() ) );
              if( ts.durationOffset < 0 ) {
                throw new NumberFormatException();
              }
            } catch( NumberFormatException e ) {
              System.out.printf
                ( "Value \"%s\" for parameter \"%s\" is not valid.%n",
                  arg, durationOffsetLabel );
              System.exit( 1 );
            }
          }
          // --- maxPerturbations ----------------------------------------------
        } else if( arg.startsWith( maxPerturbationsLabel ) ) {
          if( initialized[2] ) {
            System.out.printf
              ( "Parameter \"%s\" initialized twice.%n",
                maxPerturbationsLabel );
            printUsage();
            System.exit( 1 );

          } else {
            try {
              ts.maxPerturbations = Long.parseLong
                ( arg.substring( format( "%s=", maxPerturbationsLabel ).length() ) );
              if( ts.maxPerturbations < 0 ) {
                throw new NumberFormatException();
              }
            } catch( NumberFormatException e ) {
              System.out.printf
                ( "Value \"%s\" for parameter \"%s\" is not valid.%n",
                  arg, maxPerturbationsLabel );
              System.exit( 1 );
            }
          }
          // --- initialTemperature --------------------------------------------
        } else if( arg.startsWith( initialTemperatureLabel ) ) {
          if( initialized[3] ) {
            System.out.printf
              ( "Parameter \"%s\" initialized twice.%n",
                initialTemperatureLabel );
            printUsage();
            System.exit( 1 );

          } else {
            try {
              ts.initialTemperature = parseInt
                ( arg.substring( format( "%s=", initialTemperatureLabel ).length() ) );
              if( ts.initialTemperature < 0 ) {
                throw new NumberFormatException();
              }
            } catch( NumberFormatException e ) {
              System.out.printf
                ( "Value \"%s\" for parameter \"%s\" is not valid.%n",
                  arg, initialTemperatureLabel );
              System.exit( 1 );
            }
          }
        }
      }
    }
    return ts;
  }


  /**
   * The method prints the usage instructions.
   */

  private static void printUsage() {
    System.out.printf
      ( "Usage: hadoop jar it.univr.auditel.TrsaAuditel "
        + "<mainDir> <inputDir> <outputDir> "
        + "[parameters]%n%n" );
    System.out.printf
      ( "<mainDir>: main directory which must contain the "
        + "input and output directories.%n" );
    System.out.printf
      ( "<inputDir>: input directory which must contain the files "
        + "\"%s\", \"%s\", \"%s\", \"%s\".%n",
        groupTypeEvolutionFile, sequenceFile, preferenceFile, channelTransitionFile ); //new
    System.out.printf
      ( "<outputDir>: output directory in which the output will be saved.%n" );
    System.out.printf
      ( "%s=<age_1,...,age_n> initial group type (age classes), without spaces%n",
        ageClassesLabel );
    System.out.printf
      ( "%s=<timeSlot> initial time slot%n", timeSlotLabel );
    //new
    System.out.printf
      ( "%s=<transitionValue> indicates the transition value between two channels%n", transitionValueLabel );
    //newend
    System.out.printf
      ( "Optional parameters:%n" );
    System.out.printf
      ( "%s=<int> duration in minutes (default = %d)%n",
        durationLabel, defaultDuration );
    System.out.printf
      ( "%s=<int> offset wrt the prescribed duration in minutes (default = %d)%n",
        durationOffsetLabel, defaultDurationOffset );
    System.out.printf
      ( "%s=<long> maximum perturbations for each temperature (default = %s)%n",
        maxPerturbationsLabel, defaultMaxPerturbations );
    System.out.printf
      ( "%s=<int> initial temperature (default = %d)%n",
        initialTemperatureLabel, defaultInitialTemperature );
  }


  /**
   * Inner class representing the arguments of the Trsa program.
   */

  static class TrsaArguments {
    private String mainDirectory;
    private String inputDirectory;
    private String outputDirectory;
    private Integer duration;
    private Integer durationOffset;
    private Long maxPerturbations;
    private Integer initialTemperature;
    private String ageClasses;
    private String timeSlot;
    //new
    private String transitionValue;
    //newend

    //private Boolean dynamic;
    //private Double historicalPercentage;
    //private Integer deltaPerVisitor;

    private TrsaArguments() {
      mainDirectory = null;
      inputDirectory = null;
      outputDirectory = null;
      duration = defaultDuration;
      durationOffset = defaultDurationOffset;
      maxPerturbations = defaultMaxPerturbations;
      initialTemperature = defaultInitialTemperature;
      ageClasses = null;
      timeSlot = null;
      //new
      transitionValue = null;
      //newend
    }
  }

  // ===========================================================================

  /**
   * The method initializes the preliminary Job which builds the initial
   * estimated Pareto-front.
   *
   * @param inPath
   * @param outPath
   * @param preferencePath
   * @param groupTypeEvolutionPath
   * @param transitionCachePath
   * @param arguments
   * @return
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */

  private static Job preliminaryJob
  ( Path inPath,
    Path outPath,
    Path preferencePath,
    Path groupTypeEvolutionPath,
    Path schedulingCachePath,
    Path transitionCachePath, //new
    TrsaArguments arguments )
    throws IOException,
    ClassNotFoundException,
    InterruptedException {

    if( inPath == null ) {
      throw new NullPointerException();
    }
    if( outPath == null ) {
      throw new NullPointerException();
    }
    if( preferencePath == null ) {
      throw new NullPointerException();
    }
    if( groupTypeEvolutionPath == null ) {
      throw new NullPointerException();
    }
    if( schedulingCachePath == null ) {
      throw new NullPointerException();
    }
    //new
    if( transitionCachePath == null ) {
      throw new NullPointerException();
    }
    //newend
    if( arguments == null ) {
      throw new NullPointerException();
    }


    final Configuration configuration = new Configuration();
    configuration.set( "shape", "wkt" );
    configuration.set( durationLabel, arguments.duration.toString() );
    configuration.set( durationOffsetLabel, arguments.durationOffset.toString() );
    configuration.set( groupTypeEvoFileLabel, groupTypeEvolutionFile );
    configuration.set( sequenceFileLabel, sequenceFile );
    configuration.set( userPreferenceFileLabel, preferenceFile );
    configuration.set( schedulingFileLabel, schedulingFile );
    //new
    configuration.set( channelTransitionFileLabel, channelTransitionFile );
    //newend
    configuration.set( maxPerturbationsLabel, arguments.maxPerturbations.toString() );
    configuration.set( initialTemperatureLabel, arguments.initialTemperature.toString() );
    configuration.set( ageClassesLabel, arguments.ageClasses );
    configuration.set( timeSlotLabel, arguments.timeSlot );
    //new
    configuration.set( transitionValueLabel, arguments.transitionValue );
    //newend

    final Job job = Job.getInstance( configuration );
    job.setJarByClass( TrsaAuditel.class );
    job.setJobName( "EstParetoFront" );
    logger.info( "EstParetoFront started: job created..." );

    // -------------------------------------------------------------------------

    final FileSystem cfs = FileSystem.get( configuration );
    job.addCacheFile( cfs.resolvePath( preferencePath ).toUri() );
    job.addCacheFile( cfs.resolvePath( groupTypeEvolutionPath ).toUri() );
    job.addCacheFile( cfs.resolvePath( schedulingCachePath ).toUri() );
    //new
    job.addCacheFile( cfs.resolvePath( transitionCachePath ).toUri() );
    //newend

    // -------------------------------------------------------------------------

    FileInputFormat.addInputPath( job, inPath );
    FileOutputFormat.setOutputPath( job, outPath );

    job.setMapperClass( EstParetoFrontMapper.class );
    job.setReducerClass( EstParetoFrontReducer.class );

    // output produced by the map!
    job.setOutputKeyClass( Text.class );
    job.setOutputValueClass( ViewSequenceValue.class );

    return job;
  }


  /**
   * The method initializes the Job that performs the TRSA algorithm.
   *
   * @param inPath
   * @param outPath
   * @param preferenceCachePath
   * @param groupTypeEvolutionCachePath
   * @param schedulingCachePath
   * @param transitionCachePath
   * @param epfOutPath
   * @param arguments
   * @return
   * @throws IOException
   */

  private static Job mainJob
  ( Path inPath,
    Path outPath,
    Path preferenceCachePath,
    Path groupTypeEvolutionCachePath,
    Path schedulingCachePath,
    Path transitionCachePath,  //new
    Path epfOutPath,
    TrsaArguments arguments )
    throws IOException {

    if( inPath == null ) {
      throw new NullPointerException();
    }
    if( outPath == null ) {
      throw new NullPointerException();
    }
    if( preferenceCachePath == null ) {
      throw new NullPointerException();
    }
    if( groupTypeEvolutionCachePath == null ) {
      throw new NullPointerException();
    }
    if( schedulingCachePath == null ) {
      throw new NullPointerException();
    }
    //new
    if( transitionCachePath == null ) {
      throw new NullPointerException();
    }
    //newend
    if( epfOutPath == null ) {
      throw new NullPointerException();
    }

    if( arguments == null ) {
      throw new NullPointerException();
    }


    final Configuration configuration = new Configuration();
    configuration.set( "shape", "wkt" );
    configuration.set( paretoFileLabel, paretoFileSuffix );
    configuration.set( durationLabel, arguments.duration.toString() );
    configuration.set( durationOffsetLabel, arguments.durationOffset.toString() );
    configuration.set( groupTypeEvoFileLabel, groupTypeEvolutionFile );
    configuration.set( schedulingFileLabel, schedulingFile );
    configuration.set( userPreferenceFileLabel, preferenceFile );
    //new
    configuration.set( channelTransitionFileLabel, channelTransitionFile );
    //newend
    configuration.set( paretoFileLabel, paretoFileSuffix );
    configuration.set( maxPerturbationsLabel, arguments.maxPerturbations.toString() );
    configuration.set( initialTemperatureLabel, arguments.initialTemperature.toString() );
    configuration.set( ageClassesLabel, arguments.ageClasses );
    configuration.set( timeSlotLabel, arguments.timeSlot );
    //new
    configuration.set( transitionValueLabel, arguments.transitionValue );
    //newend

    final Job job = Job.getInstance( configuration );
    job.setJarByClass( TrsaAuditel.class );
    job.setJobName( "Trsa" );
    logger.info( "Trsa started: job created..." );

    // -------------------------------------------------------------------------

    final FileSystem cfs = FileSystem.get( configuration );
    job.addCacheFile( cfs.resolvePath( preferenceCachePath ).toUri() );
    job.addCacheFile( cfs.resolvePath( groupTypeEvolutionCachePath ).toUri() );
    job.addCacheFile( cfs.resolvePath( schedulingCachePath ).toUri() );
    //new
    job.addCacheFile( cfs.resolvePath( transitionCachePath ).toUri() );
    //newend
    job.addCacheFile( cfs.resolvePath( epfOutPath ).toUri() );

    // -------------------------------------------------------------------------

    FileInputFormat.addInputPath( job, inPath );
    FileOutputFormat.setOutputPath( job, outPath );

    job.setMapperClass( SpatialMapper.class );
    job.setReducerClass( SpatialReducer.class );

    // output produced by the map!
    job.setOutputKeyClass( Text.class );
    job.setOutputValueClass( ViewSequenceWritable.class );
    return job;
  }

  // ===========================================================================


  /**
   * MISSING_COMMENT
   *
   * @param configuration
   */

  /*public static void initializeEto( Configuration configuration ) {

    if( configuration == null ) {
      throw new NullPointerException();
    }

    for( int poi = 1; poi <= maxPoiId; poi++ ) {
      for( int i = 1; i <= 24; i++ ) {
        //configuration.set
        //  ( format
        //      ( "%s_%s", configuration.get( requiredChannelLabel ), i ),
        //    "0" );
        configuration.set( format( "%s_%s", poi, i ), "0" );
      }
    }
  }//*/
}


