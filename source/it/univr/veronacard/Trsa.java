package it.univr.veronacard;

import it.univr.veronacard.mapred.EstParetoFrontMapper;
import it.univr.veronacard.mapred.EstParetoFrontReducer;
import it.univr.veronacard.mapred.SpatialMapper;
import it.univr.veronacard.mapred.SpatialReducer;
import it.univr.veronacard.shadoop.core.TripValue;
import it.univr.veronacard.shadoop.core.TripWritable;
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

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class Trsa {

  // logger
  private static final Log logger = LogFactory.getLog( Trsa.class );

  // case di giulietta 3, arena = 1
  //private static final String requiredPoi = "3";
  private static final Integer defaultMaxDuration = 360; // in minutes
  private static final Integer defaultDurationOffset = 0; // in minutes
  private static final Integer defaultMaxHour = 18;
  private static long defaultMaxPerturbations = 100L;
  private static int defaultInitialTemperature = 8;

  public static final String requiredPoiLabel = "requiredPoi";
  public static final String maxDurationLabel = "maxDuration";
  public static final String durationOffsetLabel = "durationOffset";
  public static final String maxHourLabel = "maxHour";
  public static final String maxPerturbationsLabel = "maxPerturbations";
  public static final String initialTemperatureLabel = "initialTemperature";
  public static final String dynamicLabel = "dynamic";
  public static final String historicalPercentageLabel = "historicalPercentage";
  public static final String deltaPerVisitorLabel = "deltaPerVisitor";

  public static final String stepsFile = "g_paths.csv";
  //final String groupTypeEvolutionFile = "g_paths_with_scenic_routes.csv";
  public static final String stayTimeFile = "stay_time_by_visits_stats.csv";
  public static final String numVisitsFile = "num_visits_stats.csv";
  public static final String paretoFileSuffix = "epf";

  public static final String stepsFileLabel = "steps_file";
  public static final String stayTimeFileLabel = "stay_time_file";
  public static final String numVisitsFileLabel = "num_visits_file";
  public static final String paretoFileLabel = "est_pareto_file";

  // TODO: FAST FIX, assume maximum POI id = 30
  // substitute with list of POI ids
  public static final int maxPoiId = 30;

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

    final Path stepCachePath = new Path
      ( format( "%s/%s", ts.mainDirectory, stepsFile ) );
    final Path stayTimeCachePath = new Path
      ( format( "%s/%s", ts.mainDirectory, stayTimeFile ) );
    final Path numVisitsCachePath = new Path
      ( format( "%s/%s", ts.mainDirectory, numVisitsFile ) );

    LogUtils.suppressDebugLogs( true );

    // running the job
    logger.info( "running job...." );

    // -------------------------------------------------------------------------
    final Job job = preliminaryJob
      ( inPath,
        epfOutPath,
        stepCachePath,
        numVisitsCachePath,
        stayTimeCachePath,
        ts );
    final int result = job.waitForCompletion( true ) ? 0 : 1;
    if( result != 0 ) {
      System.exit( result );
    }
    logger.info( "EstParetoFront completed." );

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
        numVisitsCachePath,
        stepCachePath,
        stayTimeCachePath,
        epfOutPath,
        ts );

    final int result2 = mainJob.waitForCompletion( true ) ? 0 : 1;
    if( result2 != 0 ) {
      System.exit( result2 );
    }
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
    if( args.length < 4 ) {
      printUsage();
      System.exit( 1 );
    }

    final TrsaArguments ts = new TrsaArguments();
    ts.mainDirectory = args[0];
    ts.inputDirectory = args[1];
    ts.outputDirectory = args[2];
    ts.requiredPoi = args[3];

    if( args.length > 4 ) {
      final boolean[] initialized = {false, false, false, false, false, false, false};

      for( String arg : args ) {
        // --- maxDuration -----------------------------------------------------
        if( arg.startsWith( maxDurationLabel ) ) {
          if( initialized[0] ) {
            System.out.printf
              ( "Parameter \"%s\" initialized twice.%n",
                maxDurationLabel );
            printUsage();
            System.exit( 1 );

          } else {
            try {
              ts.maxDuration = parseInt
                ( arg.substring( format( "%s=", maxDurationLabel ).length() ) );
              if( ts.maxDuration < 0 ) {
                throw new NumberFormatException();
              }
            } catch( NumberFormatException e ) {
              System.out.printf
                ( "Value \"%s\" for parameter \"%s\" is not valid.%n",
                  arg, maxDurationLabel );
              System.exit( 1 );
            }
          }

          // --- maxHour -------------------------------------------------------
        } else if( arg.startsWith( maxHourLabel ) ) {
          if( initialized[1] ) {
            System.out.printf
              ( "Parameter \"%s\" initialized twice.%n",
                maxHourLabel );
            printUsage();
            System.exit( 1 );

          } else {
            try {
              ts.maxHour = parseInt
                ( arg.substring( format( "%s=", maxHourLabel ).length() ) );
              if( ts.maxHour < 0 || ts.maxHour > 23 ) {
                throw new NumberFormatException();
              }
            } catch( NumberFormatException e ) {
              System.out.printf
                ( "Value \"%s\" for parameter \"%s\" is not valid.%n",
                  arg, maxHourLabel );
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
          // --- dynamic -------------------------------------------------------
        } else if( arg.startsWith( dynamicLabel ) ) {
          if( initialized[4] ) {
            System.out.printf
              ( "Parameter \"%s\" initialized twice.%n",
                dynamicLabel );
            printUsage();
            System.exit( 1 );
          } else {
            final String value =
              arg.substring( format( "%s=", dynamicLabel ).length() ).toLowerCase();
            if( value.equals( "true" ) || value.equals( "t" ) ) {
              ts.dynamic = true;
            } else if( value.equals( "false" ) || value.equals( "f" ) ) {
              ts.dynamic = false;
            } else {
              System.out.printf
                ( "Value \"%s\" for parameter \"%s\" is not valid.%n",
                  arg, dynamicLabel );
              System.exit( 1 );
            }
          }
          // historical percentage ---------------------------------------------
        } else if( arg.startsWith( historicalPercentageLabel ) ) {
          if( initialized[5] ) {
            System.out.printf
              ( "Parameter \"%s\" initialized twice.%n",
                historicalPercentageLabel );
            printUsage();
            System.exit( 1 );
          } else {
            final String value =
              arg.substring( format( "%s=", historicalPercentageLabel ).length() );
            try {
              ts.historicalPercentage = parseDouble
                ( value );
              if( ts.historicalPercentage < 0 || ts.historicalPercentage > 1 ) {
                throw new NumberFormatException();
              }
            } catch( NumberFormatException e ) {
              System.out.printf
                ( "Value \"%s\" for parameter \"%s\" is not valid.%n",
                  value, historicalPercentageLabel );
              System.exit( 1 );
            }
          }
        } else if( arg.startsWith( deltaPerVisitorLabel ) ) {
          if( initialized[6] ) {
            System.out.printf
              ( "Parameter \"%d\" initialized twice.%n",
                deltaPerVisitorLabel );
            System.exit( 1 );
          } else {
            final String value = arg.substring( format( "%s=", deltaPerVisitorLabel ).length() );
            try {
              ts.deltaPerVisitor = parseInt( value );
            } catch( NumberFormatException e ){
              System.out.printf
                ( "Value \"%s\" for parameter \"%s\" is not valid.%n",
                  value, deltaPerVisitorLabel );
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
      ( "Usage: hadoop jar it.univr.veronacard.Trsa "
        + "<mainDir> <inputDir> <outputDir> <requiredPoi> "
        + "[parameters]%n%n" );
    System.out.printf
      ( "<minDir>: main directory which must contain the files "
        + "\"%s\", \"%s\", \"%s\" and the input and output directories.%n",
        stepsFile, stayTimeFile, numVisitsFile );
    System.out.printf
      ( "<inputDir>: input directory which must contains the verona "
        + "card historical records.%n" );
    System.out.printf
      ( "<outputDir>: output directory in which the output will be saved.%n" );
    System.out.printf
      ( "<requiredPoi>: required POI "
        + "(e.g., 3 = \"Casa di Giulietta\", 1 = \"Arena\".%n%n" );
    System.out.printf
      ( "Optional parameters:%n" );
    System.out.printf
      ( "%s=<int> maximum trip duration in minutes (default = %d)%n",
        maxDurationLabel, defaultMaxDuration );
    System.out.printf
      ( "%s=<int> maximum hour for the trip termination (default = %d)%n",
        maxHourLabel, defaultMaxHour );
    System.out.printf
      ( "%s=<long> maximum perturbations for each temperature (default = %s)%n",
        maxPerturbationsLabel, defaultMaxPerturbations );
    System.out.printf
      ( "%s=<int> initial temperature (default = %d)%n",
        initialTemperatureLabel, defaultInitialTemperature );
    System.out.printf
      ( "%s=<bool> use dynamic information or not (default = false, "
        + "possible values = \"true\", \"t\", \"false\", \"f\")%n",
        dynamicLabel );
    System.out.printf
      ( "%s=<double> percentage of historical information to consider during"
        + "the computation of ATO, from 0.0 to 1.0%n",
        historicalPercentageLabel );
    System.out.printf
      ( "%s=<integer> additional visiting time for each visitor%n",
        deltaPerVisitorLabel );
  }


  /**
   * Inner class representing the arguments of the Trsa program.
   */

  static class TrsaArguments {
    private String mainDirectory;
    private String inputDirectory;
    private String outputDirectory;
    private String requiredPoi;
    private Integer maxDuration;
    private Integer durationOffset;
    private Integer maxHour;
    private Long maxPerturbations;
    private Integer initialTemperature;
    private Boolean dynamic;
    private Double historicalPercentage;
    private Integer deltaPerVisitor;

    private TrsaArguments() {
      mainDirectory = null;
      inputDirectory = null;
      outputDirectory = null;
      requiredPoi = null;
      maxDuration = defaultMaxDuration;
      durationOffset = defaultDurationOffset;
      maxHour = defaultMaxHour;
      maxPerturbations = defaultMaxPerturbations;
      initialTemperature = defaultInitialTemperature;
      dynamic = false;
      historicalPercentage = 1.0;
      deltaPerVisitor = 0;
    }
  }

  // ===========================================================================

  /**
   * The method initializes the preliminary Job which builds the initial
   * estimated Pareto-front.
   *
   * @param inPath
   * @param outPath
   * @param stepCachePath
   * @param numVisitsCachePath
   * @param stayTimeCachePath
   * @param arguments
   * @return
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */

  private static Job preliminaryJob
  ( Path inPath,
    Path outPath,
    Path stepCachePath,
    Path numVisitsCachePath,
    Path stayTimeCachePath,
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
    if( stepCachePath == null ) {
      throw new NullPointerException();
    }
    if( numVisitsCachePath == null ) {
      throw new NullPointerException();
    }
    if( stayTimeCachePath == null ) {
      throw new NullPointerException();
    }
    if( arguments == null ) {
      throw new NullPointerException();
    }


    final Configuration configuration = new Configuration();
    configuration.set( "shape", "wkt" );
    configuration.set( requiredPoiLabel, arguments.requiredPoi );
    configuration.set( maxDurationLabel, arguments.maxDuration.toString() );
    configuration.set( durationOffsetLabel, arguments.durationOffset.toString() );
    configuration.set( stepsFileLabel, stepsFile );
    configuration.set( stayTimeFileLabel, stayTimeFile );
    configuration.set( numVisitsFileLabel, numVisitsFile );
    configuration.set( maxHourLabel, arguments.maxHour.toString() );
    configuration.set( maxPerturbationsLabel, arguments.maxPerturbations.toString() );
    configuration.set( initialTemperatureLabel, arguments.initialTemperature.toString() );
    configuration.set( dynamicLabel, arguments.dynamic.toString() );
    configuration.set( historicalPercentageLabel, arguments.historicalPercentage.toString() );
    configuration.set( deltaPerVisitorLabel, arguments.deltaPerVisitor.toString() );
    initializeEto( configuration );

    final Job job = Job.getInstance( configuration );
    job.setJarByClass( Trsa.class );
    job.setJobName( "EstParetoFront" );
    logger.info( "EstParetoFront started: job created..." );

    // -------------------------------------------------------------------------

    final FileSystem cfs = FileSystem.get( configuration );
    job.addCacheFile( cfs.resolvePath( stepCachePath ).toUri() );
    job.addCacheFile( cfs.resolvePath( stayTimeCachePath ).toUri() );
    job.addCacheFile( cfs.resolvePath( numVisitsCachePath ).toUri() );

    // -------------------------------------------------------------------------

    FileInputFormat.addInputPath( job, inPath );
    FileOutputFormat.setOutputPath( job, outPath );

    job.setMapperClass( EstParetoFrontMapper.class );
    job.setReducerClass( EstParetoFrontReducer.class );

    // output produced by the map!
    job.setOutputKeyClass( Text.class );
    job.setOutputValueClass( TripValue.class );

    return job;
  }


  /**
   * The method initializes the Job that performs the TRSA algorithm.
   *
   * @param inPath
   * @param outPath
   * @param paretoFrontCache
   * @param stepCache
   * @param numVisitsCachePath
   * @param stayTimeCachePath
   * @param arguments
   * @return
   * @throws IOException
   */

  private static Job mainJob
  ( Path inPath,
    Path outPath,
    Path paretoFrontCache,
    Path stepCache,
    Path numVisitsCachePath,
    Path stayTimeCachePath,
    TrsaArguments arguments )
    throws IOException {

    if( inPath == null ) {
      throw new NullPointerException();
    }
    if( outPath == null ) {
      throw new NullPointerException();
    }
    if( paretoFrontCache == null ) {
      throw new NullPointerException();
    }
    if( stepCache == null ) {
      throw new NullPointerException();
    }
    if( numVisitsCachePath == null ) {
      throw new NullPointerException();
    }
    if( stayTimeCachePath == null ) {
      throw new NullPointerException();
    }
    if( arguments == null ) {
      throw new NullPointerException();
    }


    final Configuration configuration = new Configuration();
    configuration.set( "shape", "wkt" );
    configuration.set( requiredPoiLabel, arguments.requiredPoi );
    configuration.set( maxDurationLabel, arguments.maxDuration.toString() );
    configuration.set( durationOffsetLabel, arguments.durationOffset.toString() );
    configuration.set( stayTimeFileLabel, stayTimeFile );
    configuration.set( numVisitsFileLabel, numVisitsFile );
    configuration.set( stepsFileLabel, stepsFile );
    configuration.set( paretoFileLabel, paretoFileSuffix );
    configuration.set( maxHourLabel, arguments.maxHour.toString() );
    configuration.set( maxPerturbationsLabel, arguments.maxPerturbations.toString() );
    configuration.set( initialTemperatureLabel, arguments.initialTemperature.toString() );
    configuration.set( dynamicLabel, arguments.dynamic.toString() );
    configuration.set( historicalPercentageLabel, arguments.historicalPercentage.toString() );
    configuration.set( deltaPerVisitorLabel, arguments.deltaPerVisitor.toString() );
    initializeEto( configuration );

    final Job job = Job.getInstance( configuration );
    job.setJarByClass( Trsa.class );
    job.setJobName( "Trsa" );
    logger.info( "Trsa started: job created..." );

    // -------------------------------------------------------------------------

    final FileSystem cfs = FileSystem.get( configuration );
    job.addCacheFile( cfs.resolvePath( stayTimeCachePath ).toUri() );
    job.addCacheFile( cfs.resolvePath( numVisitsCachePath ).toUri() );
    job.addCacheFile( cfs.resolvePath( stepCache ).toUri() );
    job.addCacheFile( cfs.resolvePath( paretoFrontCache ).toUri() );

    // -------------------------------------------------------------------------

    FileInputFormat.addInputPath( job, inPath );
    FileOutputFormat.setOutputPath( job, outPath );

    job.setMapperClass( SpatialMapper.class );
    job.setReducerClass( SpatialReducer.class );

    // output produced by the map!
    job.setOutputKeyClass( Text.class );
    job.setOutputValueClass( TripWritable.class );
    return job;
  }

  // ===========================================================================


  /**
   * MISSING_COMMENT
   *
   * @param configuration
   */

  public static void initializeEto( Configuration configuration ) {

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
  }
}


