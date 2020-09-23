package it.univr.veronacard.mapred;

import it.univr.veronacard.shadoop.core.Step;
import it.univr.veronacard.shadoop.core.TripValue;
import it.univr.veronacard.shadoop.core.TripWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static it.univr.veronacard.Trsa.*;
import static it.univr.veronacard.shadoop.core.FileReader.*;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.Math.round;
import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class SpatialReducer
  extends Reducer<Text, TripWritable, Text, TripWritable> {

  // === Properties ============================================================

  private Map<String, Map<String, List<Step>>> possibleSteps;
  private Set<TripValue> paretoFront;
  private Map<String, Map<String, Integer>> numVisits;
  private Map<String, Map<Integer, Integer>> stayTimes;
  private Map<String, Map<Integer, Integer>> eto;
  private FileSystem hdfs;
  private String requiredPoi;
  private Integer duration;
  private Integer durationOffset;
  private Integer maxHour;
  private Long maxPerturbations;
  private Integer initialTemperature;
  private Boolean dynamic;
  private Double historicalPercentage;
  private Integer deltaPerVisitor;

  private static double alpha = 0.88;
  private static double finalTemperature = 1;

  // === Methods ===============================================================

  @Override
  protected void setup( Context context )
    throws IOException, InterruptedException {

    final Configuration configuration = context.getConfiguration();
    requiredPoi = configuration.get( requiredPoiLabel );
    duration = parseInt( configuration.get( maxDurationLabel ) );
    durationOffset = parseInt( configuration.get( durationOffsetLabel ) );
    maxHour = parseInt( configuration.get( maxHourLabel ) );
    maxPerturbations = parseLong( configuration.get( maxPerturbationsLabel ) );
    initialTemperature = parseInt( configuration.get( initialTemperatureLabel ) );
    deltaPerVisitor = parseInt( configuration.get( deltaPerVisitorLabel ) );
    dynamic = parseBoolean( configuration.get( dynamicLabel ) );
    if( dynamic == true ) {
      if( configuration.get( historicalPercentageLabel ) == null ) {
        historicalPercentage = 1.0;
      } else {
        historicalPercentage = parseDouble
          ( configuration.get( historicalPercentageLabel ) );
      }
    } else {
      historicalPercentage = 1.0;
    }

    hdfs = FileSystem.get( configuration );
    final URI[] cachedFiles = context.getCacheFiles();

    if( cachedFiles != null && cachedFiles.length == 4 ) {
      final String steps = configuration.get( stepsFileLabel );
      final String times = configuration.get( stayTimeFileLabel );
      final String estPareto = configuration.get( paretoFileLabel );
      final String visits = configuration.get( numVisitsFileLabel );

      for( int i = 0; i < cachedFiles.length; i++ ) {
        final URI uri = cachedFiles[i];

        if( uri.getPath().endsWith( steps ) ) {
          possibleSteps = readStepsFromCacheFile( hdfs, uri );
        } else if( uri.getPath().endsWith( times ) ) {
          stayTimes = readStayTimes( hdfs, uri );
        } else if( uri.getPath().endsWith( estPareto ) ) {
          paretoFront = readParetoFrontFromHdfs( hdfs, uri );
        } else if( uri.getPath().endsWith( visits ) ) {
          numVisits = readNumVisits( hdfs, uri );
        }
      }
    }//*/

    // --- build the eto map ---------------------------------------------------
    eto = new HashMap<>();
    final Map<Integer, Integer> value = new HashMap<>();

    for( int poi = 1; poi <= maxPoiId; poi++ ) {
      for( int i = 1; i <= 24; i++ ) {
        final String s = configuration.get( format( "%s_%s", poi, i ) );
        value.put( i, parseInt( s ) );
      }
      eto.put( format( "%s", poi ), value );
    }
  }


  @Override
  /**
   * FIX FOR PAPER!!!!
   */
  protected void reduce
    ( Text key,
      Iterable<TripWritable> values,
      Context context )
    throws IOException, InterruptedException {

    // trips with the same key
    final List<TripWritable> trips = new ArrayList<>();

    for( TripWritable s : values ) {
      trips.add( s );
    }

    final Random generator = new Random();
    final Integer index = (int) round( ( trips.size() - 1 ) * generator.nextDouble() );
    final TripWritable selected = trips.get( index );

    // fix
    selected.computeVisitingTime
      ( numVisits,
        stayTimes,
        eto,
        historicalPercentage,
        deltaPerVisitor );//*/

    context.write( new Text( selected.getVcSerial() ), selected );
  }

  /*protected void reduce
    ( Text key,
      Iterable<TripWritable> values,
      Context context )
    throws IOException, InterruptedException {

    // -------------------------------------------------------------------------

    // process the key to obtain the query parameters
    final String k = key.toString();
    String origin = null;
    String travelMode = null;
    VcProfileEnum profile = null;

    final StringTokenizer tk = new StringTokenizer( k, "_" );
    int h = 0;
    while( tk.hasMoreTokens() ) {
      final String c = tk.nextToken();
      if( h == 0 ) {
        origin = c;
      } else if( h == 1 ) {
        travelMode = c;
      } else if( h == 2 ) {
        profile = fromText( c );
      }
      h++;
    }

    // -------------------------------------------------------------------------

    // initialize the Pareto-set with the trips that satisfy the constraints
    // and are not dominated by other
    final Set<TripWritable> paretoSet = new HashSet<>();

    int numRows = 0;
    for( TripWritable s : values ) {
      // IMPORTANT: the iterator in the Hadoop reducer uses a single object
      // whose contents it changes each time it goes to the next value.
      // If the object is not copied before added to the paretoSet list, all
      // the objects in the paretoSet list becomes the same!!!!
      final TripWritable current = new TripWritable( s );

      current.buildStepsFromCompleteMap( possibleSteps );
      current.computeVisitingTime( numVisits, stayTimes, eto );
      updateParetoSet
        ( paretoSet,
          current,
          duration -  durationOffset,
          duration +  durationOffset );

      numRows++;
    }

    System.out.printf
      ( "[OUT] Built the initial Pareto-set. Size: %d (processed trips %d)%n",
        paretoSet.size(), numRows + 1 );

    // fast fix
    if( paretoSet.size() == 0 ) {
      return;
    }//*/


  // -------------------------------------------------------------------------

    /*final Random generator = new Random( 3110L );
    //final int maxIterations = ;

    for( int i = 0; i < maxIterations; i++ ) {
      final int solIndex = ( abs( generator.nextInt() ) % paretoSet.size() );
      TripWritable currentSol = new ArrayList<>( paretoSet ).get( solIndex );

      Set<TripValue> paretoFront = computeParetoFront( paretoSet );

      performSa
        ( currentSol,
          possibleSteps,
          stayTimes,
          travelMode,
          duration - durationOffset,
          duration + durationOffset,
          paretoSet,
          paretoFront,
          generator,
          initialTemperature,
          finalTemperature,
          alpha,
          maxPerturbations );

      System.out.printf( "[OUT] Performed iteration: %s of %s%n",
                         ( i + 1 ), maxIterations );
    }//*/

    /*for( TripWritable s : paretoSet ) {
      final String composedKey = format
        ( "%s_%s_%s",
          s.getSource(),
          s.getTravelMode(),
          getProfile( s.getProfile() ) );

      context.write( new Text( composedKey ), s );
    }
  }//*/
}
