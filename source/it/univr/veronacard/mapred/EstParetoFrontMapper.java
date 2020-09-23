package it.univr.veronacard.mapred;

import it.univr.veronacard.shadoop.core.Step;
import it.univr.veronacard.shadoop.core.TripValue;
import it.univr.veronacard.shadoop.core.TripWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static it.univr.veronacard.Trsa.*;
import static it.univr.veronacard.shadoop.core.FileReader.readNumVisits;
import static it.univr.veronacard.shadoop.core.FileReader.readStayTimes;
import static it.univr.veronacard.shadoop.core.FileReader.readStepsFromCacheFile;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.*;
import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */

public class EstParetoFrontMapper
  extends Mapper<LongWritable, Text, Text, TripValue> {

  // ===========================================================================

  private Map<String, Map<String, List<Step>>> possibleSteps;
  private Map<String, Map<String, Integer>> numVisits;
  private Map<String, Map<Integer, Integer>> stayTimes;
  private Map<String, Map<Integer, Integer>> eto;
  private FileSystem hdfs;

  private String startPoi;
  private Integer duration;
  private Integer durationOffset;
  private Integer maxHour;
  private Double historicalPercentage;
  private Boolean dynamic;
  private Integer deltaPerVisitor;

  // ===========================================================================

  @Override
  protected void setup( Context context )
    throws IOException, InterruptedException {

    final Configuration configuration = context.getConfiguration();
    startPoi = configuration.get( requiredPoiLabel );
    duration = parseInt( configuration.get( maxDurationLabel ) );
    durationOffset = parseInt( configuration.get( durationOffsetLabel ) );
    maxHour = parseInt( configuration.get( maxHourLabel ) );
    deltaPerVisitor = parseInt( configuration.get( deltaPerVisitorLabel ) );
    dynamic = parseBoolean( configuration.get( dynamicLabel ) );
    if( dynamic == true ) {
      if( configuration.get( historicalPercentageLabel ) == null ){
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

    if( cachedFiles != null && cachedFiles.length == 3 ) {
      final String steps = configuration.get( stepsFileLabel );
      final String times = configuration.get( stayTimeFileLabel );
      final String visits = configuration.get( numVisitsFileLabel );

      for( int i = 0; i < cachedFiles.length; i++ ) {
        final URI uri = cachedFiles[i];
        if( uri.getPath().endsWith( steps ) ) {
          possibleSteps = readStepsFromCacheFile( hdfs, uri );
        } else if( uri.getPath().endsWith( times ) ) {
          stayTimes = readStayTimes( hdfs, uri );
        } else if( uri.getPath().endsWith( visits ) ) {
          numVisits = readNumVisits( hdfs, uri );
        }
      }
    }

    // --- build the eto map ---------------------------------------------------

    eto = new HashMap<>();
    final Map<Integer, Integer> value = new HashMap<>();
    for( int i = 1; i <= 24; i++ ) {
      final String s = configuration.get( format( "%s_%s", startPoi, i ) );
      value.put( i, parseInt( s ) );
    }
    eto.put( startPoi, value );
  }


  @Override
  protected void map( LongWritable key, Text value, Context context )
    throws IOException, InterruptedException {

    final TripWritable feature = new TripWritable();
    feature.fromText( value );

    if( feature.checkFeature( startPoi, 45, 2015, false ) ) {
      feature.buildStepsFromCompleteMap( possibleSteps );
      feature.computeVisitingTime( numVisits, stayTimes, eto, historicalPercentage, deltaPerVisitor );

      // compute the duration so that it can be performed until 11pm
      // duration is in minutes!!!
      final int dur =
      Math.min( ( maxHour - feature.getStartHour() ) * 60, duration );

      if( feature.checkFeatureDuration( dur, durationOffset ) ) {
        final TripValue tv = new TripValue( feature );
        final String compositeKey =
          format( "%s_%s", feature.getSource(), feature.getTravelMode() );
        context.write( new Text( compositeKey ), tv );
      }
    }
  }
}
