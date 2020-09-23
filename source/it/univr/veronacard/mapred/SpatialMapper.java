package it.univr.veronacard.mapred;

import it.univr.veronacard.mosa.MosaUtils;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static it.univr.veronacard.Trsa.*;
import static it.univr.veronacard.shadoop.core.FileReader.*;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.*;
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

public class SpatialMapper
  extends Mapper<LongWritable, Text, Text, TripWritable> {

  // ===========================================================================

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


  // ===========================================================================

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
    }

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
  protected void map( LongWritable key, Text value, Context context )
    throws IOException, InterruptedException {

    final Random generator = new Random( 3110L );

    final TripWritable feature = new TripWritable();
    feature.fromText( value );

    // casa di giulietta 45, arena 115
    // casa di giulietta 2015, arena 2016
    if( feature.checkFeature( requiredPoi, 45, 2015, false ) ) {
      feature.buildStepsFromCompleteMap( possibleSteps );
      feature.computeVisitingTime
        ( numVisits,
          stayTimes,
          eto,
          historicalPercentage,
          deltaPerVisitor );

      //feature.setStartHour( 8 );

      final int dur =
        Math.min( ( maxHour - feature.getStartHour()) * 60, feature.getDuration() );
        //( maxHour - feature.getStartHour() ) * 60;


      Set<TripWritable> paretoSet = new HashSet<>();
      paretoSet.add( feature );

      // performSa() updates the paretoSet
      paretoSet = MosaUtils.performSa
        ( feature,
          possibleSteps,
          numVisits,
          stayTimes,
          eto,
          feature.getTravelMode(),
          //feature.getDuration() - durationOffset,
          //duration - durationOffset,
          // fast fix! minDuration = 60
          // max( dur - durationOffset, 0 ),
          60,
          //feature.getDuration() + durationOffset,
          //duration + durationOffset,
          dur + durationOffset,
          paretoSet,
          paretoFront,
          generator,
          initialTemperature,
          finalTemperature,
          alpha,
          maxPerturbations,
          requiredPoi,
          historicalPercentage,
          deltaPerVisitor );


      // --- FIX FOR PAPER!!! --------------------------------------------------

      /*final List<TripWritable> tlist = new ArrayList<>( paretoSet.size() );
      for( TripWritable t : paretoSet ){
        if( t.getDuration() < dur + durationOffset ){
          tlist.add( t );
        }
      }

      if( tlist.isEmpty() ){
        return;
      } //*/

      final List<TripWritable> tlist = new ArrayList<>( paretoSet );

      // Only one choice so the result can be used to update ETO
      final Integer index =
        (int) round( ( tlist.size() - 1 ) * generator.nextDouble() );
      final TripWritable selected = tlist.get( index );
      // -----------------------------------------------------------------------

      //for( TripWritable selected : paretoSet ) {
      context.write( new Text( selected.getVcSerial() ), selected );

      if( dynamic ) {
        for( String site : selected.getSites()) {
          final int hour =
            selected.getSiteArrivingHour

              ( site,
                numVisits,
                stayTimes,
                eto,
                historicalPercentage,
                deltaPerVisitor );
          if( hour != -1 ) {
            Map<Integer, Integer> map = eto.get( site );
            if( map == null ) {
              map = new HashMap<>();
              map.put( hour, 1 );
            } else {
              map.put( hour, map.get( hour ) != null ? map.get( hour ) + 1 : 1 );
            }
            eto.put( site, map );
          }
        }

        final Configuration conf = context.getConfiguration();
        for( String k : eto.keySet() ) {
          for( Integer h : eto.get( k ).keySet() ) {
            final String l = format( "%s_%s", k, h );
            if( h >= 1 && h <= 24 ) {
              final Integer prevValue = conf.get( l ) != null ?
                parseInt( conf.get( l ) ) : 0;
              final Integer nextValue = prevValue + 1;
              conf.set( l, nextValue.toString() );
            }
          }
        }
      }
      //}
    }
  }

}
