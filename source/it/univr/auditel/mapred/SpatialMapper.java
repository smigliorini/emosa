package it.univr.auditel.mapred;

import it.univr.auditel.entities.GContext;
import it.univr.auditel.entities.GroupView;
import it.univr.auditel.entities.ProgramRecord;
import it.univr.auditel.entities.UserPreference;
import it.univr.auditel.mosa.MosaUtils;
import it.univr.auditel.shadoop.core.ViewSequenceWritable;
import it.univr.auditel.shadoop.core.ViewSequenceValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

import static it.univr.auditel.TrsaAuditel.*;
import static it.univr.auditel.shadoop.core.FileReader.*;
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
  extends Mapper<LongWritable, Text, Text, ViewSequenceWritable> {

  // ===========================================================================

  private Map<GContext, Map<GContext, Double>> groupTypeEvolutionMap;
  private Map<String, List<UserPreference>> preferenceMap;
  private Map<Date, Map<String, List<ProgramRecord>>> schedulingMap;
  private GContext initialContext;
  private FileSystem hdfs;

  private Set<ViewSequenceValue> paretoFront;
  private Integer duration;
  private Integer durationOffset;
  private Long maxPerturbations;
  private Integer initialTemperature;
  //private Boolean dynamic;
  //private Double historicalPercentage;
  //private Integer deltaPerVisitor;

  private static double alpha = 0.88;
  private static double finalTemperature = 1;


  // ===========================================================================

  @Override
  protected void setup( Context context )
    throws IOException, InterruptedException {

    final Configuration configuration = context.getConfiguration();
    duration = parseInt( configuration.get( durationLabel ));
    durationOffset = parseInt( configuration.get( durationOffsetLabel ) );
    maxPerturbations = parseLong( configuration.get( maxPerturbationsLabel ) );
    initialTemperature = parseInt( configuration.get( initialTemperatureLabel ) );


    hdfs = FileSystem.get( configuration );
    final URI[] cachedFiles = context.getCacheFiles();

    if( cachedFiles != null && cachedFiles.length == 4 ) {
      final String groupTypeEvo = configuration.get( groupTypeEvoFileLabel );
      final String preference = configuration.get( userPreferenceFileLabel );
      final String scheduling = configuration.get( schedulingFileLabel );
      final String estPareto = configuration.get( paretoFileLabel );

      for( int i = 0; i < cachedFiles.length; i++ ) {
        final URI uri = cachedFiles[i];
        if( uri.getPath().endsWith( groupTypeEvo ) ) {
          groupTypeEvolutionMap = readGroupTypeEvolution( hdfs, uri );
        } else if( uri.getPath().endsWith( preference ) ) {
          preferenceMap = readUserPreferences( hdfs, uri );
        } else if( uri.getPath().endsWith( scheduling )){
          schedulingMap = readScheduling( hdfs, uri );
        } else if( uri.getPath().endsWith( estPareto ) ) {
          paretoFront = readParetoFrontFromHdfs( hdfs, uri );
        }
      }
    }

    initialContext = new GContext();
    final String a = configuration.get( ageClassesLabel );
    final StringTokenizer tk = new StringTokenizer( a, "," );
    while( tk.hasMoreTokens() ) {
      initialContext.addAgeClass( tk.nextToken() );
    }

    initialContext.setTimeSlot( configuration.get( timeSlotLabel ) );
  }



  @Override
  protected void map( LongWritable key, Text value, Context context )
    throws IOException, InterruptedException {

    final Random generator = new Random( 3110L );

    final ViewSequenceWritable sequence = new ViewSequenceWritable();
    sequence.fromText( value );

    if( sequence.checkSequence( initialContext, duration, durationOffset ) &&
        sequence.getSequence() != null &&
        !sequence.isEmpty() ) {

      final GroupView start = sequence.getSequence().get( 0 );
      final GroupView end = sequence.getSequence().get( sequence.getSequence().size() - 1 );
      // duration in minutes
      //final int dur =
      //  (int) (( end.getIntervalEnd().getTime() -
      //            start.getIntervalStart().getTime() ) / (1000*60));


      Set<ViewSequenceWritable> paretoSet = new HashSet<>();
      paretoSet.add( sequence );

      // performSa() updates the paretoSet
      paretoSet = MosaUtils.performSa
        ( sequence,
          schedulingMap,
          preferenceMap,
          duration - durationOffset,//minDuration,
          duration + durationOffset,//maxDuration,
          paretoSet,
          paretoFront,
          generator,
          initialTemperature,
          finalTemperature,
          alpha,
          maxPerturbations );


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

      final List<ViewSequenceWritable> vlis = new ArrayList<>( paretoSet );

      // Only one choice so the result can be used to update ETO
      final Integer index =
        (int) round( ( vlis.size() - 1 ) * generator.nextDouble() );
      final ViewSequenceWritable selected = vlis.get( index );
      // -----------------------------------------------------------------------

      //for( TripWritable selected : paretoSet ) {
      context.write( new Text( selected.getKey() ), selected );

    }
  }

}
