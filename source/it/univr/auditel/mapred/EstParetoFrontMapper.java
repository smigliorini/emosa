package it.univr.auditel.mapred;

import it.univr.auditel.entities.GContext;
import it.univr.auditel.entities.ProgramRecord;
import it.univr.auditel.entities.UserPreference;
//new
import it.univr.auditel.entities.ChannelTransition;
//endnew
import it.univr.auditel.shadoop.core.ViewSequenceValue;
import it.univr.auditel.shadoop.core.ViewSequenceWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import static it.univr.auditel.TrsaAuditel.*;
import static it.univr.auditel.shadoop.core.FileReader.*;
import static java.lang.Integer.parseInt;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */

public class EstParetoFrontMapper
  extends Mapper<LongWritable, Text, Text, ViewSequenceValue> {

  // ===========================================================================

  private Map<GContext, Map<GContext, Double>> groupTypeEvolutionMap;
  private Map<String, List<UserPreference>> preferenceMap;
  private Map<Date, Map<String, List<ProgramRecord>>> schedulingMap;
  //new
  private Map<String, List<ChannelTransition>> transitionMap;
  //endnew
  private GContext initialContext;
  private Integer maxDuration;
  private FileSystem hdfs;


  private Integer durationOffset;

  // ===========================================================================

  @Override
  protected void setup( Context context )
    throws IOException, InterruptedException {

    final Configuration configuration = context.getConfiguration();
    durationOffset = parseInt( configuration.get( durationOffsetLabel ) );

    hdfs = FileSystem.get( configuration );
    final URI[] cachedFiles = context.getCacheFiles();

    if( cachedFiles != null && cachedFiles.length == 4 ) {  // era 3
      final String groupTypeEvo = configuration.get( groupTypeEvoFileLabel );
      final String preference = configuration.get( userPreferenceFileLabel );
      final String scheduling = configuration.get( schedulingFileLabel );
      //new
      final String transition = configuration.get( channelTransitionFileLabel );
      //endnew


      for( int i = 0; i < cachedFiles.length; i++ ) {
        final URI uri = cachedFiles[i];
        if( uri.getPath().endsWith( groupTypeEvo ) ) {
          groupTypeEvolutionMap = readGroupTypeEvolution( hdfs, uri );
        } else if( uri.getPath().endsWith( preference ) ) {
          preferenceMap = readUserPreferences( hdfs, uri );
        } else if( uri.getPath().endsWith( scheduling ) ) {
          schedulingMap = readScheduling( hdfs, uri );
        }
        //new
        else if( uri.getPath().endsWith( transition ) ) {
          transitionMap = readChannelTransition( hdfs, uri );
        }
        //endnew
      }
    }

    initialContext = new GContext();
    final String a = configuration.get( ageClassesLabel );
    final StringTokenizer tk = new StringTokenizer( a, "," );
    while( tk.hasMoreTokens() ) {
      initialContext.addAgeClass( tk.nextToken() );
    }
    initialContext.setTimeSlot( configuration.get( timeSlotLabel ) );

    maxDuration = Integer.parseInt( configuration.get( durationLabel ) );
  }


  @Override
  protected void map( LongWritable key, Text value, Context context )
    throws IOException, InterruptedException {

    final ViewSequenceWritable sequence = new ViewSequenceWritable();
    sequence.fromText( value );

    if( sequence.checkSequence( initialContext, maxDuration, durationOffset ) ) {
      final ViewSequenceValue view =
        new ViewSequenceValue( sequence, preferenceMap, schedulingMap );
      context.write( new Text( "1" ), view );
    }
  }
}
