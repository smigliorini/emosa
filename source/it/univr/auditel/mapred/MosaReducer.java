package it.univr.auditel.mapred;


import it.univr.auditel.entities.GContext;
import it.univr.auditel.entities.ProgramRecord;
import it.univr.auditel.entities.UserPreference;
import it.univr.auditel.shadoop.core.ViewSequenceValue;
import it.univr.auditel.shadoop.core.ViewSequenceWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static it.univr.auditel.TrsaAuditel.*;
import static it.univr.auditel.shadoop.core.FileReader.*;
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
public class MosaReducer
  //extends Reducer<Text, ViewSequenceWritable, Text, ViewSequenceValue> {
  extends Reducer<Text, Text, Text, ViewSequenceValue> {

  // === Properties ============================================================

  private Map<String, List<UserPreference>> preferenceMap;
  private Map<String, Map<String, Double>> genreSeqPreferenceMap;
  private Map<Long, Map<String, List<ProgramRecord>>> schedulingMap;
  private Map<GContext, Map<GContext, Double>> groupTypeEvolutionMap;

  private FileSystem hdfs;

  private Integer duration;
  private Integer durationOffset;

  private boolean auditel;
  private boolean dynamic;

  // === Methods ===============================================================

  @Override
  protected void setup( Context context )
    throws IOException, InterruptedException{

    final Configuration configuration = context.getConfiguration();
    hdfs = FileSystem.get( configuration );
    final URI[] cachedFiles = context.getCacheFiles();

    duration = parseInt( configuration.get( durationLabel ) );
    durationOffset = parseInt( configuration.get( durationOffsetLabel ) );
    auditel = Boolean.parseBoolean( configuration.get( auditelLabel ) );
    dynamic = Boolean.parseBoolean( configuration.get( dynamicLabel ) );

    if( cachedFiles != null && cachedFiles.length == 5 ){
      final String groupTypeEvo = configuration.get( groupTypeEvoFileLabel );
      final String preference = configuration.get( userPreferenceFileLabel );
      final String genreSequence = configuration.get( genreSequenceFileLabel );
      final String scheduling = configuration.get( schedulingFileLabel );
      final String estPareto = configuration.get( paretoFileLabel );

      for( int i = 0; i < cachedFiles.length; i++ ){
        final URI uri = cachedFiles[ i ];
        if( uri.getPath().endsWith( groupTypeEvo ) ){
          groupTypeEvolutionMap = readGroupTypeEvolution( hdfs, uri );
        } else if( uri.getPath().endsWith( preference ) ){
          preferenceMap = readUserPreferences( hdfs, uri );
        } else if( uri.getPath().endsWith( genreSequence ) ){
          genreSeqPreferenceMap = readGenreSequencePreferences( hdfs, uri );
        } else if( uri.getPath().endsWith( scheduling ) ){
          if( scheduling.startsWith( "poi_" )){
            schedulingMap = readVisitingTime( hdfs, uri );
          } else if( scheduling.startsWith( "epg_" )){
            schedulingMap = readScheduling( hdfs, uri );
          }
        }
        //else if( uri.getPath().endsWith( estPareto ) ) {
        //  paretoFront = readParetoFrontFromHdfs( hdfs, uri );
        //}
      }
    }
  }


  @Override
  /**
   * FIX FOR PAPER!!!!
   */
  protected void reduce
    ( Text key,
      //Iterable<ViewSequenceWritable> values,
      Iterable<Text> values,
      Context context )
    throws IOException, InterruptedException{

    // trips with the same key
    final List<ViewSequenceValue> seqValues = new ArrayList<>();

    for( Text s : values ){
      final ViewSequenceWritable w = new ViewSequenceWritable();
      w.fromText( s );
      final ViewSequenceValue value = new ViewSequenceValue
        ( w, preferenceMap, genreSeqPreferenceMap,
          groupTypeEvolutionMap, schedulingMap, auditel, dynamic );
      seqValues.add( value );
    }

    for( int i = 0; i < seqValues.size(); i++ ){
      boolean dominated = false;
      for( int j = i + 1; j < seqValues.size(); j++ ){
        if( seqValues.get( j ).
          dominate
            ( seqValues.get( i ),
              duration - durationOffset,
              duration + durationOffset,
              dynamic )){
          dominated = true;
        }
      }

      if( dominated == false ){
        context.write( new Text( "1" ), seqValues.get( i ) );
      }
    }
  }


}
