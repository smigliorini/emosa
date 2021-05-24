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
public class SpatialReducer
  extends Reducer<Text, ViewSequenceWritable, Text, ViewSequenceValue> {

  // === Properties ============================================================

  private Map<String, List<UserPreference>> preferenceMap;
  private Map<Date, Map<String, List<ProgramRecord>>> schedulingMap;
  private FileSystem hdfs;

  // === Methods ===============================================================

  @Override
  protected void setup( Context context )
    throws IOException, InterruptedException {

    final Configuration configuration = context.getConfiguration();
    hdfs = FileSystem.get( configuration );
    final URI[] cachedFiles = context.getCacheFiles();

    if( cachedFiles != null && cachedFiles.length == 4 ) {
      final String groupTypeEvo = configuration.get( groupTypeEvoFileLabel );
      final String preference = configuration.get( userPreferenceFileLabel );
      final String scheduling = configuration.get( schedulingFileLabel );
      final String estPareto = configuration.get( paretoFileLabel );

      for( int i = 0; i < cachedFiles.length; i++ ) {
        final URI uri = cachedFiles[i];
        //if( uri.getPath().endsWith( groupTypeEvo ) ) {
        //  groupTypeEvolutionMap = readGroupTypeEvolution( hdfs, uri );
        //} else
        if( uri.getPath().endsWith( preference ) ) {
          preferenceMap = readUserPreferences( hdfs, uri );
        } else if( uri.getPath().endsWith( scheduling )){
          schedulingMap = readScheduling( hdfs, uri );
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
      Iterable<ViewSequenceWritable> values,
      Context context )
    throws IOException, InterruptedException {

    // trips with the same key
    final List<ViewSequenceWritable> trips = new ArrayList<>();

    for( ViewSequenceWritable s : values ) {
      trips.add( s );
    }

    final Random generator = new Random();
    final Integer index = (int) round( ( trips.size() - 1 ) * generator.nextDouble() );
    final ViewSequenceWritable selected = trips.get( index );

    final ViewSequenceValue value = new ViewSequenceValue( selected, preferenceMap, schedulingMap );

    context.write( new Text( "1" ), value );
  }


}
