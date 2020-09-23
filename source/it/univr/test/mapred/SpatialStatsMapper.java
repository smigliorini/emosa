package it.univr.test.mapred;

import it.univr.veronacard.shadoop.core.Step;
import it.univr.veronacard.shadoop.core.TripWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static it.univr.veronacard.shadoop.core.FileReader.readStepsFromCacheFile;
import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */

public class SpatialStatsMapper
  extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  // ===========================================================================

  private Map<String, Map<String, List<Step>>> possibleSteps;
  private FileSystem hdfs;

  // ===========================================================================

  @Override
  protected void setup( Context context )
    throws IOException, InterruptedException {

    hdfs = FileSystem.get( context.getConfiguration() );
    final URI[] cachedFiles = context.getCacheFiles();

    if( cachedFiles != null && cachedFiles.length > 0 ) {
      final URI stepFile = cachedFiles[0];
      if( stepFile != null ) {
        possibleSteps = readStepsFromCacheFile( hdfs, stepFile );
      }
    }
  }

  @Override
  protected void map( LongWritable key, Text value, Context context )
    throws IOException, InterruptedException {

    final TripWritable feature = new TripWritable();
    feature.fromText( value );
    feature.buildStepsFromCompleteMap( possibleSteps );

    /*final double d;
    if( feature.getGeometries() != null ) {
      final MultiLineString aGeometry =
        new MultiLineString
          ( feature.getGeometries().toArray
            ( new LineString[feature.getGeometries().size()] ),
            new GeometryFactory() );
      d = aGeometry.getLength();
    } else {
      d = 0;
    }*/

    final double d;
    if( feature.getDuration() != null ){
      d = feature.getDuration();
    } else {
      d = 0;
    }

    final String ck = format
      ( "%s_%s", feature.getProfile(), feature.getTravelMode() );

    context.write( new Text( ck ), new DoubleWritable( d ) );
  }

}
