package it.univr.test.mapred;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class SpatialStatsReducer
  extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  @Override
  protected void reduce
    ( Text key,
      Iterable<DoubleWritable> values,
      Context context )
    throws IOException, InterruptedException {

    int counter = 0;
    double total = 0;
    for( DoubleWritable value : values ) {
      total += value.get();
      counter++;
    }
    context.write( key, new DoubleWritable( total / counter ) );
  }
}
