package it.univr.auditel.mapred;

import it.univr.auditel.shadoop.core.ViewSequenceValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashSet;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */

public class EstParetoFrontReducer
  extends Reducer<Text, ViewSequenceValue, Text, ViewSequenceValue> {

  @Override
  protected void reduce
    ( Text key,
      Iterable<ViewSequenceValue> values,
      Context context )
    throws IOException, InterruptedException {

    final HashSet<ViewSequenceValue> result = new HashSet<>();

    int i = 0;
    for( ViewSequenceValue vv : values ) {
      result.add( vv );
      i++;
    }
    System.out.println
      ( String.format
        ( "Number of processed lines: %d, Pareto-front size: %d ",
          i, result.size() ) );

    for( ViewSequenceValue vv : result ) {
      context.write( key, vv );
    }
  }

}
