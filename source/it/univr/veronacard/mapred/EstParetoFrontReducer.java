package it.univr.veronacard.mapred;

import it.univr.veronacard.shadoop.core.TripValue;
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
  extends Reducer<Text, TripValue, Text, TripValue> {

  @Override
  protected void reduce
    ( Text key,
      Iterable<TripValue> values,
      Context context )
    throws IOException, InterruptedException {

    final HashSet<TripValue> result = new HashSet<>();

    int i = 0;
    for( TripValue tv : values ) {
      result.add( tv );
      i++;
    }
    System.out.println
      ( String.format( "Number of processed lines: %d, Pareto-front size: %d ",
                       i, result.size() ));

    for( TripValue tv : result ) {
      context.write( key, tv );
    }
  }

}
