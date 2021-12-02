package it.univr.auditel.mapred;

import it.univr.auditel.shadoop.core.ViewSequenceValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

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

    final List<ViewSequenceValue> result = new ArrayList<>();


    final Iterator<ViewSequenceValue> it = values.iterator();
    while( it.hasNext() ){
      final ViewSequenceValue v = new ViewSequenceValue( it.next() );
      if( ! result.contains( v ) ){
        result.add( v );
      }
    }


    for( ViewSequenceValue vv : result ) {
      /*if( vv.getDuration() < 0 ){
        System.out.printf( "Negative duration. %n");
      }//*/
      context.write( key, vv );
    }
  }

}
