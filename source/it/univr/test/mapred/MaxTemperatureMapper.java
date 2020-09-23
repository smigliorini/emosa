package it.univr.test.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  private static int missing = 9999;

  @Override
  protected void map
    ( LongWritable key,
      Text value,
      Context context )
    throws IOException, InterruptedException {

    final String line = value.toString();
    if( line.length() > 92 ) {
      final String year = line.substring( 15, 19 );
      int airTemperature;
      if( line.charAt( 87 ) == '+' ) {
        airTemperature = Integer.parseInt( line.substring( 88, 92 ) );
      } else {
        airTemperature = Integer.parseInt( line.substring( 87, 92 ) );
      }
      final String quality = line.substring( 92, 93 );
      if( airTemperature != missing && quality.matches( "[01459]" ) ) {
        context.write( new Text( year ), new IntWritable( airTemperature ) );
      }
    }
  }
}
