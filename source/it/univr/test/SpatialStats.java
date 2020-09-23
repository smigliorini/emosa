package it.univr.test;

import it.univr.test.mapred.SpatialStatsMapper;
import it.univr.test.mapred.SpatialStatsReducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Date;

import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class SpatialStats {

  // logger
  private static final Log logger = LogFactory.getLog( SpatialStats.class );


  /**
   * MISSING_COMMENT
   *
   * @param args
   * @throws java.io.IOException
   */
  public static void main( String[] args )
    throws IOException,
    ClassNotFoundException,
    InterruptedException {

    final Configuration configuration = new Configuration();
    configuration.set( "shape", "wkt" );

    final Job job = Job.getInstance( configuration );
    job.setJarByClass( SpatialStats.class );
    job.setJobName( "SpatialStatistics" );
    logger.info( "SpatialStatistics starts: job created..." );

    // -------------------------------------------------------------------------

    final String steps = "g_paths.csv";
    final FileSystem cfs = FileSystem.get( configuration );
    final Path path = cfs.resolvePath( new Path( format( "test04/%s", steps ) ) );

    job.addCacheFile( path.toUri() );

    // -------------------------------------------------------------------------

    FileInputFormat.addInputPath
      ( job, new Path( format( "test04/%s", args[0] ) ) );

    final long timestamp = new Date().getTime();
    FileOutputFormat.setOutputPath
      ( job, new Path( format( "test04/%s/%s", args[1], timestamp ) ) );

    job.setMapperClass( SpatialStatsMapper.class );
    job.setReducerClass( SpatialStatsReducer.class );

    // output produced by the map!
    job.setOutputKeyClass( Text.class );
    job.setOutputValueClass( DoubleWritable.class );

    // running the job
    logger.info( "running job...." );
    final int result = job.waitForCompletion( true ) ? 0 : 1;

    logger.info( "SpatialStatistics ends..." );
    System.exit( result );
  }
}


