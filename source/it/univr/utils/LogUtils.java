package it.univr.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Jdk14Logger;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;

import static it.univr.utils.LogUtils.Level.*;
import static it.univr.utils.LogUtils.Level.INFO;
import static java.lang.String.*;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class LogUtils {

  private LogUtils() {
    // nothing here
  }

  public static enum Level{
    OFF,
    FATAL,
    ERROR,
    WARN,
    INFO,
    DEBUG,
    TRACE,
    ALL;
  }


  public static org.apache.log4j.Level toLog4jLevel( Level l ){
    if( l == null ) {
      throw new NullPointerException();
    }

    switch( l ){
      case OFF:
        return org.apache.log4j.Level.OFF;
      case FATAL:
        return org.apache.log4j.Level.FATAL;
      case ERROR:
        return org.apache.log4j.Level.ERROR;
      case WARN:
        return  org.apache.log4j.Level.WARN;
      case INFO:
        return org.apache.log4j.Level.INFO;
      case DEBUG:
        return org.apache.log4j.Level.DEBUG;
      case TRACE:
        return org.apache.log4j.Level.TRACE;
      case ALL:
        return org.apache.log4j.Level.ALL;
      default:
        throw new IllegalArgumentException();
    }
  }

  public static java.util.logging.Level toJuLevel( Level l ){
    if( l == null ) {
      throw new NullPointerException();
    }

    switch( l ){
      case OFF:
        return java.util.logging.Level.OFF;
      case FATAL:
        return java.util.logging.Level.SEVERE;
      case ERROR:
        return java.util.logging.Level.SEVERE;
      case WARN:
        return  java.util.logging.Level.WARNING;
      case INFO:
        return java.util.logging.Level.INFO;
      case DEBUG:
        return java.util.logging.Level.FINER;
      case TRACE:
        return java.util.logging.Level.FINEST;
      case ALL:
        return java.util.logging.Level.ALL;
      default:
        throw new IllegalArgumentException();
    }
  }



  /**
   * MISSING_COMMENT
   *
   * @param log
   * @param level
   */

  public static void setLogLevel( Log log, Level level ) {
    if( log == null ) {
      throw new NullPointerException();
    }
    if( level == null ) {
      throw new NullPointerException();
    }


    if( log instanceof Log4JLogger ) {
      final Log4JLogger jlog = (Log4JLogger) log;
      jlog.getLogger().setLevel( toLog4jLevel( level ) );
      //log.info( format( "Set log level to: %s.%n",
      //                  jlog.getLogger().getLevel() ) );

    } else if( log instanceof Jdk14Logger ) {
      final Jdk14Logger jlog = (Jdk14Logger) log;
      jlog.getLogger().setLevel( toJuLevel( level ) );
      //log.info( format( "Set log level to: %s.%n",
      //                  jlog.getLogger().getLevel() ) );

    } else {
      System.out.printf( "Sorry, " + log.getClass() + " not supported.<br />" );
      //log.info( format( "Log \"%s\" not supported.%n",
      //                  log.getClass() ) );
    }
  }
  
  
  // ===========================================================================
  
  private static final String[] logClasses = {
    "org.apache.hadoop.security.UserGroupInformation",
    "org.apache.hadoop.conf.Configuration",
    "org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter",
    "org.apache.hadoop.mapreduce.task.reduce.LocalFetcher",
    "org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl",
    "org.apache.hadoop.mapred.LocalJobRunner",
    "org.apache.hadoop.metrics2.lib.MutableMetricsFactory",
    "org.apache.hadoop.metrics2.impl.MetricsSystemImpl",
    "org.apache.hadoop.util.Shell",
    "org.apache.hadoop.security.authentication.util.KerberosName",
    "org.apache.hadoop.security.Groups",
    "org.apache.hadoop.util.NativeCodeLoader",
    "org.apache.hadoop.util.PerformanceAdvisory",
    "org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback",
    "org.apache.hadoop.security.Groups",
    "org.apache.htrace.core.Tracer",
    "org.apache.hadoop.mapreduce.Cluster",
    "org.apache.hadoop.mapreduce.JobSubmitter",
    "org.apache.hadoop.mapreduce.JobResourceUploader",
    "org.apache.hadoop.mapreduce.lib.input.FileInputFormat",
    "org.apache.hadoop.yarn.util.FSDownload",
    "org.apache.hadoop.mapred.SortedRanges",
    "org.apache.hadoop.mapred.Task",
    "org.apache.hadoop.mapred.MapTask",
    "org.apache.hadoop.mapreduce.task.reduce.EventFetcher",
    "org.apache.hadoop.mapreduce.task.reduce.ShuffleSchedulerImpl",

  };
  
  public static void suppressDebugLogs( boolean flag ){
    for( String s : logClasses ) {
      setLogLevel( LogFactory.getLog( s ), flag ? ERROR : DEBUG );
    }

  }
}
