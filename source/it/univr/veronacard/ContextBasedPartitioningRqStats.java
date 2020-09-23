package it.univr.veronacard;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class ContextBasedPartitioningRqStats {

  private static final String durationLabel = "DURATION:";
  private static final String numMapTasksLabel = "Total number of map tasks:";
  private static final String multiDimTechnique = "md";
  private static final String boxCountingTechnique = "bc";
  private static final String randomTechnique = "csv";

  private static final String outFile = "rq_results.csv";

  // === Methods ===============================================================

  public static void main( String[] args ) {

    final File mainDir = new File( args[0] );

    final File[] logFiles = mainDir.listFiles( new FileFilter() {
      @Override
      public boolean accept( File file ) {
        if( file.getName().endsWith( ".log" ) ||
            file.getName().endsWith( ".LOG" ) ) {
          return true;
        } else {
          return false;
        }
      }
    } );

    final Map<String, RangeQueryStats> results = new HashMap<>();

    for( File f : logFiles ) {
      final String[] tokens = processFileName( f.getName() );
      final String partTech = tokens[0];
      final String key = format( "%s_%s", tokens[1], tokens[2] );

      RangeQueryStats stats = results.get( key );
      if( stats == null ) {
        stats = new RangeQueryStats();
        stats.overlap = Double.parseDouble( tokens[1] );
        stats.iteration = Integer.parseInt( tokens[2] );
      }
      processFile( f, stats, partTech );
      results.put( key, stats );
    }

    final List<String> keys = new ArrayList<>( results.keySet() );
    Collections.sort( keys );


    try( BufferedWriter bw =
           new BufferedWriter
             ( new FileWriter
                 ( new File( mainDir, outFile ) ) ) ) {

      bw.write( format( "overlap, iteration, "
                        + "ms RP, sec RP, numMaps RP,"
                        + "ms MD, sec MD, numMaps MD, "
                        + "ms BC, sec BC, numMaps BC%n" ) );

      for( String k : keys ) {
        final RangeQueryStats stats = results.get( k );
        bw.write( format( "%s,%s,"
                          + "%s,%s,%s,"
                          + "%s,%s,%s,"
                          + "%s,%s,%s%n",
                          stats.overlap, stats.iteration,
                          stats.durationRp != null ? stats.durationRp :  "",
                          stats.durationRp != null ? stats.durationRp / 1000.0 : "",
                          stats.numMapsRp,
                          stats.durationMd != null ? stats.durationMd : "",
                          stats.durationMd != null ? stats.durationMd / 1000.0 : "",
                          stats.numMapsMd,
                          stats.durationBc != null ? stats.durationBc : "",
                          stats.durationBc != null ? stats.durationBc / 1000.0 : "",
                          stats.numMapsBc ) );
      }
    } catch( IOException e ) {
      e.printStackTrace();
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param filename
   * @return
   */

  private static String[] processFileName( String filename ) {

    if( filename == null ) {
      throw new NullPointerException();
    }

    final StringTokenizer tk = new StringTokenizer( filename, "_" );
    int i = 0;

    final String[] result = new String[3];

    while( tk.hasMoreTokens() ) {
      final String t = tk.nextToken();
      switch( i ) {
        case 0: // output
          i += 1;
          break;
        case 1: // partitioning techinque
          result[0] = t;
          i += 1;
          break;
        case 2: // rq
          i += 1;
          break;
        case 3: // overlap integer part
          result[1] = t;
          i += 1;
          break;
        case 4: // overlap decimal part
          result[1] = result[1] + "." + t;
          i += 1;
          break;
        case 5: // iteration number
          result[2] = t.replace( ".log", "" )
                       .replace( ".LOG", "" );
          i += 1;
          break;
        default:
          i += 1;
          break;
      }
    }
    return result;
  }


  /**
   * MISSING_COMMENT
   *
   * @param file
   * @param stats
   * @param technique
   */

  private static void processFile
  ( File file,
    RangeQueryStats stats,
    String technique ) {

    if( file == null ) {
      throw new NullPointerException();
    }
    if( stats == null ) {
      throw new NullPointerException();
    }
    if( technique == null ) {
      throw new NullPointerException();
    }

    try( BufferedReader br = new BufferedReader( new FileReader( file ) ) ) {
      String line;

      while( ( line = br.readLine() ) != null ) {
        if( line.startsWith( durationLabel ) ) {
          final String v = line.substring( durationLabel.length() ).trim();
          if( technique.equalsIgnoreCase( boxCountingTechnique ) ) {
            stats.durationBc = Long.parseLong( v );
          } else if( technique.equalsIgnoreCase( multiDimTechnique ) ) {
            stats.durationMd = Long.parseLong( v );
          } else if( technique.equalsIgnoreCase( randomTechnique )){
            stats.durationRp = Long.parseLong( v );
          }

        } else if( line.startsWith( numMapTasksLabel ) ) {
          final String v = line.substring( numMapTasksLabel.length() )
                               .replace( ".", "" )
                               .trim();
          if( technique.equalsIgnoreCase( boxCountingTechnique ) ) {
            stats.numMapsBc = Integer.parseInt( v );
          } else if( technique.equalsIgnoreCase( multiDimTechnique ) ) {
            stats.numMapsMd = Integer.parseInt( v );
          } else if( technique.equalsIgnoreCase( randomTechnique )){
            stats.numMapsRp = Integer.parseInt( v );
          }
        }

      }
    } catch( FileNotFoundException e ) {
      System.out.printf( "File \"%s\" not found.%n", file );
    } catch( IOException e ) {
      System.out.printf( "Unable to read file \"%s\".%n", file );
    }
  }


  /**
   * MISSING_COMMENT
   */

  static class RangeQueryStats {
    private Double overlap;
    private Integer iteration;
    private Long durationRp;
    private Long durationBc;
    private Long durationMd;
    private Integer numMapsRp;
    private Integer numMapsBc;
    private Integer numMapsMd;
  }
}


