package it.univr.veronacard;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
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

import static java.lang.Math.*;
import static java.lang.Math.abs;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class ContextBasedPartitioningStats {

  private static final String mainDir =
    "C:\\workspace\\projects\\veronacard_analysis\\test\\test_context_partitioning\\";

  private static final String files[] = {
    // "vc_2000_2019_spread",
    "vc_2000_2019_cluster_age_15_55"
  };

  private static final String xOutFile = "_results_x.csv";
  private static final String yOutFile = "_results_y.csv";
  private static final String tOutFile = "_results_t.csv";
  private static final String aOutFile = "_results_a.csv";

  private static final String xPointsFile = "_points_x.csv";
  private static final String yPointsFile = "_points_y.csv";
  private static final String tPointsFile = "_points_t.csv";
  private static final String aPointsFile = "_points_a.csv";

  private static final String attributes[] = {
    "x", "y", "time", "age"
  };

  private static final double cellSide = pow( 2, -9 );

  private static final Double xGridRG[] = {
    10.977600,
    10.988200,
    10.998800,
    11.009400
  };

  private static final Double xGridRT[] = {
    10.977600000000000,
    10.995440917968750,
    10.999051025390624,
    11.008639160156250,
    11.009400000000000
  };

  private static final Double yGridRG[] = {
    45.431300,
    45.437300,
    45.443300,
    45.449300
  };

  private static final Double yGridRT[] = {
    45.431300000000000,
    45.440673535156250,
    45.443508007812500,
    45.449299999999994
  };

  private static final Double tGridRG[] = {
    945574471601.721100,
    1156068153540.417500,
    1366561835479.114000,
    1577055517417.810500
  };

  private static final Double tGridRT[] = {
    945574471601.721100000000000,
    1155554252949.746800000000000,
    1366921565892.583500000000000,
    1577055517417.810500000000000
  };

  private static final Double aGridRG[] = {
    5.000000,
    35.000000,
    65.000000,
    95.000000
  };

  private static final Double aGridQT[] = {
    12.000000000000000,
    14.875000000000000,
    17.750000000000000,
    23.500000000000000,
    35.000000000000000,
    46.500000000000000,
    52.250000000000000,
    55.125000000000000,
    58.000000000000000
  };

  private static final String separator = ",";
  private static final long partSize = 1024 * 1024 * 128;

  public static void main( String[] args ) throws IOException {

    for( String fn : files ) {
      final File f = new File( mainDir, fn + ".csv" );
      final Map<Double, Integer> xResults = new HashMap<>();
      final Map<Double, Integer> yResults = new HashMap<>();
      final Map<Double, Integer> tResults = new HashMap<>();
      final Map<Double, Integer> aResults = new HashMap<>();
      processLines( f, xResults, yResults, tResults, aResults );

      // final long fileSize = f.length();
      // final int numParts = (int) ceil( pow( ceil( fileSize / partSize ), 1.0 / 4 ) );

      // =======================================================================

      final int[] xCountersRG = countElements( xGridRG, xResults );
      final int[] xCountersRT = countElements( xGridRT, xResults );

      final List<Double> xkeys = new ArrayList<>( xResults.keySet() );
      Collections.sort( xkeys );

      try( BufferedWriter bw =
             new BufferedWriter
               ( new FileWriter
                   ( new File( mainDir, fn + xOutFile ) ) ) ) {
        bw.write( String.format( "RG%s%s%s"
                                 + "RT%n",
                                 separator, separator, separator ));
        bw.write( String.format( "cell left%s"
                                 + "cell right%s"
                                 + "counter%s"
                                 + "cell left%s"
                                 + "cell right%s"
                                 + "counter%n",
                                 separator, separator, separator,
                                 separator, separator ) );

        for( int i = 0; i < max( xCountersRG.length, xCountersRT.length ); i++ ) {
          if( i < xCountersRG.length ){
            bw.write( String.format
              ( "%.15f%s" +
                "%.15f%s" +
                "%d%s",
                xGridRG[i],
                separator,
                xGridRG[i + 1],
                separator,
                xCountersRG[i],
                separator ) );
          } else {
            bw.write( String.format( "%s%s%s", separator, separator, separator ) );
          }

          if( i < xCountersRT.length ){
            bw.write( String.format
              ( "%.15f%s" +
                "%.15f%s" +
                "%d%n",
                xGridRT[i],
                separator,
                xGridRT[i + 1],
                separator,
                xCountersRT[i] ) );
          } else {
            bw.write( String.format( "%n" ) );
          }
        }
      }

      final int numCells =  (int) ceil( ( xkeys.get( xkeys.size() - 1 ) - xkeys.get( 0 ) ) / cellSide );

      final int[] xscCounters = smallCellCounters
        ( xkeys.get( 0 ), xkeys.get( xkeys.size() - 1 ), numCells, xResults );

      try( BufferedWriter bw =
             new BufferedWriter
               ( new FileWriter
                   ( new File( mainDir, fn + xPointsFile ) ) ) ) {

        double key = xkeys.get( 0 );
        for( int i = 0; i < xscCounters.length; i++ ) {
          bw.write( String.format( "%.15f,%d%n", key, xscCounters[i] ) );
          key += cellSide;
        }
      }

      // =======================================================================

      final int[] yCountersRG = countElements( yGridRG, yResults );
      final int[] yCountersRT = countElements( yGridRT, yResults );

      final List<Double> ykeys = new ArrayList<>( yResults.keySet() );
      Collections.sort( ykeys );

      try( BufferedWriter bw =
             new BufferedWriter
               ( new FileWriter
                   ( new File( mainDir, fn + yOutFile ) ) ) ) {
        bw.write( String.format( "RG%s%s%s"
                                 + "RT%n",
                                 separator, separator, separator ));
        bw.write( String.format( "cell left%s"
                                 + "cell right%s"
                                 + "counter%s"
                                 + "cell left%s"
                                 + "cell right%s"
                                 + "counter%n",
                                 separator, separator, separator,
                                 separator, separator ) );

        for( int i = 0; i < max( yCountersRG.length, yCountersRT.length ); i++ ) {
          if( i < yCountersRG.length ){
            bw.write( String.format
              ( "%.15f%s" +
                "%.15f%s" +
                "%d%s",
                yGridRG[i],
                separator,
                yGridRG[i + 1],
                separator,
                yCountersRG[i],
                separator ) );
          } else {
            bw.write( String.format( "%s%s%s", separator, separator, separator ) );
          }

          if( i < yCountersRT.length ){
            bw.write( String.format
              ( "%.15f%s" +
                "%.15f%s" +
                "%d%n",
                yGridRT[i],
                separator,
                yGridRT[i + 1],
                separator,
                yCountersRT[i] ) );
          } else {
            bw.write( String.format( "%n" ) );
          }
        }
      }

      final int[] yscCounters = smallCellCounters
        ( ykeys.get( 0 ), ykeys.get( ykeys.size() - 1 ), numCells, yResults );
      try( BufferedWriter bw =
             new BufferedWriter
               ( new FileWriter
                   ( new File( mainDir, fn + yPointsFile ) ) ) ) {

        double key = ykeys.get( 0 );
        for( int i = 0; i < yscCounters.length; i++ ) {
          bw.write( String.format( "%.15f,%d%n", key, yscCounters[i] ) );
          key += cellSide;
        }
      }

      // =======================================================================

      final int[] tCountersRG = countElements( tGridRG, tResults );
      final int[] tCountersRT = countElements( tGridRT, tResults );

      final List<Double> tkeys = new ArrayList<>( tResults.keySet() );
      Collections.sort( tkeys );

      try( BufferedWriter bw =
             new BufferedWriter
               ( new FileWriter
                   ( new File( mainDir, fn + tOutFile ) ) ) ) {
        bw.write( String.format( "RG%s%s%s"
                                 + "RT%n",
                                 separator, separator, separator ));
        bw.write( String.format( "cell left%s"
                                 + "cell right%s"
                                 + "counter%s"
                                 + "cell left%s"
                                 + "cell right%s"
                                 + "counter%n",
                                 separator, separator, separator,
                                 separator, separator ) );

        for( int i = 0; i < max( tCountersRG.length, tCountersRT.length ); i++ ) {
          if( i < tCountersRG.length ){
            bw.write( String.format
              ( "%.15f%s" +
                "%.15f%s" +
                "%d%s",
                tGridRG[i],
                separator,
                tGridRG[i + 1],
                separator,
                tCountersRG[i],
                separator ) );
          } else {
            bw.write( String.format( "%s%s%s", separator, separator, separator ) );
          }

          if( i < tCountersRT.length ){
            bw.write( String.format
              ( "%.15f%s" +
                "%.15f%s" +
                "%d%n",
                tGridRT[i],
                separator,
                tGridRT[i + 1],
                separator,
                tCountersRT[i] ) );
          } else {
            bw.write( String.format( "%n" ) );
          }
        }
      }

      final int[] tscCounters = smallCellCounters
        ( tkeys.get( 0 ), tkeys.get( tkeys.size() - 1 ), numCells, tResults );
      try( BufferedWriter bw =
             new BufferedWriter
               ( new FileWriter
                   ( new File( mainDir, fn + tPointsFile ) ) ) ) {

        double key = tkeys.get( 0 );
        for( int i = 0; i < tscCounters.length; i++ ) {
          bw.write( String.format( "%.15f,%d%n", key, tscCounters[i] ) );
          key += cellSide;
        }
      }

      // =======================================================================

      final int[] aCountersRG = countElements( aGridRG, aResults );
      final int[] aCountersQT = countElements( aGridQT, aResults );

      final List<Double> akeys = new ArrayList<>( aResults.keySet() );
      Collections.sort( akeys );

      try( BufferedWriter bw =
             new BufferedWriter
               ( new FileWriter
                   ( new File( mainDir, fn + aOutFile ) ) ) ) {
        bw.write( String.format( "RG%s%s%s"
                                 + "RT%n",
                                 separator, separator, separator ));
        bw.write( String.format( "cell left%s"
                                 + "cell right%s"
                                 + "counter%s"
                                 + "cell left%s"
                                 + "cell right%s"
                                 + "counter%n",
                                 separator, separator, separator,
                                 separator, separator ) );

        for( int i = 0; i < max( aCountersRG.length, aCountersQT.length ); i++ ) {
          if( i < aCountersRG.length ){
            bw.write( String.format
              ( "%.15f%s" +
                "%.15f%s" +
                "%d%s",
                aGridRG[i],
                separator,
                aGridRG[i + 1],
                separator,
                aCountersRG[i],
                separator ) );
          } else {
            bw.write( String.format( "%s%s%s", separator, separator, separator ) );
          }

          if( i < aCountersQT.length ){
            bw.write( String.format
              ( "%.15f%s" +
                "%.15f%s" +
                "%d%n",
                aGridQT[i],
                separator,
                aGridQT[i + 1],
                separator,
                aCountersQT[i] ) );
          } else {
            bw.write( String.format( "%n" ) );
          }
        }
      }

      final int[] ascCounters = smallCellCounters
        ( akeys.get( 0 ), akeys.get( akeys.size() - 1 ), numCells, aResults );
      try( BufferedWriter bw =
             new BufferedWriter
               ( new FileWriter
                   ( new File( mainDir, fn + aPointsFile ) ) ) ) {

        double key = akeys.get( 0 );
        for( int i = 0; i < ascCounters.length; i++ ) {
          bw.write( String.format( "%.15f,%d%n", key, ascCounters[i] ) );
          key += cellSide;
        }
      }
    }
  }

  // ===========================================================================

  private static void processLines
    ( File file,
      Map<Double, Integer> xResults,
      Map<Double, Integer> yResults,
      Map<Double, Integer> tResults,
      Map<Double, Integer> aResults ) {

    if( file == null ) {
      throw new NullPointerException();
    }
    if( xResults == null ) {
      throw new NullPointerException();
    }
    if( yResults == null ) {
      throw new NullPointerException();
    }
    if( tResults == null ) {
      throw new NullPointerException();
    }
    if( aResults == null ) {
      throw new NullPointerException();
    }

    try( BufferedReader br = new BufferedReader( new FileReader( file ) ) ) {
      String line;

      while( ( line = br.readLine() ) != null ) {
        final StringTokenizer tk = new StringTokenizer( line, "," );
        int i = 0;
        while( tk.hasMoreTokens() ) {
          final Double key = Double.parseDouble( tk.nextToken() );
          switch( i ) {
            case 0:
              Integer xcount = xResults.get( key );
              if( xcount == null ) {
                xcount = 0;
              }
              xResults.put( key, xcount + 1 );
              i = i + 1;
              break;
            case 1:
              Integer ycount = yResults.get( key );
              if( ycount == null ) {
                ycount = 0;
              }
              yResults.put( key, ycount + 1 );
              i = i + 1;
              break;
            case 2:
              Integer tcount = tResults.get( key );
              if( tcount == null ) {
                tcount = 0;
              }
              tResults.put( key, tcount + 1 );
              i = i + 1;
              break;
            case 3:
              Integer acount = aResults.get( key );
              if( acount == null ) {
                acount = 0;
              }
              aResults.put( key, acount + 1 );
              i = i + 1;
              break;
            default:
              break;
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
   *
   * @param grid
   * @param values
   * @return
   */

  private static int[] countElements
  ( Double[] grid,
    Map<Double,Integer> values ){

    if( grid == null ) {
      throw new NullPointerException();
    }
    if( values == null ) {
      throw new NullPointerException();
    }

    final int[] counters = new int[grid.length-1];

    final List<Double> keys = new ArrayList<>( values.keySet() );
    Collections.sort( keys );

    for( Double k : keys ) {
      for( int i = 1; i < grid.length; i++ ) {
        if( k >= grid[i - 1] &&
            k < grid[i] ) {
          counters[i - 1] += values.get( k );
        }
      }
      if( k.equals( keys.get( keys.size() - 1 ) ) ) {
        counters[grid.length - 2] += values.get( k );
      }
    }
    return counters;
  }

  /**
   * MISSING_COMMENT
   *
   * @param min
   * @param max
   * @param numCells
   * @param values
   * @return
   */

  private static int[] smallCellCounters
    ( double min,
      double max,
      int numCells,
      Map<Double,Integer> values ){

    final int[] counters = new int[numCells];
    final double cs = ( max - min ) / numCells;

    for( Double k : values.keySet() ){
      final int d;
      if( k.equals( max )){
        d = counters.length - 1;
      } else {
        d = (int) floor( ( k - min ) / cs );
      }
      counters[d] += values.get( k );
    }

    return counters;
  }

  /**
   * MISSING_COMMENT
   *
   * @param xGrid
   * @param xCounters
   * @param yGrid
   * @param yCounters
   * @param tGrid
   * @param tCounters
   * @param aGrid
   * @param aCounters
   * @return
   */

  public static double totalArea
  ( double[] xGrid,
    double[] xCounters,
    double[] yGrid,
    double[] yCounters,
    double[] tGrid,
    double[] tCounters,
    double[] aGrid,
    double[] aCounters ) {

    /*double totalAreaX = 0;
    double totalAreaY = 0;
    double totalAreaT = 0;
    double totalAreaA = 0;//*/

    double totalVolume = 0;

    /*for( int i = 0; i < xGrid.length - 1; i++ ) {
      if( xCounters[i] != 0 ) {
        totalAreaX += abs( xGrid[i + 1] - xGrid[i] );
      }
    }

    for( int i = 0; i < yGrid.length - 1; i++ ) {
      if( yCounters[i] != 0 ) {
        totalAreaY += abs( yGrid[i + 1] - yGrid[i] );
      }
    }

    for( int i = 0; i < tGrid.length - 1; i++ ) {
      if( tCounters[i] != 0 ) {
        totalAreaT += abs( tGrid[i + 1] - tGrid[i] );
      }
    }

    for( int i = 0; i < aGrid.length - 1; i++ ) {
      if( aCounters[i] != 0 ) {
        totalAreaA += abs( aGrid[i + 1] - aGrid[i] );
      }
    }//*/

    for( int x = 0; x < xGrid.length; x++ ) {
      for( int y = 0; y < yGrid.length; y++ ) {
        for( int t = 0; t < tGrid.length; t++ ) {
          for( int a = 0; a < aGrid.length; a++ ) {
            if( xCounters[x] != 0 && yCounters[y] != 0 &&
                tCounters[t] != 0 && aCounters[a] != 0 ) {
              totalVolume += abs( xGrid[x + 1] - xGrid[x] ) *
                             abs( yGrid[y + 1] - yGrid[y] ) *
                             abs( tGrid[t + 1] - tGrid[t] ) *
                             abs( aGrid[a + 1] - aGrid[a] );
            }
          }
        }
      }
    }

    //final double[] result = {totalAreaX, totalAreaY, totalAreaT, totalAreaA, totalVolume};
    return totalVolume;
  }

  /**
   * MISSING_COMMENT
   *
   * @param xGrid
   * @param xCounters
   * @param yGrid
   * @param yCounters
   * @param tGrid
   * @param tCounters
   * @param aGrid
   * @param aCounters
   * @return
   */

  public static double totalMargin
  ( double[] xGrid,
    double[] xCounters,
    double[] yGrid,
    double[] yCounters,
    double[] tGrid,
    double[] tCounters,
    double[] aGrid,
    double[] aCounters ) {

    double totalMargin = 0;
    for( int x = 0; x < xGrid.length; x++ ) {
      for( int y = 0; y < yGrid.length; y++ ) {
        for( int t = 0; t < tGrid.length; t++ ) {
          for( int a = 0; a < aGrid.length; a++ ) {
            if( xCounters[x] != 0 && yCounters[y] != 0 &&
                tCounters[t] != 0 && aCounters[a] != 0 ) {
              totalMargin += abs( xGrid[x + 1] - xGrid[x] ) +
                             abs( yGrid[y + 1] - yGrid[y] ) +
                             abs( tGrid[t + 1] - tGrid[t] ) +
                             abs( aGrid[a + 1] - aGrid[a] );
            }
          }
        }
      }
    }
    return totalMargin;
  }


  /**
   * MISSING_COMMENT
   *
   * @param partitions
   * @return
   */

  /*public static double totalOverlap( List<Partition> partitions ) {
    if( partitions == null ) {
      throw new NullPointerException();
    }

    double totalOverlap = 0;
    for( Partition p1 : partitions ) {
      totalOverlap += p1.mbr.getArea() * p1.numBlocks * ( p1.numBlocks - 1 ) / 2;
      for( Partition p2 : partitions ) {
        if( p1.id != p2.id && p1.mbr.intersects( p2.mbr ) ) {
          totalOverlap += p1.mbr.intersection( p2.mbr ).getArea() *
                          p1.numBlocks * p2.numBlocks;
        }
      }
    }
    return totalOverlap;
  }//*/

  /**
   * MISSING_COMMENT
   *
   * @param partitions
   * @return
   */

  /*public static double loadBalance( List<Partition> partitions ) {
    if( partitions == null ) {
      throw new NullPointerException();
    }

    double sum = 0;
    double std = 0;

    for( Partition p : partitions ) {
      sum += p.fileSize / 1024 / 1024;
    }

    double mean = sum / partitions.size();
    for( Partition p : partitions ) {
      std += Math.pow( ( p.fileSize / 1024 / 1024 ) - mean, 2 );
    }

    return Math.sqrt( std / partitions.size() );
  }//*/
}
