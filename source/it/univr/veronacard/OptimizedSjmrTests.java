package it.univr.veronacard;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import it.univr.utils.Statistics;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.*;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class OptimizedSjmrTests {

  final static String mainDirectory = "C:\\workspace\\projects\\veronacard_analysis\\test\\test_optimized_sjmr\\";
  //final static String filename1 = "test.txt";
  //final static String filename2 = "test.txt";
  final static String filename1 = "PRIMARYROADS.csv";
  //final static String filename2 = "AREAWATER.csv";
  final static String filename2 = "STATE.csv";

  // split size in Mbytes
  final static long splitSize = 13 * 1024 * 1024;

  public static void main( String[] args ) {

    final File file1 = new File( mainDirectory + filename1 );
    final Envelope mbr1 = new Envelope();
    final long rows1 = computeMbr( file1, false, mbr1 );
    System.out.printf( "MBR 1: (%.2f,%.2f,%.2f,%.2f).%n", mbr1.getMinX(), mbr1.getMinY(), mbr1.getMaxX(), mbr1.getMaxX() );
    // length() = size in bytes
    final double size1 = file1.length();
    System.out.printf( "D1 size: %.2f Mbytes.%n", size1 / 1024 / 1024 );
    System.out.printf( "D1 num rows: %d.%n", rows1 );

    final File file2 = new File( mainDirectory + filename2 );
    final Envelope mbr2 = new Envelope();
    final long rows2 = computeMbr( file2, false, mbr2 );
    System.out.printf( "MBR 2: (%.2f,%.2f,%.2f,%.2f).%n", mbr2.getMinX(), mbr2.getMinY(), mbr2.getMaxX(), mbr2.getMaxX() );
    // length() = size in bytes
    final double size2 = file2.length();
    System.out.printf( "D2 size: %.2f Mbytes.%n", size2 / 1024 / 1024 );
    System.out.printf( "D2 num rows: %d.%n", rows2 );

    final Envelope mbr = new Envelope( mbr1 );
    mbr.expandToInclude( mbr2 );
    System.out.printf( "[SJMR] MBR Union: (%.2f,%.2f,%.2f,%.2f).%n", mbr.getMinX(), mbr.getMinY(), mbr.getMaxX(), mbr.getMaxX() );
    System.out.printf( "[SJMR] MBR Union size: %.2f Mbytes.%n", ( size1 + size2 ) / 1024 / 1024 );
    System.out.printf( "[SJMR] MBR Union num rows: %d.%n", rows1 + rows2 );

    final int numCells = (int) ceil( ( size1 + size2 ) / splitSize );
    System.out.printf( "[SJMR] Number of cells: %d.%n", numCells );

    final int horizontalCells = (int) ceil( sqrt( numCells ) );
    final int verticalCells = horizontalCells;
    System.out.printf( "[SJMR] Horizontal/vertical cells: %d/%d.%n", horizontalCells, verticalCells );

    final double horizontalSize = ( mbr.getMaxX() - mbr.getMinX() ) / horizontalCells;
    final double verticalSize = ( mbr.getMaxY() - mbr.getMinY() ) / verticalCells;
    System.out.printf( "[SJMR] Horizontal/vertical cell side: %.2f/%.2f.%n", horizontalSize, verticalSize );

    // determine the number of geometries in each cells
    final Long[] cellCounters = new Long[numCells];
    for( int i = 0; i < numCells; i++ ) {
      cellCounters[i] = 0L;
    }
    countGeometriesPerCell( file1, false, mbr, horizontalSize, verticalSize, horizontalCells, verticalCells, cellCounters );
    countGeometriesPerCell( file2, false, mbr, horizontalSize, verticalSize, horizontalCells, verticalCells, cellCounters );
    for( int i = 0; i < numCells; i++ ) {
      System.out.printf( "[SJMR] Geometries in cell %d: %d.%n", i, cellCounters[i] );
    }

    final List<Double> l1 = new ArrayList<>();
    for( Long d : cellCounters ) {
      if( d > 0 ) {
        l1.add( new Double( d ) );
      }
    }
    final double stdev = Statistics.computeStandardDeviation( l1 );
    final double avg = Statistics.computeMean( l1 );
    final double rsd = stdev / avg;
    System.out.printf( "[SJMR] %%RDS: %.6f%%.%n", rsd * 100 );

    // --- ESJMR ---------------------------------------------------------------

    final double rowSize1 = size1 / rows1;
    final double rowSize2 = size2 / rows2;

    final Envelope mbrInt = mbr1.intersection( mbr2 );
    final int geomInt1 = countGeometriesInIntersection( file1, false, mbrInt );
    final int geomInt2 = countGeometriesInIntersection( file2, false, mbrInt );
    final double intSize = rowSize1 * geomInt1 + rowSize2 * geomInt2;
    System.out.printf( "[ESJMR] MBR Intersection size: %.2f Mbytes.%n", intSize / 1024 / 1024 );
    System.out.printf( "[ESJMR] MBR Intersection num rows: %d.%n", geomInt1 + geomInt2 );

    final int numCellsInt = (int) ceil( intSize / splitSize );
    System.out.printf( "[ESJMR] Number of cells: %d.%n", numCellsInt );

    final int horizontalCellsInt = (int) ceil( sqrt( numCellsInt ) );
    final int verticalCellsInt = horizontalCellsInt;
    System.out.printf( "[ESJMR] Horizontal/vertical cells: %d/%d.%n", horizontalCellsInt, verticalCellsInt );

    final double horizontalSizeInt = ( mbrInt.getMaxX() - mbrInt.getMinX() ) / horizontalCellsInt;
    final double verticalSizeInt = ( mbrInt.getMaxY() - mbrInt.getMinY() ) / verticalCellsInt;
    System.out.printf( "[ESJMR] Horizontal/vertical cell side: %.2f/%.2f.%n", horizontalSizeInt, verticalSizeInt );

    final int threshold = ( geomInt1 + geomInt2 ) / numCellsInt;
    System.out.printf( "[ESJMR] Threshold: %d.%n", threshold );

    // determine the number of geometries in each cells
    final Long[] cellCountersInt = new Long[numCellsInt];
    for( int i = 0; i < numCellsInt; i++ ) {
      cellCountersInt[i] = 0L;
    }
    countGeometriesPerCell( file1, false, mbrInt, horizontalSizeInt, verticalSizeInt, horizontalCellsInt, verticalCellsInt, cellCountersInt );
    countGeometriesPerCell( file2, false, mbrInt, horizontalSizeInt, verticalSizeInt, horizontalCellsInt, verticalCellsInt, cellCountersInt );

    final List<Double> l = new ArrayList<>();
    for( int i = 0; i < numCellsInt; i++ ) {
      if( cellCountersInt[i] > threshold ) {
        final int numRow = i / verticalCells;
        final int numColumn = i % verticalCells;

        final Envelope cell = new Envelope
          ( mbrInt.getMinX() + numRow * horizontalSizeInt,
            mbrInt.getMinX() + ( numRow + 1 ) * horizontalSizeInt,
            mbrInt.getMinY() + numColumn * verticalSizeInt,
            mbrInt.getMinY() + ( numColumn + 1 ) * verticalSizeInt );
        final long[] newCounters = repartitionCell( cell, file1, file2, false );

        for( int j = 0; j < 4; j++ ){
          if( newCounters[j] < threshold ){
            if( newCounters[j] > 0 ) {
              l.add( new Double( newCounters[j] ) );
            }
          } else {
            final Envelope cell2 = new Envelope
              ( mbrInt.getMinX() + numRow * horizontalSizeInt + ( j / 2 ) * horizontalSizeInt / 2,
                mbrInt.getMinX() + ( numRow + 1 ) * horizontalSizeInt + (( j / 2 )+1) * horizontalSizeInt / 2,
                mbrInt.getMinY() + numColumn * verticalSizeInt + ( j % 2 ) * horizontalSizeInt / 2,
                mbrInt.getMinY() + ( numColumn + 1 ) * verticalSizeInt + (( j % 2 )+1) * horizontalSizeInt / 2);
            final long[] newCounters2 = repartitionCell( cell2, file1, file2, false );

            for( int k = 0; k < 4; k++ ){
              if( newCounters2[k] > 0 ) {
                l.add( new Double( newCounters2[k] ) );
              }
            }
          }
        }
      } else {
        if( cellCounters[i] > 0 ) {
          l.add( new Double( cellCounters[i] ) );
        }
      }
    }

    final double stdev2 = Statistics.computeStandardDeviation( l );
    final double avg2 = Statistics.computeMean( l );
    final double rsd2 = stdev2 / avg2;
    System.out.printf( "[ESJMR] Number of cells: %d.%n", l.size() );
    System.out.printf( "[ESJMR] %%RDS: %.6f%%.%n", rsd2 * 100 );
  }

  private static Envelope[] splitIntoFour( Envelope cell ){
    final Envelope[] subCells = new Envelope[4];

    subCells[0] = new Envelope
      ( cell.getMinX(), cell.getMinX() + ( cell.getMaxX() - cell.getMinX() ) / 2,
        cell.getMinY(), cell.getMinY() + ( cell.getMaxY() - cell.getMinY() ) / 2 );

    subCells[1] = new Envelope
      ( cell.getMinX() + ( cell.getMaxX() - cell.getMinX() ) / 2, cell.getMaxX(),
        cell.getMinY(), cell.getMinY() + ( cell.getMaxY() - cell.getMinY() ) / 2 );

    subCells[2] = new Envelope
      ( cell.getMinX(), cell.getMinX() + ( cell.getMaxX() - cell.getMinX() ) / 2,
        cell.getMinY() + ( cell.getMaxY() - cell.getMinY() ) / 2, cell.getMaxY() );

    subCells[3] = new Envelope
      ( cell.getMinX() + ( cell.getMaxX() - cell.getMinX() ) / 2, cell.getMaxX(),
        cell.getMinY() + ( cell.getMaxY() - cell.getMinY() ) / 2, cell.getMaxY() );

    return subCells;
  }


  private static long[] repartitionCell( Envelope cell, File file1, File file2, boolean header ) {

    final long[] counter = new long[4];
    counter[0] = 0;
    counter[1] = 0;
    counter[2] = 0;
    counter[3] = 0;

    final Envelope[] subCells = splitIntoFour( cell );

    String line = null;
    int lineCount = 0;
    final WKTReader reader = new WKTReader();

    try( BufferedReader br = new BufferedReader( new FileReader( file1 ) ) ) {
      while( ( line = br.readLine() ) != null ) {
        if( ( !header || lineCount > 0 ) && line.trim().length() > 0 ) {

          line = line.replace( "\"", "" );
          final Geometry g = reader.read( line );

          if( g.getEnvelopeInternal().intersects( subCells[0] ) ) {
            counter[0]++;
          } else if( g.getEnvelopeInternal().intersects( subCells[1] ) ){
            counter[1]++;
          } else if( g.getEnvelopeInternal().intersects( subCells[2] )  ){
            counter[2]++;
          } else if( g.getEnvelopeInternal().intersects( subCells[3] )  ){
            counter[3]++;
          }
        }
      }
    } catch( FileNotFoundException e ) {
      System.out.printf( "File \"%s\" not found.%n", file1 );
    } catch( IOException e ) {
      System.out.printf( "Unable to read file \"%s\".%n", file1 );
    } catch( ParseException e ) {
      System.out.printf( "Unable to parse geometry \"%s\".%n", line );
    }

    try( BufferedReader br = new BufferedReader( new FileReader( file2 ) ) ) {
      while( ( line = br.readLine() ) != null ) {
        if( ( !header || lineCount > 0 ) && line.trim().length() > 0 ) {

          line = line.replace( "\"", "" );
          final Geometry g = reader.read( line );

          if( g.getEnvelopeInternal().intersects( subCells[0] ) ) {
            counter[0]++;
          } else if( g.getEnvelopeInternal().intersects( subCells[1] ) ){
            counter[1]++;
          } else if( g.getEnvelopeInternal().intersects( subCells[2] )  ){
            counter[2]++;
          } else if( g.getEnvelopeInternal().intersects( subCells[3] )  ) {
            counter[3]++;
          }
        }
      }
    } catch( FileNotFoundException e ) {
      System.out.printf( "File \"%s\" not found.%n", file1 );
    } catch( IOException e ) {
      System.out.printf( "Unable to read file \"%s\".%n", file1 );
    } catch( ParseException e ) {
      System.out.printf( "Unable to parse geometry \"%s\".%n", line );
    }

    return counter;
  }


  /**
   * MISSING_COMMENT
   *
   * @param file
   * @param header
   * @param mbr
   */

  private static int countGeometriesInIntersection
  ( File file,
    boolean header,
    Envelope mbr ) {

    final WKTReader reader = new WKTReader();

    String line = null;
    int lineCount = 0;

    try( BufferedReader br = new BufferedReader( new FileReader( file ) ) ) {
      while( ( line = br.readLine() ) != null ) {
        if( ( !header || lineCount > 0 ) && line.trim().length() > 0 ) {

          line = line.replace( "\"", "" );
          final Geometry g = reader.read( line );

          if( g.getEnvelopeInternal().intersects( mbr ) ) {
            lineCount++;
          }
        }
      }
    } catch( FileNotFoundException e ) {
      System.out.printf( "File \"%s\" not found.%n", file );
    } catch( IOException e ) {
      System.out.printf( "Unable to read file \"%s\".%n", file );
    } catch( ParseException e ) {
      System.out.printf( "Unable to parse geometry \"%s\".%n", line );
    }
    return lineCount;
  }

  /**
   * MISSING_COMMENT
   *
   * @param file
   * @param header
   * @param mbr
   * @param cellWidth
   * @param cellHeight
   * @param numHorizontalCells
   * @param numVerticalCells
   * @param cellCounters
   */

  private static void countGeometriesPerCell
  ( File file,
    boolean header,
    Envelope mbr,
    double cellWidth,
    double cellHeight,
    int numHorizontalCells,
    int numVerticalCells,
    Long[] cellCounters ) {

    final WKTReader reader = new WKTReader();

    String line = null;
    int lineCount = 0;

    try( BufferedReader br = new BufferedReader( new FileReader( file ) ) ) {
      while( ( line = br.readLine() ) != null ) {
        if( ( !header || lineCount > 0 ) && line.trim().length() > 0 ) {

          line = line.replace( "\"", "" );
          final Geometry g = reader.read( line );

          for( int i = 0; i < numHorizontalCells; i++ ) {
            for( int j = 0; j < numVerticalCells; j++ ) {
              final Envelope cell = new Envelope
                ( mbr.getMinX() + i * cellWidth,
                  mbr.getMinX() + ( i + 1 ) * cellWidth,
                  mbr.getMinY() + j * cellHeight,
                  mbr.getMinY() + ( j + 1 ) * cellHeight );
              if( cell.intersects( g.getEnvelopeInternal() ) ) {
                cellCounters[i + j] += 1;
              }
            }
          }
        }
      }
    } catch( FileNotFoundException e ) {
      System.out.printf( "File \"%s\" not found.%n", file );
    } catch( IOException e ) {
      System.out.printf( "Unable to read file \"%s\".%n", file );
    } catch( ParseException e ) {
      System.out.printf( "Unable to parse geometry \"%s\".%n", line );
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param file
   * @param header
   * @return
   */

  private static long computeMbr( File file, boolean header, Envelope mbr ) {
    if( file == null ) {
      throw new NullPointerException();
    }

    final WKTReader reader = new WKTReader();

    String line = null;
    int lineCount = 0;

    try( BufferedReader br = new BufferedReader( new FileReader( file ) ) ) {
      while( ( line = br.readLine() ) != null ) {
        if( ( !header || lineCount > 0 ) && line.trim().length() > 0 ) {

          line = line.substring( line.indexOf( "\"" ) + 1, line.lastIndexOf( "\"" ) );
          //line = line.replace( "\"", "" );
          final Geometry g = reader.read( line );
          if( mbr == null || mbr.isNull() ) {
            mbr.expandToInclude( g.getEnvelopeInternal() );
          } else {
            mbr.expandToInclude( g.getEnvelopeInternal() );
          }
        }
        lineCount++;
      }
    } catch( FileNotFoundException e ) {
      System.out.printf( "File \"%s\" not found.%n", file );
    } catch( IOException e ) {
      System.out.printf( "Unable to read file \"%s\".%n", file );
    } catch( ParseException e ) {
      System.out.printf( "Unable to parse geometry \"%s\".%n", line );
    }

    System.out.printf( "Number of read lines: %d.%n", lineCount );
    return lineCount;
  }
}
