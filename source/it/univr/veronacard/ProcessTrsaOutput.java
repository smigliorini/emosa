package it.univr.veronacard;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import java.awt.Color;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import static it.univr.utils.StatCharts.chartToImage;
import static it.univr.utils.StatCharts.writeImageToFile;
import static java.lang.Integer.parseInt;
import static java.lang.Math.max;
import static java.lang.String.format;

/**
 * The class process the output produced by the TRSA algorithm and compares the
 * number of visits in the historical dataset, the static TRSA output and the
 * dynamic TRSA output. It produces some CSV comparison files and some charts.
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class ProcessTrsaOutput {

  // private static final String run = "run_100";
  private static final String run = "";
  private static final String poi = "11";
  private static final Integer dynPerc = 50;
  private static final String dynPercLabel = dynPerc.toString();
  private static final String pert = "100";


  //private static final boolean fix = false;
  private static final boolean delay = false;
  private static final Double delayPerc = 1.0;
  private static final String delayPercLabel =
    delay ? "_delay_" + delayPerc.toString().replace( ".", "" ) : "";
  private static final int hourDelay = 2;

  private static final String outPath =
    "C:\\workspace\\projects\\veronacard_analysis\\test\\test_all_pois"
    + run + "\\%s";

  private static final String pathA = format
    // ( "static_pert_%s_temp_10\\output\\", pert );
    ( "poi_%s_pert_%s_temp_10_static\\output\\", poi, pert );
  private static final String pathB = format
    // ( "dynamic_pert_%s_temp_10_perc_%s\\output\\", pert, dynPercLabel );
    ( "poi_%s_pert_%s_temp_10_dynamic_%s\\output\\", poi, pert, dynPercLabel );

  // casa di giulietta poi = 3, arena poi = 1, castelvecchio = 11
  // private static final String poi = "3";

  private static final String poiFile = format
    ( "C:\\workspace\\projects\\veronacard_analysis\\resources\\"
      + "poi_%s_num_visits_day_45_2015.csv", poi );
  //+ "arena_num_visits_day_115_2016.csv";
  private static final String delimiter = ",";


  // ===========================================================================

  private static final int serialIndex = 1;
  private static final int stepsIndex = 2;
  private static final int stepsGeomIndex = 3;
  private static final int travelModeIndex = 4;
  private static final int profileIndex = 5;
  private static final int numStepsIndex = 6;
  private static final int durationIndex = 7;
  private static final int travelTimeIndex = 8;
  private static final int distanceIndex = 9;
  private static final int numScenicRoutesIndex = 10;
  private static final int smoothnessMeanIndex = 11;
  private static final int smoothnessStnDevIndex = 12;
  private static final int arrivalHoursIndex = 13;

  // ===========================================================================

  public static void main( String[] args ) throws FileNotFoundException {

    final List<String> historicalData = readLines( new File( poiFile ), true );
    final XYSeries originalSerie = buildPoiTraffic( historicalData, "Original" );

    int count = 0;
    for( int i = 0; i < originalSerie.getItemCount(); i++ ) {
      count += originalSerie.getDataItem( i ).getY().intValue();
    }
    System.out.printf( "original: %d%n", count );//*/

    // -------------------------------------------------------------------------

    final File staticFile = retrieveOutputFile
      ( new File( format( outPath, pathA ) ) );

    final List<String> staticLines = readLines( staticFile, false );
    final Map<Integer, Integer> staticResult = new HashMap<>();
    int c1 = 0;
    for( String s : staticLines ) {
      if( !processLine( s, staticResult, poi, false ) ) {
        c1 += 1;
      }
    }

    /*if( fix ) {
      for( Map.Entry<Integer, Integer> v : staticResult.entrySet() ) {
        if( v.getKey().equals( 8 ) ) {
          staticResult.put( 8, staticResult.get( v.getKey() ) - 15 );
        } else if( v.getKey().equals( 12 ) ) {
          staticResult.put( 12, staticResult.get( v.getKey() ) + 15 );
        } else if( v.getKey().equals( 9 ) ) {
          staticResult.put( 9, staticResult.get( v.getKey() ) - 5 );
        } else if( v.getKey().equals( 16 ) ) {
          staticResult.put( 16, staticResult.get( v.getKey() ) + 5 );
        }
      }
      staticResult.remove( 19 );
    }//*/

    final List<Integer> staticKeys = new ArrayList<>( staticResult.keySet() );
    Collections.sort( staticKeys );

    final XYSeries staticSerie = new XYSeries( "Static TRSA" );
    count = 0;
    for( Integer k : staticKeys ) {
      staticSerie.add( k, staticResult.get( k ) );
      count += staticResult.get( k );
    }
    System.out.printf( "Static result: %d + %d = %d <?> %d%n",
                       c1, count, c1 + count, staticLines.size() );

    // -------------------------------------------------------------------------

    final File dynFile = retrieveOutputFile
      ( new File( format( outPath, pathB ) ) );

    final List<String> dynLines = readLines( dynFile, false );
    final Map<Integer, Integer> dynResult = new HashMap<>();
    c1 = 0;
    int dynCount = 0;

    for( String s : dynLines ) {
      final boolean addDelay;

      if( delay && dynCount < dynLines.size() * delayPerc ) {
        addDelay = true;
      } else {
        addDelay = false;
      }
      dynCount += 1;

      if( !processLine( s, dynResult, poi, addDelay ) ) {
        c1 += 1;
      }
    }

    /*if( fix ) {

      for( Map.Entry<Integer, Integer> v : dynResult.entrySet() ) {
        if( v.getKey() == 19 ) {
          dynResult.put( 18, dynResult.get( 18 ) + dynResult.get( 19 ) );
        }
        if( v.getKey() == 9 ) {
          dynResult.put( 9, dynResult.get( 9 ) - 35 );
        }
        if( v.getKey() == 16 ) {
          dynResult.put( 16, dynResult.get( 16 ) + 5 );
        }
        if( v.getKey() == 17 ) {
          dynResult.put( 17, dynResult.get( 17 ) + 15 );
        }
        if( v.getKey() == 18 ) {
          dynResult.put( 18, dynResult.get( 18 ) + 15 );
        }
      }
      dynResult.remove( 19 );
    }//*/

    final List<Integer> dynKeys = new ArrayList<>( dynResult.keySet() );
    Collections.sort( dynKeys );

    final XYSeries dynSerie = new XYSeries( "Dynamic TRSA" );
    count = 0;

    for( int c = 0; c < dynKeys.size(); c++ ) {
      final Integer k = dynKeys.get( c );
      dynSerie.add( k, dynResult.get( k ) );
      count += dynResult.get( k );
    }
    System.out.printf( "Dynamic: %d + %d = %d <?> %d%n", c1, count, c1 + count, dynLines.size() );


    // -------------------------------------------------------------------------

    final File directory = new File
      //( format( outPath, format( "avg_%s%s", dynPercLabel, delayPercLabel ) ) );
      ( format( outPath, "avg" ) );
    if( !directory.exists() ) {
      directory.mkdirs();
    }

    final String path =
      format
        ( //"%s\\casa_giulietta_num_visits_day_45_2015.png",
          "%s\\poi_%s_num_visits_day_45_2015.png",
          directory, poi );
    buildChart( "POI " + poi, //"Casa Giulietta (2015/02/14)",
                new XYSeries[]{originalSerie, staticSerie, dynSerie},
                7, 23, "Hour", "Num. visits", path );

    // -------------------------------------------------------------------------

    final int items =
      max
        ( max( originalSerie.getItemCount(), staticSerie.getItemCount() ),
          dynSerie.getItemCount() );
    final StringBuilder b = new StringBuilder();
    b.append( format( "hour%s"
                      + "original_visits%s"
                      + "static_visits%s"
                      + "dynamic_visits%n",
                      delimiter, delimiter, delimiter ) );

    for( int i = 0; i < items; i++ ) {
      final double xValue =
        originalSerie.getItemCount() > i ?
          originalSerie.getDataItem( i ).getXValue() :
          staticSerie.getItemCount() > i ?
            staticSerie.getDataItem( i ).getXValue() :
            dynSerie.getDataItem( i ).getXValue();

      b.append( format( "%s%s%s%s%s%s%s%n",
                        xValue,
                        delimiter,
                        originalSerie.getItemCount() > i ?
                          originalSerie.getDataItem( i ).getYValue() : 0,
                        delimiter,
                        staticSerie.getItemCount() > i ?
                          staticSerie.getDataItem( i ).getYValue() : 0,
                        delimiter,
                        dynSerie.getItemCount() > i ?
                          dynSerie.getDataItem( i ).getYValue() : 0 ) );
    }

    final String filepath =
      //format
      //  ( "%s\\casa_giulietta_num_visits_day_45_2015.csv",
      //    directory );
      format
        ( "%s\\poi_%s_num_visits_day_45_2015.csv",
          directory, poi );
    final PrintWriter indexWriter = new PrintWriter( filepath );
    indexWriter.write( b.toString() );
    indexWriter.close();
  }


  // ===========================================================================

  private static boolean processLine
    ( String line,
      Map<Integer, Integer> result,
      String poi,
      boolean addDelay ) {

    if( line == null ) {
      throw new NullPointerException();
    }
    if( result == null ) {
      throw new NullPointerException();
    }
    if( poi == null ) {
      throw new NullPointerException();
    }

    final StringTokenizer tk = new StringTokenizer( line, "\t" );
    int index = 0;

    int poiPosition = 0;
    int hour = -1;

    while( tk.hasMoreTokens() ) {
      String current = tk.nextToken();

      if( index == stepsIndex ) { // steps
        final StringTokenizer inTk = new StringTokenizer( current, "-" );

        boolean found = false;
        while( inTk.hasMoreTokens() && !found ) {
          String s = inTk.nextToken();
          s = s.replace( "(", "" );
          s = s.replace( ")", "" );

          final String f = s.substring( 0, s.indexOf( "," ) );
          if( f.equals( poi ) ) {
            found = true;
          } else if( !inTk.hasMoreTokens() ) {
            final String l = s.substring( s.indexOf( "," ) + 1 );
            if( l.equals( poi ) ) {
              found = true;
              poiPosition++;
            }
          } else { //if( inTk.hasMoreTokens() ) {
            poiPosition++;
          }
        }

        if( !found ) {
          poiPosition = -1;
        }

      } else if( index == arrivalHoursIndex ) { // times
        if( poiPosition == -1 ) {
          hour = -1;
        } else {
          current = current.replace( "(", "" );
          current = current.replace( ",)", "" );

          final StringTokenizer inTk = new StringTokenizer( current, "," );
          final List<Integer> hours = new ArrayList<>();
          while( inTk.hasMoreTokens() ) {
            final String s = inTk.nextToken();
            hours.add( Integer.parseInt( s ) );
          }

          hour = hours.get( poiPosition );

          if( addDelay ) {
            /*final Random rg = new Random();
            final int delayPos = rg.nextInt( hours.size() );

            for( int ii = 0; ii < delayPos; ii++ ){
              hours.set( ii, hours.get( ii ) + hourDelay );
            }//*/

            /*final Random rg = new Random();
            if( rg.nextDouble() < 0.33 ){
              hour = hour + hourDelay;
            } else if ( rg.nextDouble() > 0.33 && rg.nextDouble() < 0.66 ) {
              hour = hour - hourDelay;
            } else if( rg.nextDouble() > 0.66 ){
              hour = hour + 2 * hourDelay;
            }//*/

            if( poiPosition < hours.size() - 1 ) {
              hour = hours.get( poiPosition + 1 );
            } else if( poiPosition > 0 ) {
              hour = hours.get( poiPosition - 1 );
            }
          }



          /*int i = 0;
          while( inTk.hasMoreTokens() && hour == -1 ) {
            final String s = inTk.nextToken();
            if( i == poiPosition ) {
              hour = Integer.parseInt( s );
              if( addDelay ) {
                hour += hourDelay;
              }
            }
            i++;
          }//*/
        }
      }
      index++;
    }

    if( hour != -1 ) {
      Integer v = result.get( hour );
      if( v == null ) {
        v = 0;
      }
      v += 1;
      result.put( hour, v );
      return true;
    } else {
      return false;
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param file
   * @param header
   * @return
   */

  private static List<String> readLines( File file, boolean header ) {
    final List<String> lines = new ArrayList<>();

    try( BufferedReader br = new BufferedReader( new FileReader( file ) ) ) {
      String line;
      int lineCount = 0;

      while( ( line = br.readLine() ) != null ) {
        if( ( !header || lineCount > 0 ) && line.trim().length() > 0 ) {
          lines.add( line );
        }
        lineCount++;
      }
    } catch( FileNotFoundException e ) {
      System.out.printf( "File \"%s\" not found.%n", file );
    } catch( IOException e ) {
      System.out.printf( "Unable to read file \"%s\".%n", file );
    }

    return lines;
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param lines
   * @param serieLabel
   * @return
   */

  private static XYSeries buildPoiTraffic( List<String> lines, String serieLabel ) {
    if( lines == null ) {
      throw new NullPointerException();
    }

    final XYSeries serie = new XYSeries( serieLabel );

    for( String l : lines ) {
      final StringTokenizer t = new StringTokenizer( l, delimiter );
      int hour = 0;
      int visits = 0;
      int index = 0;

      while( t.hasMoreTokens() ) {
        switch( index ) {
          case 0:
            t.nextToken();
            index++; // site_name
            break;
          case 1:
            t.nextToken();
            index++; // doy
            break;
          case 2:
            hour = parseInt( t.nextToken() );
            index++;
            break;
          case 3:
            visits = parseInt( t.nextToken() );
            index++;
            break;
          default:
            throw new IllegalStateException( "Invalid token index" );
        }
      }
      serie.add( hour, visits );
    }

    return serie;
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param poi
   * @param series
   * @param xLabel
   * @param yLabel
   * @param path
   */

  private static void buildChart
  ( String poi,
    XYSeries[] series,
    int minDomainValue,
    int maxDomainValue,
    String xLabel,
    String yLabel,
    String path ) {

    if( poi == null ) {
      throw new NullPointerException();
    }
    if( series == null ) {
      throw new NullPointerException();
    }
    if( path == null ) {
      throw new NullPointerException();
    }

    final XYSeriesCollection collection = new XYSeriesCollection();
    for( XYSeries s : series ) {
      collection.addSeries( s );
    }

    final JFreeChart chart = ChartFactory.createXYLineChart
      ( format( "%s", poi ),
        xLabel,
        yLabel,
        collection,
        PlotOrientation.VERTICAL,
        true,
        false,
        false );

    final XYPlot plot = chart.getXYPlot();
    plot.setBackgroundPaint( Color.WHITE );
    plot.setDomainGridlinesVisible( true );
    //plot.setDomainGridlinePaint( Color.GRAY );
    plot.setDomainGridlinesVisible( false );
    plot.setRangeGridlinePaint( Color.GRAY );
    plot.setRangeGridlinesVisible( true );

    final ValueAxis va = plot.getDomainAxis();
    va.setRange( minDomainValue, maxDomainValue );

    //final NumberAxis domain = new NumberAxis( xLabel );
    va.setStandardTickUnits( NumberAxis.createIntegerTickUnits() );
    //plot.setDomainAxis( domain );

    final byte[] image = chartToImage( chart, 2000, 1000 );
    writeImageToFile( image, path );
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param directory
   * @return
   */

  private static File retrieveOutputFile( File directory ) {
    if( directory == null ) {
      throw new NullPointerException();
    }

    if( directory.exists() ) {
      final File[] filteredDirs =
        directory.listFiles( new FilenameFilter() {
          @Override
          public boolean accept( File dir, String name ) {
            return name.toLowerCase().contains( "_trsa" );
          }
        } );

      if( filteredDirs == null || filteredDirs.length != 1 ) {
        System.out.printf( "Unique TRSA output not found in \"%s\".%n", directory );
        System.exit( 1 );
      }

      final File[] filteredFiles = filteredDirs[0].listFiles( new FilenameFilter() {
        @Override
        public boolean accept( File dir, String name ) {
          return name.toLowerCase().equals( "part-r-00000" );
        }
      } );

      if( filteredFiles == null || filteredFiles.length != 1 ) {
        System.out.printf( "Unique TRSA output not found in \"%s\".%n", directory );
        System.exit( 1 );
      }

      return filteredFiles[0];
    } else {
      System.out.printf( "Directory \"%s\" does not exist.%n", directory );
      System.exit( 1 );
    }

    return null;
  }
}
