package it.univr.veronacard;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import sun.plugin.dom.exception.InvalidStateException;
import java.awt.Color;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import static it.univr.utils.StatCharts.chartToImage;
import static it.univr.utils.StatCharts.writeImageToFile;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.*;
import static java.lang.Math.ceil;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.Calendar.*;

/**
 * The class produces the files:
 *
 * - num_visits_stats.csv
 * - stay_time_stats.csv
 * - stay_time_by_visits_stats.csv
 *
 * used by the TRSA algorithm based on the files:
 * - poi_statistics_raw.csv
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class TouristStats {

  final static int startHour = 8;
  final static int endHour = 22;

  final static String poiFile =
    "C:\\workspace\\projects\\veronacard_analysis\\resources\\poi_statistics\\poi_statistics_raw.csv";

  final static String arenaFile =
    "C:\\workspace\\projects\\veronacard_analysis\\resources\\poi_statistics\\arena_visits_day_115.csv";

  final static String outPath =
    "C:\\workspace\\projects\\veronacard_analysis\\out\\%s";

  // ---------------------------------------------------------------------------

  final static String delimiter = ",";

  final static String poiRowHeader =
    format( "site_name%s"
            + "arriving_date%s"
            + "next_arriving_date%s"
            + "next_time_walking%s"
            + "next_time_driving%n",
            delimiter,
            delimiter,
            delimiter,
            delimiter );

  final static String visitsByPeriodHeader =
    format( "site_name%s"
            + "month%s"
            + "week_day%s"
            + "hour%s"
            + "avg_num_visits%n",
            delimiter,
            delimiter,
            delimiter,
            delimiter );

  final static String stayTimeByPeriodHeader =
    format( "site_name%s"
            + "month%s"
            + "week_day%s"
            + "hour%s"
            + "avg_stay_time (min)%n",
            delimiter,
            delimiter,
            delimiter,
            delimiter );

  final static String stayTimeByVisitsHeader =
    format( "site_name%s"
            + "num_visits%s"
            + "stay_time (min)%n",
            delimiter,
            delimiter );

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param args
   */

  public static void main( String[] args ) throws FileNotFoundException {

    // --- read poi visits -----------------------------------------------------

    final List<String> lines = readLines( new File( poiFile ), true );
    System.out.printf( "[LOG] Read %d lines.%n", lines.size() );

    final Map<String, List<PoiStatistics>> pois = buildPois( lines );
    normalizePois( pois );
    System.out.printf( "[LOG] Build %d POI data.%n", pois.size() );

    final Map<String, Map<String, Integer>> poiAvgVisits = new HashMap<>();
    final Map<String, Map<String, Integer>> poiAvgStayTimes = new HashMap<>();
    final Map<String, Map<String, List<Integer>>> allPoiStayTimes = new HashMap<>();
    buildPoiAvgStats( pois, poiAvgVisits, poiAvgStayTimes, allPoiStayTimes );

    final File numVisitOut = new File
      ( format( "%s\\%s", format( outPath, "avg" ), "num_visits" ) );
    buildPoiChart
      ( poiAvgVisits,
        numVisitOut,
        "Week Day / Day Hour",
        "Num. visits" );
    buildCsvByPeriod
      ( visitsByPeriodHeader,
        poiAvgVisits,
        format( outPath, "avg" ),
        "num_visits_stats.csv" );

    final File stayTimeOut = new File
      ( format( "%s\\%s", format( outPath, "avg" ), "stay_times" ) );
    buildPoiChart
      ( poiAvgStayTimes,
        stayTimeOut,
        "Week Day / Day Hour",
        "Stay time (min.)" );
    buildCsvByPeriod
      ( stayTimeByPeriodHeader,
        poiAvgStayTimes,
        format( outPath, "avg" ),
        "stay_time_stats.csv" );


    final Map<String, Map<Integer, Integer>> st =
      buildAvgStayTimePerNumVisits( poiAvgVisits, allPoiStayTimes );
    final File outDir = new File( format( outPath, "avg" ) );
    buildPoiChart( st, outDir, "%s: #People / Stay time (min)" );
    buildCsvByVisits
      ( stayTimeByVisitsHeader,
        st,
        format( outPath, "avg" ),
        "stay_time_by_visits_stats.csv" );//*/


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
      int linevalue = 0;

      while( ( line = br.readLine() ) != null ) {
        if( !header || linevalue > 0 ) {
          lines.add( line );
        }
        linevalue++;
      }
    } catch( FileNotFoundException e ) {
      System.out.printf( "File \"%s\" not found.%n", file );
    } catch( IOException e ) {
      System.out.printf( "Unable to read file \"%s\".%n", file );
    }

    return lines;
  }


  /**
   * MISSING_COMMENT
   *
   * @param lines
   * @return
   */

  private static Map<String, List<PoiStatistics>> buildPois
  ( List<String> lines ) {
    if( lines == null ) {
      throw new NullPointerException();
    }

    final Map<String, List<PoiStatistics>> pois = new HashMap<>();
    for( String l : lines ) {
      final StringTokenizer tk = new StringTokenizer( l, delimiter );
      final PoiStatistics poi = new PoiStatistics();

      int element = 0;

      while( tk.hasMoreTokens() ) {
        String token = tk.nextToken();
        token = token.replaceAll( "\"", "" );
        if( element == 0 ) {
          poi.setPoiName( token );
        } else if( element == 1 ) {
          poi.setArrivingDate( parseTimestamp( token ) );
        } else if( element == 2 ) {
          poi.setNextArrivingDate( parseTimestamp( token ) );
        } else if( element == 3 ) {
          poi.setNextTimeWalking( parseDouble( token ) );
        } else if( element == 4 ) {
          poi.setNextTimeDriving( parseDouble( token ) );
        } else {
          throw new InvalidStateException( "More tokens than expected!!" );
        }
        element++;
      }

      List<PoiStatistics> pl = pois.get( poi.getPoiName() );
      if( pl == null ) {
        pl = new ArrayList<>();
      }
      pl.add( poi );
      pois.put( poi.getPoiName(), pl );
    }

    return pois;
  }


  /**
   * Check the timestamp of arriving at the next POI and eventually delete it,
   * if is is performed the next day.
   *
   * @param pois
   * @return
   */
  private static void normalizePois( Map<String, List<PoiStatistics>> pois ) {

    if( pois == null ) {
      throw new NullPointerException();
    }

    for( Map.Entry<String, List<PoiStatistics>> e : pois.entrySet() ) {
      for( PoiStatistics v : e.getValue() ) {
        if( v.getNextArrivingDate().get( DAY_OF_YEAR ) !=
            v.getArrivingDate().get( DAY_OF_YEAR ) ) {
          v.setNextArrivingDate( null );
        }
      }
    }
  }


  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param pois
   * @return
   */

  private static void buildPoiAvgStats
  ( Map<String, List<PoiStatistics>> pois,
    Map<String, Map<String, Integer>> avgNumVisits,
    Map<String, Map<String, Integer>> avgStayTimes,
    Map<String, Map<String, List<Integer>>> allPoiStayTimes ) {

    if( pois == null ) {
      throw new NullPointerException();
    }

    if( avgNumVisits == null ) {
      throw new NullPointerException();
    }
    if( avgStayTimes == null ) {
      throw new NullPointerException();
    }
    if( allPoiStayTimes == null ) {
      throw new NullPointerException();
    }

    avgNumVisits.clear();
    avgStayTimes.clear();

    for( String poiName : pois.keySet() ) {

      // count the number of visits for the year/day_of_year/hour
      final Map<String, Integer> poiNumVisits = new HashMap<>();
      int minYear = MAX_VALUE;
      int maxYear = MIN_VALUE;
      int minDoy = MAX_VALUE;
      int maxDoy = MIN_VALUE;
      int minHour = MAX_VALUE;
      int maxHour = MIN_VALUE;
      final Map<String, List<Integer>> poiStayTimes = new HashMap<>();

      for( PoiStatistics poiStat : pois.get( poiName ) ) {
        final String key = format
          ( "%s_%s_%s",
            poiStat.getYear(),
            poiStat.getDayOfYear(),
            poiStat.getHour() );

        minYear = Integer.min( minYear, poiStat.getYear() );
        maxYear = Integer.max( maxYear, poiStat.getYear() );
        minDoy = Integer.min( minDoy, poiStat.getDayOfYear() );
        maxDoy = Integer.max( maxDoy, poiStat.getDayOfYear() );
        minHour = Integer.min( minHour, poiStat.getHour() );
        maxHour = Integer.max( maxHour, poiStat.getHour() );

        final Integer c =
          poiNumVisits.get( key ) != null ? poiNumVisits.get( key ) + 1 : 1;
        poiNumVisits.put( key, c );

        if( poiStat.getNextArrivingDate() != null ) {
          long t =
            poiStat.getNextArrivingDate().getTimeInMillis() -
            poiStat.getArrivingDate().getTimeInMillis();
          if( poiStat.getNextTimeWalking() != null &&
              t > poiStat.getNextTimeWalking() ) {
            t = t - round( poiStat.getNextTimeWalking() );
          }

          List<Integer> st = poiStayTimes.get( key );
          if( st == null ) {
            st = new ArrayList<>();
          }
          st.add( (int) ( t / 60000 ) ); // time in minutes!
          poiStayTimes.put( key, st );
        }
      }


      // retrieve the number of visits for the same day of week and hour
      final Map<String, List<Integer>> poiNumVisitReps = new HashMap<>();
      final Map<String, List<Integer>> poiStayTimeReps = new HashMap<>();

      for( int year = minYear; year <= maxYear; year++ ) {
        for( int doy = minDoy; doy <= maxDoy; doy++ ) {
          for( int hour = minHour; hour <= maxHour; hour++ ) {
            final String countKey = format( "%s_%s_%s", year, doy, hour );
            final Calendar c = new GregorianCalendar();
            c.set( YEAR, year );
            c.set( DAY_OF_YEAR, doy );
            final String key = format
              ( "%s_%s_%s",
                c.get( MONTH ),
                c.get( DAY_OF_WEEK ),
                hour );

            if( poiNumVisits.get( countKey ) != null ) {
              List<Integer> v = poiNumVisitReps.get( key );
              if( v == null ) {
                v = new ArrayList<>();
              }
              v.add( poiNumVisits.get( countKey ) );
              poiNumVisitReps.put( key, v );
            }

            if( poiStayTimes.get( countKey ) != null ) {
              List<Integer> v = poiStayTimeReps.get( key );
              if( v == null ) {
                v = new ArrayList<>();
              }
              v.addAll( poiStayTimes.get( countKey ) );
              poiStayTimeReps.put( key, v );
            }
          }
        }
      }

      // compute the averages
      final Map<String, Integer> avgPoiNumVisits = new HashMap<>();
      final Map<String, Integer> avgPoiStayTimes = new HashMap<>();
      final Map<String, List<Integer>> listPoiStayTimes = new HashMap<>(  );

      for( String key : poiNumVisitReps.keySet() ) {
        //avgPoiNumVisits.put
        //  ( key, (int) ceil( computeAvg( poiNumVisitReps.get( key ) ) ) );
        avgPoiNumVisits.put( key, computeMax( poiNumVisitReps.get( key ) ) );
        //avgPoiNumVisits.put( key, computeMedian( poiNumVisitReps.get( key ) ) );

        //avgPoiStayTimes.put( key, (int) ceil( computeAvg( poiStayTimeReps.get( key ) ) ) );
        //avgPoiStayTimes.put( key, computeMax( poiStayTimeReps.get( key ) ));
        avgPoiStayTimes.put( key, computeMedian( poiStayTimeReps.get( key ) ));
        //avgPoiStayTimes.put( key, computeMin( poiStayTimeReps.get( key ) ));

        listPoiStayTimes.put( key, poiStayTimeReps.get( key ) );
      }

      avgNumVisits.put( poiName, avgPoiNumVisits );
      avgStayTimes.put( poiName, avgPoiStayTimes );
      allPoiStayTimes.put( poiName, listPoiStayTimes );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param avgNumVisits
   * @param allStayTimes
   */

  private static Map<String, Map<Integer, Integer>> buildAvgStayTimePerNumVisits
  ( Map<String, Map<String, Integer>> avgNumVisits,
    Map<String, Map<String, List<Integer>>> allStayTimes ) {

    if( avgNumVisits == null ) {
      throw new NullPointerException();
    }

    if( allStayTimes == null ) {
      throw new NullPointerException();
    }

    final Map<String, Map<Integer, Integer>> result = new HashMap<>();
    for( String poiName : avgNumVisits.keySet() ) {

      final Map<String, Integer> poiNumVisits = avgNumVisits.get( poiName );
      final Map<String, List<Integer>> poiStayTimes = allStayTimes.get( poiName );

      final Map<Integer, List<Integer>> values = new HashMap<>();
      for( String k : poiNumVisits.keySet() ) {
        final Integer pnv = poiNumVisits.get( k );
        final List<Integer> pst = poiStayTimes.get( k );

        if( pst != null ) {
          List<Integer> v = values.get( pnv );
          if( v == null ) {
            v = new ArrayList<>();
          }
          v.addAll( pst );

          values.put( pnv, v );
        }
      }

      Map<Integer, Integer> pair = new HashMap<>();
      for( Map.Entry<Integer, List<Integer>> e : values.entrySet() ) {
        pair.put( e.getKey(), (int) ceil( computeMedian( e.getValue() ) ) );
        //pair.put( e.getKey(), (int) ceil( computeAvg( e.getValue() ) ) );
        //pair.put( e.getKey(), computeMax( e.getValue() ) );
      }

      result.put( poiName, pair );
    }
    return result;
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param poiAvg
   * @param directory
   * @param xLabel
   * @param yLabel
   */

  private static void buildPoiChart
  ( Map<String, Map<String, Integer>> poiAvg,
    File directory,
    String xLabel,
    String yLabel ) {

    if( poiAvg == null ) {
      throw new NullPointerException();
    }

    if( !directory.exists() ) {
      directory.mkdirs();
    }

    for( String poi : poiAvg.keySet() ) {
      final DefaultCategoryDataset d = new DefaultCategoryDataset();
      for( int month = JANUARY; month <= DECEMBER; month++ ) {
        for( int day = SUNDAY; day <= SATURDAY; day++ ) {
          for( int hour = 8; hour <= 22; hour++ ) {
            final String key = month + "_" + day + "_" + hour;
            final Integer value = ( poiAvg.get( poi ) ).get( key );
            if( value != null ) {
              d.addValue( value, format( "%2d", hour ), dayNames[day - 1] );
            } else {
              d.addValue( 0, format( "%2d", hour ), dayNames[day - 1] );
            }
          }
        }

        final String imgPath =
          format
            ( "%s\\%s_num_visits_%s.png",
              directory,
              normalizeName( poi ),
              format( "%02d", month + 1 ) );
        buildChart( poi, d, month, xLabel, yLabel, imgPath );
        System.out.printf
          ( "[LOG] Create visit chart for POI %s and month %s.%n",
            poi, monthNames[month] );
      }
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param poiAvg
   * @param directory
   */

  private static void buildPoiChart
  ( Map<String, Map<Integer, Integer>> poiAvg,
    File directory,
    String serieLabel ) {

    if( poiAvg == null ) {
      throw new NullPointerException();
    }

    if( !directory.exists() ) {
      directory.mkdirs();
    }

    for( String poiName : poiAvg.keySet() ) {
      final Map<Integer, Integer> current = poiAvg.get( poiName );

      final XYSeries serie = new XYSeries( format( serieLabel, poiName ) );

      int minDomainValue = Integer.MAX_VALUE;
      int maxDomainValue = Integer.MIN_VALUE;

      for( Map.Entry<Integer, Integer> e : current.entrySet() ) {
        if( e.getValue() > 0 ) {
          serie.add( e.getKey(), e.getValue() );
          minDomainValue = min( minDomainValue, e.getKey() );
          maxDomainValue = max( maxDomainValue, e.getKey() );
        }
      }

      final String imgPath =
        format
          ( "%s\\%s_stay_time_per_num_visits.png",
            directory, normalizeName( poiName ) );
      buildChart
        ( poiName,
          serie,
          minDomainValue,
          maxDomainValue,
          "Num. Visits",
          "Stay time",
          imgPath );
    }

  }

  // ===========================================================================

  private static void buildCsvByPeriod
    ( String header,
      Map<String, Map<String, Integer>> data,
      String directory,
      String filename ) throws FileNotFoundException {

    if( data == null ) {
      throw new NullPointerException();
    }

    if( directory == null ) {
      throw new NullPointerException();
    }

    if( filename == null ) {
      throw new NullPointerException();
    }

    final File outDir = new File( directory );
    if( !outDir.exists() ) {
      outDir.mkdirs();
    }

    final StringBuilder b = new StringBuilder();
    if( header != null ) {
      b.append( header );
    }

    for( String poiName : data.keySet() ) {
      for( int month = JANUARY; month <= DECEMBER; month++ ) {
        for( int day = SUNDAY; day <= SATURDAY; day++ ) {
          for( int hour = 8; hour <= 23; hour++ ) {

            final String label = month + "_" + day + "_" + hour;
            final Integer value = data.get( poiName ).get( label ) != null ?
              data.get( poiName ).get( label ) : 0;

            b.append
              ( format
                  ( "%s%s%s%s%s%s%s%s%s%n",
                    poiName, delimiter,
                    month, delimiter,
                    day, delimiter,
                    hour, delimiter,
                    value ) );

          }
        }
      }
    }

    final String path = format( "%s\\%s", outDir, filename );
    final PrintWriter indexWriter = new PrintWriter( path );
    indexWriter.write( b.toString() );
    indexWriter.close();
    System.out.printf
      ( "POI waiting time statistics written in %s.%n", filename );
  }


  /**
   * MISSING_COMMENT
   *
   * @param header
   * @param data
   * @param directory
   * @param filename
   * @throws FileNotFoundException
   */

  private static void buildCsvByVisits
  ( String header,
    Map<String, Map<Integer, Integer>> data,
    String directory,
    String filename ) throws FileNotFoundException {

    if( data == null ) {
      throw new NullPointerException();
    }

    if( directory == null ) {
      throw new NullPointerException();
    }

    if( filename == null ) {
      throw new NullPointerException();
    }

    final File outDir = new File( directory );
    if( !outDir.exists() ) {
      outDir.mkdirs();
    }

    final StringBuilder b = new StringBuilder();
    if( header != null ) {
      b.append( header );
    }

    for( String poiName : data.keySet() ) {
      for( int key : data.get( poiName ).keySet() ) {
        b.append
          ( format
              ( "%s%s%s%s%s%n",
                poiName, delimiter,
                key, delimiter,
                data.get( poiName ).get( key ) ) );
      }
    }

    final String path = format( "%s\\%s", outDir, filename );
    final PrintWriter indexWriter = new PrintWriter( path );
    indexWriter.write( b.toString() );
    indexWriter.close();
    System.out.printf
      ( "Waiting time by visits statistics written in %s.%n", filename );
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param timestamp
   * @return
   */

  private static Calendar parseTimestamp( String timestamp ) {
    if( timestamp == null ) {
      return null;
    }

    final SimpleDateFormat f = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
    final Calendar c = new GregorianCalendar();
    try {
      c.setTime( f.parse( timestamp ) );
      return c;
    } catch( ParseException e ) {
      System.out.printf( "[ERR] Error in parsing date:\"%s\".%n", timestamp );
      return null;
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param values
   * @return
   */

  private static int computeMax( List<Integer> values ){
    if( values == null || values.isEmpty() ) {
      return 0;
    }

    int result = Integer.MIN_VALUE;
    for( Integer n : values ) {
      result = max( result, n );
    }

    return result; //total / values.size();
  }

  /**
   * MISSING_COMMENT
   *
   * @param values
   * @return
   */

  private static int computeMin( List<Integer> values ){
    if( values == null || values.isEmpty() ) {
      return 0;
    }

    int result = Integer.MIN_VALUE;
    for( Integer n : values ) {
      result = min( result, n );
    }

    return result; //total / values.size();
  }


  /**
   * MISSING_COMMENT
   *
   * @param values
   * @return
   */

  private static double computeAvg( List<Integer> values ) {
    if( values == null || values.isEmpty() ) {
      return 0;
    }

    double total = 0;
    for( Integer n : values ) {
      total += n;
    }

    return total / values.size();
  }//*/


  /**
   * MISSING_COMMENT
   *
   * @param stats
   * @return
   */

  private static int computeMedian( List<Integer> stats ) {
    if( stats == null || stats.isEmpty() ) {
      return 0;
    }

    final List<Integer> values = new ArrayList<>();
    for( Integer p : stats ) {
      values.add( p );
    }

    Collections.sort( values );
    final int position = ( values.size() + 1 ) / 2;

    if( values.size() == 1 ) {
      return (int) Math.round( values.get( 0 ).doubleValue() );
    } else if( values.size() % 2 == 0 ) {
      return (int) Math.round( values.get( position - 1 ).doubleValue() );
    } else {
      final int a = values.get( position - 1 );
      final int b = values.get( position );
      return (int) Math.round((a + b ) / 2.0);
    }
  }//*/

  // ===========================================================================


  /**
   * MISSING_COMMENT
   *
   * @param poi
   * @param d
   * @param month
   * @param xLabel
   * @param yLabel
   * @param path
   */

  private static void buildChart
  ( String poi,
    DefaultCategoryDataset d,
    int month,
    String xLabel,
    String yLabel,
    String path ) {

    if( poi == null ) {
      throw new NullPointerException();
    }
    if( path == null ) {
      throw new NullPointerException();
    }

    final JFreeChart chart = ChartFactory.createBarChart
      ( format( "%s - %s", poi, monthNames[month] ),
        xLabel,
        yLabel,
        d,
        PlotOrientation.VERTICAL,
        true,
        false,
        false );

    final CategoryPlot plot = chart.getCategoryPlot();
    plot.setBackgroundPaint( Color.WHITE );
    plot.setDomainGridlinesVisible( true );
    //plot.setDomainGridlinePaint( Color.GRAY );
    plot.setDomainGridlinesVisible( false );
    plot.setRangeGridlinePaint( Color.GRAY );
    plot.setRangeGridlinesVisible( true );

    final CategoryAxis domainAxis = plot.getDomainAxis();
    domainAxis.setCategoryLabelPositions
      ( CategoryLabelPositions.UP_90 );
    //createUpRotationLabelPositions( Math.PI / 6.0 ) );

    final ValueAxis valueAxis = plot.getRangeAxis();
    valueAxis.setStandardTickUnits( NumberAxis.createIntegerTickUnits() );
    plot.setRangeAxis( valueAxis );

    final BarRenderer renderer = (BarRenderer) plot.getRenderer();
    renderer.setSeriesPaint( 0, Color.RED );
    renderer.setSeriesPaint( 1, Color.BLUE );
    renderer.setSeriesPaint( 2, Color.GREEN );
    renderer.setItemMargin( 0 );

    final byte[] image = chartToImage( chart, 2000, 1000 );
    writeImageToFile( image, path );
  }


  /**
   * MISSING_COMMENT
   *
   * @param poi
   * @param serie
   * @param xLabel
   * @param yLabel
   * @param path
   */

  private static void buildChart
  ( String poi,
    XYSeries serie,
    int minDomainValue,
    int maxDomainValue,
    String xLabel,
    String yLabel,
    String path ) {

    if( poi == null ) {
      throw new NullPointerException();
    }
    if( serie == null ) {
      throw new NullPointerException();
    }
    if( path == null ) {
      throw new NullPointerException();
    }

    final XYSeriesCollection collection = new XYSeriesCollection();
    collection.addSeries( serie );

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
    //plot.setRenderer( new XYSplineRenderer(  ) );

    final ValueAxis va = plot.getDomainAxis();
    va.setRange( minDomainValue, maxDomainValue );

    //final NumberAxis domain = new NumberAxis( xLabel );
    va.setStandardTickUnits( NumberAxis.createIntegerTickUnits() );
    //plot.setDomainAxis( domain );

    final byte[] image = chartToImage( chart, 2000, 1000 );
    writeImageToFile( image, path );
  }

  // ===========================================================================


  static class PoiStatistics {
    private String poiName;
    private Calendar arrivingDate;
    private Calendar nextArrivingDate;
    private Double nextTimeWalk;
    private Double nextTimeDriving;

    PoiStatistics() {
      this.poiName = null;
      this.arrivingDate = null;
      this.nextArrivingDate = null;
      this.nextTimeWalk = null;
      this.nextTimeDriving = null;
    }

    public String getPoiName() {
      return poiName;
    }

    public void setPoiName( String poiName ) {
      this.poiName = poiName;
    }

    public Calendar getArrivingDate() {
      return arrivingDate;
    }

    public void setArrivingDate( Calendar arrivingDate ) {
      this.arrivingDate = arrivingDate;
    }

    public Calendar getNextArrivingDate() {
      return nextArrivingDate;
    }

    public void setNextArrivingDate( Calendar nextArrivingDate ) {
      this.nextArrivingDate = nextArrivingDate;
    }

    public Double getNextTimeWalking() {
      return nextTimeWalk;
    }

    public void setNextTimeWalking( Double nextTimeWalk ) {
      this.nextTimeWalk = nextTimeWalk;
    }

    public Double getNextTimeDriving() {
      return nextTimeDriving;
    }

    public void setNextTimeDriving( Double nextTimeDriving ) {
      this.nextTimeDriving = nextTimeDriving;
    }


    /**
     * This field takes values SUNDAY (=1), MONDAY, TUESDAY, WEDNESDAY,
     * THURSDAY, FRIDAY, and SATURDAY.
     *
     * @return
     */
    public Integer getDayOfWeek() {
      if( arrivingDate != null ) {
        return arrivingDate.get( DAY_OF_WEEK );
      } else {
        return null;
      }
    }


    /**
     * MISSING_COMMENT
     *
     * @return
     */

    public Integer getDayOfYear() {
      if( arrivingDate != null ) {
        return arrivingDate.get( DAY_OF_YEAR );
      } else {
        return null;
      }
    }


    /**
     * MISSING_COMMENT
     *
     * @return
     */

    public Integer getYear() {
      if( arrivingDate != null ) {
        return arrivingDate.get( YEAR );
      } else {
        return null;
      }
    }


    /**
     * The first month of the year in the Gregorian and Julian calendars is
     * JANUARY which is 0.
     *
     * @return
     */
    public Integer getMonth() {
      if( arrivingDate != null ) {
        return arrivingDate.get( Calendar.MONTH );
      } else {
        return null;
      }
    }


    /**
     * HOUR_OF_DAY is used for the 24-hour clock. Midnight are represented by
     * 0.
     *
     * @return
     */

    public Integer getHour() {
      if( arrivingDate != null ) {
        return arrivingDate.get( HOUR_OF_DAY );
      } else {
        return null;
      }
    }

    private String getKey() {
      if( arrivingDate != null ) {
        return format
          ( "%s_%s_%s",
            getMonth(),
            getDayOfWeek(),
            getHour() );
      } else {
        return null;
      }
    }
  }

  // ===========================================================================

  private static String[] monthNames =
    {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul",
      "Aug", "Sep", "Oct", "Nov", "Dec"};

  private static String[] dayNames =
    {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};

  private static String printDay( int dayOfYear, int year ) {
    final Calendar c = getInstance();
    c.set( DAY_OF_YEAR, dayOfYear );

    final String day = format
      ( "%s-%s-%s (%s)",
        c.get( DAY_OF_MONTH ),
        monthNames[c.get( MONTH )],
        year,
        dayNames[c.get( DAY_OF_WEEK ) - 1] );

    return day;
  }

  private static String normalizeName( String s ) {
    if( s == null ) {
      throw new NullPointerException();
    }

    s = s.trim();
    s = s.toLowerCase();
    s = s.replaceAll( " ", "_" );
    return s;
  }
}
