package it.univr.auditel;

import it.univr.auditel.entities.GroupView;
import it.univr.auditel.entities.ViewRecord;
import it.univr.auditel.shadoop.core.ViewSequenceValue;
import it.univr.auditel.shadoop.core.ViewSequenceWritable;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static it.univr.auditel.ProcessAuditelRowData.*;
import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class ProcessAuditelResults {

  private static final String mainDirPath =
    "C:\\workspace\\projects\\veronacard_analysis\\test_auditel\\output";

  private static final String outFile = "results.csv";

  public static void main( String[] args ) throws FileNotFoundException, ParseException {

    // ===========================================================================

    final List<String> hlines =
      readLines( new File( format( "%s\\%s", directory, logFile ) ), true );
    System.out.printf( "Number of individual views: %d.%n", hlines.size() );

    final List<ViewRecord> records = discardShortViews( hlines );
    System.out.printf( "Number of cleaned individual views: %d.%n", records.size() );

    final Map<ProcessAuditelRowData.Key, List<ViewRecord>> candidateGroupViews = candidateGroupViews( records );
    System.out.printf( "Number of candidate group views: %d.%n", candidateGroupViews.size() );

    final Map<String, String> userAges = readUserAges();
    System.out.printf( "Number of user ages read: %d.%n", userAges.size() );

    final Map<Integer, GroupView> views = refineGroupViews( candidateGroupViews, userAges );
    System.out.printf( "Number of refined group views: %d.%n", views.size() );

    final Collection<ViewSequenceWritable> sequences = groupViewSequences( views.values() );
    System.out.printf( "Number of query sequences: %d.%n", sequences.size() );

    final List<ViewSequenceWritable> querySequences = new ArrayList<>();
    final Random generator = new Random( 3948382918L );
    for( ViewSequenceWritable s : sequences ) {
      if( s.size() > 1 ) {
        final int p = generator.nextInt( 3 );
        if( p > 2 ) {
          querySequences.add( s );
        }
      }
    }

    // ===========================================================================

    final StringBuilder b = new StringBuilder();
    b.append( format
                ( "index, "
                  + "duration,"
                  + "numViews,"
                  + "history,"
                  + "missedProgramSeconds,"
                  + "%%RSD missedProgramSeconds,"
                  + "preferences,"
                  + "%%RSD preferences%n" ) );

    final File mainDir = new File( mainDirPath );
    if( mainDir.isDirectory() ) {
      final File[] outDirs = mainDir.listFiles();
      for( File od : outDirs ) {
        final String index = od.getName();
        if( od.isDirectory() ) {
          final File[] singleOutDirs = od.listFiles( new FilenameFilter() {
            @Override
            public boolean accept( File dir, String name ) {
              if( name.endsWith( "_trsa" ) ) {
                return true;
              } else {
                return false;
              }
            }
          } );

          for( File sod : singleOutDirs ) {
            final File[] out = sod.listFiles( new FilenameFilter() {
              @Override
              public boolean accept( File dir, String name ) {
                if( name.startsWith( "part-r" ) ) {
                  return true;
                } else {
                  return false;
                }
              }
            } );

            if( out.length == 0 ) {
              System.out.printf( "No file founds in \"%s\".%n", sod );
            } else if( out.length > 1 ) {
              System.out.printf( "Too many files in \"%s\".%n", sod );
            } else {
              final File resFile = out[0];
              System.out.printf( "%s%n", resFile.getName() );

              final List<String> lines = readLines( resFile, false );
              System.out.printf( "Number of lines %d.%n", lines.size() );

              for( String l : lines ) {
                final ViewSequenceValue v =
                  it.univr.auditel.shadoop.core.FileReader
                    .processViewSequenceValue( l );

                b.append( index );
                b.append( "," );
                b.append( v.getDuration() );
                b.append( "," );
                b.append( v.getChannelSequence().size() );
                b.append( "," );

                b.append( 0 ); // history!
                b.append( "," );

                int m = 0;
                final Collection<Double> tmp = new ArrayList<>( v.getMissedProgramSeconds().size() );
                for( Map.Entry<String, Integer> e : v.getMissedProgramSeconds().entrySet() ) {
                  m += e.getValue();
                  tmp.add( Double.parseDouble( e.getValue().toString() ) );
                }
                double mavg = v.getMissedProgramSeconds().size() == 0 ? 0 : average( tmp );
                double mstd = v.getMissedProgramSeconds().size() == 0 ? 0: standardDeviation( tmp );
                b.append( m );
                b.append( "," );
                b.append( ( mavg != 0 && mstd != 0 ) ? mstd / mavg : 0 );
                b.append( "," );

                double tp = 0.0;
                double tavg = v.getUserPreferences().size() == 0 ? 0 : average( v.getUserPreferences().values() );
                double tsdt = v.getUserPreferences().size() == 0 ? 0 : standardDeviation( v.getUserPreferences().values() );
                for( Map.Entry<String, Double> p : v.getUserPreferences().entrySet() ) {
                  tp += p.getValue();
                }
                b.append( tp );
                b.append( "," );
                b.append( (tsdt != 0 && tavg != 0 ) ? tsdt / tavg : 0 );
                b.append( format( "%n" ) );
              }
            }
          }
        }
      }
    }
    writeFile( mainDirPath, outFile, b.toString() );
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param input
   * @return
   */

  public static double standardDeviation( Collection<Double> input ) {
    double standardDeviation = 0.0;
    int length = input.size();

    double mean = average( input );

    for( double num : input ) {
      standardDeviation += Math.pow( num - mean, 2 );
    }

    return Math.sqrt( standardDeviation / length );
  }

  /**
   * MISSING_COMMENT
   *
   * @param input
   * @return
   */

  public static double average( Collection<Double> input ){
    double sum = 0.0;
    int length = input.size();

    for( double num : input ) {
      sum += num;
    }
    return sum / length;
  }


  // ===========================================================================

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
   * @param dir
   * @param filename
   * @param content
   * @throws FileNotFoundException
   */

  private static void writeFile( String dir, String filename, String content )
    throws FileNotFoundException {

    if( dir == null ) {
      throw new NullPointerException();
    }

    if( filename == null ) {
      throw new NullPointerException();
    }

    final String path = format( "%s\\%s", dir, filename );
    final PrintWriter indexWriter = new PrintWriter( path );
    indexWriter.write( content );
    indexWriter.close();
    System.out.printf( "File written in \"%s\".%n", path );
  }
}
