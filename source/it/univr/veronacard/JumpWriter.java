package it.univr.veronacard;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jump.feature.BasicFeature;
import com.vividsolutions.jump.feature.Feature;
import com.vividsolutions.jump.feature.FeatureCollection;
import com.vividsolutions.jump.feature.FeatureDataset;
import com.vividsolutions.jump.feature.FeatureSchema;
import com.vividsolutions.jump.io.DriverProperties;
import com.vividsolutions.jump.io.JMLWriter;
import it.univr.veronacard.services.ServiceException;
import it.univr.veronacard.services.VcDataService;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import static com.vividsolutions.jump.feature.AttributeType.*;
import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class JumpWriter {

  final static String path = "test/%s";
  final static String directory = "test05_arena_statistics";

  public static void main( String[] args ) throws ServiceException {
    final File output = new File( format( path, directory ) );

    if( output.isDirectory() ) {
      final File[] files = output.listFiles();

      final Map<String, List<Trip>> geometries = new HashMap<>();

      for( File f : files ) {
        if( f.isFile() && f.getName().startsWith( "part-r-" ) ) {
          try( BufferedReader br = new BufferedReader( new FileReader( f ) ) ) {
            String line;
            while( ( line = br.readLine() ) != null ) {
              // process the line.
              processLine( line, geometries );
            }
          } catch( IOException e ) {
            System.out.printf( "Reading of file \"%s\" failed.", f.getName() );
            System.exit( 1 );
          }
        }
      }

      try {
        final PrintWriter writer = new PrintWriter
          ( format( "%s/%s",
                    format( path, directory ),
                    "arena_statistics_overlaps.csv" ),
            "UTF-8" );
        writer.println( "\"Serial\", \"ov_orig\", \"ov_pert\", \"scenic_routes\"" );
        /*writer.println( "\"Serial\","
                        + "\"NumSteps\","
                        + "\"Duration (sec)\","
                        + "\"TravelTime (sec)\","
                        + "\"WaitingTime (sec)\","
                        + "\"Distance (meters)\","
                        + "\"NumScenicRoutes\","
                        + "\"Overlaps\"" );//*/

        final VcDataService ds = new VcDataService();

        for( Map.Entry<String, List<Trip>> l : geometries.entrySet() ) {
          if( l.getValue() != null && l.getValue().size() > 0 ) {
            for( int i = 0; i < l.getValue().size(); i++ ) {

              final Trip t = l.getValue().get( i );
              final MultiLineString tgp = (MultiLineString) t.getGeometry();
              final double tArea = tgp.buffer( 0.01 ).getArea();

              double tPathArea = 0;
              for( int g = 0; g < tgp.getNumGeometries(); g++ ) {
                final LineString ls = (LineString) tgp.getGeometryN( g );
                tPathArea += ls.buffer( 0.01 ).getArea();
              }

              final double tValue = tArea / tPathArea;

              final boolean scenicRoutes =
                t.getSteps().contains( "PB" ) ||
                t.getSteps().contains( "PE" ) ||
                t.getSteps().contains( "PTB" );


              /*writer.println( String.format
                ( "\"%s\",%d,%.2f,%.2f,%.2f,%.2f,%d,%d",
                  t.getId(),
                  t.getNumSteps(),
                  t.getDuration(),
                  t.getTravelTime(),
                  t.getWaitingTime(),
                  t.getDistance(),
                  t.getNumScenicRoutes(), 0 ) );//*/

              //final List<String> origSites = ds.getVcTickets( t.getId() );
              //int origWaiting = 0;
              /*for( int s = 0; s < origSites.size(); s++ ) {
                final String site = origSites.get( s );
                final int index = s % 2;
                origWaiting += VcSiteInfo.waitingTime[index][new Integer( site ) - 1];
              }

              writer.println( String.format( "\"%s\", %d, %.2f", t.getId(), origWaiting, t.getWaitingTime() ) );//*/


              final Geometry go = ds.getVcGeometry( t.getId() );
              if( go == null ){
                System.out.println( "skipped vc " + t.getId() );
                continue;
              }

              final MultiLineString gOrig = (MultiLineString) go;

              final double oArea = gOrig.buffer( 0.01 ).getArea();

              double oPathArea = 0;
              for( int g = 0; g < gOrig.getNumGeometries(); g++ ) {
                final LineString ls = (LineString) gOrig.getGeometryN( g );
                oPathArea += ls.buffer( 0.01 ).getArea();
              }

              final double oValue = oArea / oPathArea;
              //*/

              //final double oValue = 0.0;

              writer.println
                ( String.format( "\"%s\", %.6f, %.6f, %d",
                                 t.getId(), oValue, tValue,
                                 scenicRoutes ? 1 : 0 ) );//*/
            }
          }
        }
        writer.close();
      } catch( IOException e ) {
        // do something
      }


      /*for( Map.Entry<String, List<Trip>> l : geometries.entrySet() ) {
        if( l.getValue() != null && l.getValue().size() > 0 ) {
          for( int i = 0; i < l.getValue().size(); i++ ) {
            writeJmlFile
              ( format( path, directory ),
                format( "trip_orig_%s_rec_%s", l.getKey(), i ),
                l.getValue().subList( i, i + 1 ) );
          }
        }
      }//*/
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param line
   * @param geometries
   * @return
   */
  public static void processLine
  ( String line,
    Map<String, List<Trip>> geometries ) {

    if( line == null ) {
      throw new NullPointerException();
    }
    if( geometries == null ) {
      throw new NullPointerException();
    }

    final WKTReader reader = new WKTReader();
    final GeometryFactory factory = new GeometryFactory();

    final StringTokenizer tk = new StringTokenizer( line, "\t" );
    int currTok = 0;
    final Trip currTrip = new Trip();

    while( tk.hasMoreTokens() ) {
      final String curr = tk.nextToken();
      if( currTok == 1 ) {
        currTrip.setId( curr );
      }
      if( currTok == 2 ) {
        currTrip.setSteps( curr );
      }
      if( currTok == 3 ) {
        try {
          final StringTokenizer gtk = new StringTokenizer( curr, "-" );
          final List<LineString> ls = new ArrayList<>();
          while( gtk.hasMoreTokens() ) {
            ls.add( (LineString) reader.read( gtk.nextToken() ) );
          }

          final MultiLineString mls = new MultiLineString
            ( ls.toArray( new LineString[ls.size()] ), factory );

          currTrip.setGeometry( mls );
        } catch( ParseException e ) {
        }
      }
      if( currTok == 7 ) {
        currTrip.setNumSteps( Integer.parseInt( curr ) );
      }
      if( currTok == 8 ) {
        currTrip.setDuration( Double.parseDouble( curr ) );
      }
      if( currTok == 9 ) {
        currTrip.setTravelTime( Double.parseDouble( curr ) );
      }
      if( currTok == 10 ) {
        currTrip.setWaitingTime( Double.parseDouble( curr ) );
      }
      if( currTok == 11 ) {
        currTrip.setDistance( Double.parseDouble( curr ) );
      }
      if( currTok == 12 ) {
        currTrip.setNumScenicRoutes( Integer.parseInt( curr ) );
      }
      currTok++;
    }

    if( currTrip.getId() != null &&
        currTrip.getGeometry() != null &&
        currTrip.getSteps() != null ) {

      List<Trip> glist = geometries.get( currTrip.getId() );
      if( glist == null ) {
        glist = new ArrayList<>();
      }
      glist.add( currTrip );
      geometries.put( currTrip.getId(), glist );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param path
   * @param filename
   * @param geometries
   */

  private static void writeJmlFile
  ( String path,
    String filename,
    List<Trip> geometries ) {

    if( path == null ) {
      throw new NullPointerException();
    }
    if( filename == null ) {
      throw new NullPointerException();
    }
    if( geometries == null ) {
      throw new NullPointerException();
    }

    final String file = format( "%s/%s.jml", path, filename );

    final JMLWriter writer = new JMLWriter();
    final DriverProperties properties = new DriverProperties();
    //properties.set( "DefaultValue", filename );
    properties.set( "File", file );

    final FeatureCollection collection = generateFeatureCollection( geometries );
    try {
      writer.write( collection, properties );
    } catch( Exception e ) {
      System.out.printf( "Problem during the writing of: \"%s\".%n", file );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param geometries
   * @return
   */

  private static FeatureCollection generateFeatureCollection
  ( List<Trip> geometries ) {

    final FeatureSchema schema = new FeatureSchema();
    schema.addAttribute( "id", STRING );
    schema.addAttribute( "geometry", GEOMETRY );
    schema.addAttribute( "steps", STRING );
    schema.addAttribute( "num_steps", INTEGER );
    schema.addAttribute( "duration", DOUBLE );
    schema.addAttribute( "travel_time", DOUBLE );
    schema.addAttribute( "waiting_time", DOUBLE );
    schema.addAttribute( "distance", DOUBLE );
    schema.addAttribute( "num_scenic_routes", DOUBLE );
    final FeatureCollection collection = new FeatureDataset( schema );

    for( int i = 0; i < geometries.size(); i++ ) {
      final Feature f = new BasicFeature( schema );
      final Trip t = geometries.get( i );
      f.setAttribute( "id", t.getId() );
      f.setAttribute( "geometry", t.getGeometry() );
      f.setAttribute( "steps", t.getSteps() );
      f.setAttribute( "num_steps", t.getNumSteps() );
      f.setAttribute( "duration", t.getDuration() );
      f.setAttribute( "travel_time", t.getTravelTime() );
      f.setAttribute( "waiting_time", t.getWaitingTime() );
      f.setAttribute( "distance", t.getDistance() );
      f.setAttribute( "num_scenic_routes", t.getNumScenicRoutes() );
      collection.add( f );
    }

    return collection;
  }

  // ===========================================================================

  static class Trip {
    private String id;
    private Geometry geometry;
    private String paths;
    private Integer numSteps;
    private Double duration;
    private Double travelTime;
    private Double waitingTime;
    private Double distance;
    private Integer numScenicRoutes;

    private Trip() {
      this.id = null;
      this.geometry = null;
      this.paths = null;
      this.numSteps = null;
      this.duration = null;
      this.travelTime = null;
      this.waitingTime = null;
      this.distance = null;
      this.numScenicRoutes = null;
    }

    public String getId() {
      return id;
    }

    public void setId( String id ) {
      this.id = id;
    }

    public Geometry getGeometry() {
      return geometry;
    }

    public void setGeometry( Geometry geometry ) {
      this.geometry = geometry;
    }

    public String getSteps() {
      return paths;
    }

    public void setSteps( String paths ) {
      this.paths = paths;
    }

    public String getPaths() {
      return paths;
    }

    public void setPaths( String paths ) {
      this.paths = paths;
    }

    public Integer getNumSteps() {
      return numSteps;
    }

    public void setNumSteps( Integer numSteps ) {
      this.numSteps = numSteps;
    }

    public Double getDuration() {
      return duration;
    }

    public void setDuration( Double duration ) {
      this.duration = duration;
    }

    public Double getTravelTime() {
      return travelTime;
    }

    public void setTravelTime( Double travelTime ) {
      this.travelTime = travelTime;
    }

    public Double getWaitingTime() {
      return waitingTime;
    }

    public void setWaitingTime( Double waitingTime ) {
      this.waitingTime = waitingTime;
    }

    public Double getDistance() {
      return distance;
    }

    public void setDistance( Double distance ) {
      this.distance = distance;
    }

    public Integer getNumScenicRoutes() {
      return numScenicRoutes;
    }

    public void setNumScenicRoutes( Integer numScenicRoutes ) {
      this.numScenicRoutes = numScenicRoutes;
    }
  }
}

