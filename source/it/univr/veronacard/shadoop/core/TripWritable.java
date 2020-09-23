package it.univr.veronacard.shadoop.core;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import static it.univr.veronacard.entities.ScenicRoutes.isScenicSite;
import static it.univr.veronacard.entities.VcSiteInfo.defaultVisitingTime;
import static java.lang.Math.ceil;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.copyOfRange;

//import static it.univr.veronacard.entities.VcSiteInfo.stayTime;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class TripWritable implements Writable {

  // === Attributes ============================================================

  private static final String emptyString = "null";

  private static enum TripAttribute {
    VC_SERIAL,
    STEP_LABELS,
    PATHS,
    DURATION,
    DISTANCE,
    TRAVEL_MODE,
    PROFILE,
    MONTH,
    DAY_OF_WEEK,
    HOUR,
    MINUTES,
    DAY_OF_YEAR,
    YEAR;
  }

  // === Properties ============================================================

  private String vcSerial;
  private List<String> sites;
  private String travelMode;
  private String profile;
  private Integer month;
  private Integer dayOfWeek;
  private Integer startHour;
  private Integer startMinutes;
  private Integer dayOfYear;
  private Integer year;
  private Integer visitingTime;

  private List<Integer> siteTimes;

  private List<Step> steps;
  //private Map<String,Map<String, Integer>> stayTimes;

  // === Methods ===============================================================

  public TripWritable() {
    this.vcSerial = null;
    this.sites = new ArrayList<>();
    this.steps = new ArrayList<>();
    this.travelMode = null;
    this.profile = null;
    this.month = null;
    this.dayOfWeek = null;
    this.startHour = null;
    this.startMinutes = null;
    this.dayOfYear = null;
    this.year = null;
    this.visitingTime = null;
    this.siteTimes = new ArrayList<>();
  }

  public TripWritable( TripWritable trip ) {
    this.vcSerial = trip.vcSerial;
    this.sites = new ArrayList<>( trip.sites );
    this.steps = new ArrayList<>( trip.steps );
    this.travelMode = trip.travelMode;
    this.profile = trip.profile;
    this.month = trip.month;
    this.dayOfWeek = trip.dayOfWeek;
    this.dayOfYear = trip.dayOfYear;
    this.year = trip.year;
    this.startHour = trip.startHour;
    this.startMinutes = trip.startMinutes;
    this.visitingTime = trip.visitingTime;
    this.siteTimes = new ArrayList<>( trip.siteTimes );
  }

  // ===========================================================================

  public String getVcSerial() {
    return vcSerial;
  }

  public void setVcSerial( String vcSerial ) {
    this.vcSerial = vcSerial;
  }

  public List<String> getSites() {
    return sites;
  }

  public void setSites( List<String> sites ) {
    this.sites = sites;
  }

  public void addSite( int position, String site ) {
    if( this.sites == null ) {
      this.sites = new ArrayList<>();
    }
    if( position < 0 || position > this.sites.size() ) {
      throw new IllegalStateException
        ( format( "Position %d is not available, site size = %d.",
                  position, this.sites.size() ) );
    }

    this.sites.add( position, site );
    this.steps.clear();
  }

  public void removeSite( int position ) {
    if( this.sites == null ) {
      throw new IllegalStateException
        ( format( "Position %d is not available, site list is null.",
                  position ) );
    }
    if( position < 0 || position >= this.sites.size() ) {
      throw new IllegalStateException
        ( format( "Position %d is not available, site size = %d.",
                  position, this.sites.size() ) );
    }
    this.sites.remove( position );
    this.steps.clear();
  }

  public int getNumSites() {
    if( this.sites == null ) {
      return 0;
    } else {
      return this.sites.size();
    }
  }

  public String getTravelMode() {
    return travelMode;
  }

  public void setTravelMode( String travelMode ) {
    this.travelMode = travelMode;
  }

  public String getProfile() {
    return profile;
  }

  public void setProfile( String profile ) {
    this.profile = profile;
  }

  public Integer getMonth() {
    return month;
  }

  public void setMonth( Integer month ) {
    this.month = month;
  }

  public Integer getDayOfWeek() {
    return dayOfWeek;
  }

  public void setDayOfWeek( Integer dayOfWeek ) {
    this.dayOfWeek = dayOfWeek;
  }

  public Integer getStartHour() {
    return startHour;
  }

  public void setStartHour( Integer startHour ) {
    this.startHour = startHour;
  }

  public Integer getStartMinutes() {
    return startMinutes;
  }

  public void setStartMinutes( Integer startMinutes ) {
    this.startMinutes = startMinutes;
  }

  public Integer getDayOfYear() {
    return dayOfYear;
  }

  public void setDayOfYear( Integer dayOfYear ) {
    this.dayOfYear = dayOfYear;
  }

  public Integer getYear() {
    return year;
  }

  public void setYear( Integer year ) {
    this.year = year;
  }

  public Integer getVisitingTime() {
    if( visitingTime == null ) {
      return 0;
    } else {
      return visitingTime;
    }
  }

  public void setVisitingTime( Integer visitingTime ) {
    this.visitingTime = visitingTime;
  }

  // ===========================================================================

  /**
   * First level key = travelMode Second level key = origin site Value =
   * possible steps from origin site.
   *
   * @param stepMap
   */

  public void buildStepsFromCompleteMap
  ( Map<String, Map<String, List<Step>>> stepMap ) {

    if( stepMap == null ) {
      throw new NullPointerException();
    }
    final Map<String, List<Step>> availableSteps = stepMap.get( travelMode );
    buildSteps( availableSteps );
  }


  /**
   * key =  origin site value = possible steps form origin site.
   *
   * @param availableSteps
   */

  public void buildSteps( Map<String, List<Step>> availableSteps ) {
    if( sites == null || sites.size() == 0 ) {
      this.steps = Collections.emptyList();

    } else {
      if( availableSteps == null ) {
        throw new IllegalStateException
          ( format( "Available steps not found for travel mode \"%s\".%n",
                    travelMode ) );
      }

      // clear old steps
      steps.clear();

      for( int i = 0; i < sites.size() - 1; i++ ) {
        final String origin = sites.get( i );
        final String destination = sites.get( i + 1 );

        final List<Step> possibleSteps = availableSteps.get( origin );
        if( possibleSteps == null ) {
          throw new IllegalStateException
            ( format( "Possible steps not found from site \"%s\".%n",
                      origin ) );
        }

        final Iterator<Step> it = possibleSteps.iterator();
        boolean found = false;
        while( it.hasNext() && !found ) {
          final Step s = it.next();
          if( s.getDestination().equals( destination ) ) {
            steps.add( s );
            found = true;
          }
        }
        if( !found ) {
          throw new IllegalStateException
            ( format( "Step not found from site \"%s\" to site \"%s\".%n",
                      origin, destination ) );
        }
      }
    }
  }


  /**
   * The method returns the time required to visiting the sites contained in the
   * current trip.
   *
   * numVisits: 1st level => key = site 2nd level => key =
   * "month_dayOfWeek_hour", value = num_visits
   *
   * stayTimes: 1st level => key = site 2nd level => key = numVisits, value =
   * stay_time
   *
   * eto: 1st level => key = site 2nd level => key = hour, value = num_visits
   *
   * @param numVisits
   * @param stayTimes
   * @param eto
   * @param historicalPercentage
   * @param deltaPerVisitor
   */

  public void computeVisitingTime
  ( Map<String, Map<String, Integer>> numVisits,
    Map<String, Map<Integer, Integer>> stayTimes,
    Map<String, Map<Integer, Integer>> eto,
    Double historicalPercentage,
    Integer deltaPerVisitor ) {

    if( stayTimes == null ) {
      throw new NullPointerException();
    }

    if( steps != null ) {
      int visitingTime = 0;

      siteTimes = new ArrayList<>( sites.size() );

      final Iterator<Step> it = steps.iterator();
      while( it.hasNext() ) {
        final Step s = it.next();

        if( isScenicSite( s.getOrigin() ) &&
            isScenicSite( s.getDestination() ) ) {
          visitingTime += s.getDuration();
        } else {
          if( !isScenicSite( s.getOrigin() ) ) {
            final Integer hour =
              (int) ceil( startHour + ( startMinutes + visitingTime ) / 60 );

            final Integer stayTime =
              getSiteVisitingTime
                ( s.getOrigin(),
                  numVisits, stayTimes, eto,
                  month, dayOfWeek, hour,
                  historicalPercentage,
                  deltaPerVisitor );

            visitingTime += s.getDuration() + stayTime;

            siteTimes.add( hour );
          }
          if( !it.hasNext() && !isScenicSite( s.getDestination() ) ) {
            final Integer hour =
              (int) ceil( startHour + ( startMinutes + visitingTime ) / 60 );

            final Integer stayTime =
              getSiteVisitingTime
                ( s.getDestination(),
                  numVisits, stayTimes, eto,
                  month, dayOfWeek, hour,
                  historicalPercentage,
                  deltaPerVisitor );

            visitingTime += stayTime;

            siteTimes.add( hour );
          }
        }
      }
      this.visitingTime = visitingTime;
    } else {
      this.visitingTime = 0;
    }
  }


  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @return
   */

  public List<Step> getSteps() {
    return steps;
  }

  /**
   * MISSING_COMMENT
   *
   * @return
   */

  public String getSource() {
    if( this.sites == null || this.sites.isEmpty() ) {
      return emptyString;
    } else {
      return this.sites.get( 0 );
    }
  }


  /**
   * The method returns the travel time required to perform the current trip.
   *
   * @return
   */

  public Integer getTravelTime() {
    if( steps == null ) {
      return 0;
    } else {
      int travelTime = 0;
      for( Step s : steps ) {
        if( !isScenicSite( s.getOrigin() ) ||
            !isScenicSite( s.getDestination() ) ) {
          travelTime += s.getDuration();
        }
      }
      return travelTime;
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param site
   * @param historicalNumVisits
   * @param historicalStayTimes
   * @param eto
   * @param historicalPercentage
   * @param deltaPerVisitor
   * @return
   */

  public int getSiteArrivingHour
  ( String site,
    Map<String, Map<String, Integer>> historicalNumVisits,
    Map<String, Map<Integer, Integer>> historicalStayTimes,
    Map<String, Map<Integer, Integer>> eto,
    Double historicalPercentage,
    Integer deltaPerVisitor ) {

    if( historicalNumVisits == null ) {
      throw new NullPointerException();
    }
    if( historicalStayTimes == null ) {
      throw new NullPointerException();
    }
    if( eto == null ) {
      throw new NullPointerException();
    }

    if( !sites.contains( site ) ) {
      return -1;
    }

    int result = startMinutes;

    boolean found = false;
    for( int i = 0; i < steps.size() && !found; i++ ) {
      final Step s = steps.get( i );
      int hour = (int) ceil( startHour + result / 60.0 );

      result += getSiteVisitingTime
        ( s.getOrigin(),
          historicalNumVisits,
          historicalStayTimes,
          eto,
          month,
          dayOfWeek,
          hour,
          historicalPercentage,
          deltaPerVisitor );

      if( s.getOrigin().equals( site ) ) {
        found = true;
      }

      if( !found ) {
        if( !isScenicSite( s.getOrigin() ) &&
            !isScenicSite( s.getDestination() ) ) {
          result += s.getDuration();
        }

        if( i == steps.size() - 1 ) {
          result += getSiteVisitingTime
            ( s.getDestination(),
              historicalNumVisits,
              historicalStayTimes,
              eto,
              month,
              dayOfWeek,
              hour,
              historicalPercentage,
              deltaPerVisitor );

        }
      }
    }

    return (int) ceil( startHour + result / 60.0 );
  }


  /**
   * Visiting time in minutes!!
   *
   * @param site
   * @param historicalNumVisits
   * @param historicalStayTimes
   * @param eto
   * @param month
   * @param dow
   * @param hour
   * @param historyPercentage
   * @param deltaPerVisitor
   * @return
   */

  private int getSiteVisitingTime
  ( String site,
    Map<String, Map<String, Integer>> historicalNumVisits,
    Map<String, Map<Integer, Integer>> historicalStayTimes,
    Map<String, Map<Integer, Integer>> eto,
    Integer month,
    Integer dow,
    Integer hour,
    Double historyPercentage,
    Integer deltaPerVisitor ) {

    final Map<String, Integer> visitMap = historicalNumVisits.get( site );
    final Map<Integer, Integer> timeMap = historicalStayTimes.get( site );
    final Map<Integer, Integer> dynamic = eto.get( site );

    if( visitMap != null && !visitMap.isEmpty() &&
        timeMap != null && !timeMap.isEmpty() ) {

      final String key = format( "%s_%s_%s", month, dow, hour );

      final Integer hvisits = visitMap.get( key ) != null ?
        visitMap.get( key ) : 0;
      final Integer dvisits = dynamic != null && dynamic.get( hour ) != null ?
        dynamic.get( hour ) : 0;

      Integer visits = (int) ceil( hvisits * historyPercentage ) + dvisits;
      final Integer time = timeMap.get( visits );

      // fax fix: additional delta depending on number of visits
      final Integer delta = visits * deltaPerVisitor;

      if( time != null ) {
        return time + delta;

      } else {
        final List<Integer> availableNumVisits = new ArrayList<>( timeMap.keySet() );
        Collections.sort( availableNumVisits );

        int result = defaultVisitingTime;

        for( int i = 0; i < availableNumVisits.size() && i <= visits; i++ ) {
          if( timeMap.get( i ) != null ) {
            result = timeMap.get( i );
          }
        }
        return result + delta;
      }
    }
    return defaultVisitingTime;
  }


  /**
   * The method returns the time spent by waiting before visit a site.
   *
   * @return
   */

  /*public Integer getWaitingTime() {
    if( sites == null ) {
      return 0;
    }

    int waitingTime = 0;
    for( int i = 0; i < sites.size(); i++ ) {
      final int rowIndex;
      if( i % 2 == 0 ) {
        rowIndex = 0;
      } else {
        rowIndex = 1;
      }

      final int columnIndex;
      try {
        columnIndex = Integer.parseInt( sites.get( i ) ) - 1;
      } catch( NumberFormatException e ) {
        // isScenicSite()
        continue;
      }

      waitingTime += VcSiteInfo.waitingTime[rowIndex][columnIndex];
    }

    return waitingTime;
  }//*/

  /**
   * The method returns the total duration of the trip.
   *
   * @return
   */

  public Integer getDuration() {
    return getTravelTime() + getVisitingTime();
    //+ getWaitingTime();
  }


  /**
   * MISSING_COMMENT
   *
   * @return
   */

  public Integer getDistance() {
    if( steps == null ) {
      return 0;
    } else {
      int distance = 0;
      for( Step s : steps ) {
        distance += s.getDistance();
      }
      return distance;
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @return
   */

  public List<Geometry> getGeometries() {
    if( steps == null ) {
      return Collections.emptyList();
    } else {
      final List<Geometry> gl = new ArrayList<>();
      for( Step s : steps ) {
        gl.add( s.getPath() );
      }
      return gl;
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @return
   */

  public int getNumScenicRoutes() {
    if( sites == null ) {
      return 0;
    } else {
      int numScenicRoutes = 0;
      for( int i = 0; i < this.sites.size() - 1; i++ ) {
        final String orig = this.sites.get( i );
        final String dest = this.sites.get( i + 1 );
        if( isScenicSite( orig ) && isScenicSite( dest ) ) {
          numScenicRoutes++;
        }
      }
      return numScenicRoutes;
    }
  }


  // ===========================================================================

  /**
   * Serialize the fields of this object to dataOutput.
   *
   * @param dataOutput
   * @throws IOException
   */

  @Override
  public void write( DataOutput dataOutput ) throws IOException {
    if( dataOutput == null ) {
      throw new NullPointerException();
    }

    // serial
    if( vcSerial != null ) {
      dataOutput.writeUTF( vcSerial );
    } else {
      dataOutput.writeUTF( emptyString );
    }

    // sites
    if( sites != null ) {
      // write the number of sites
      dataOutput.writeInt( sites.size() );
      for( String label : sites ) {
        dataOutput.writeUTF( label );
      }
    } else {
      dataOutput.writeInt( 0 );
    }

    // steps
    if( steps != null ) {
      dataOutput.writeInt( steps.size() );
      for( Step s : steps ) {
        dataOutput.writeUTF( s.getOrigin() );
        dataOutput.writeUTF( s.getDestination() );
        dataOutput.writeInt( s.getDuration() );
        dataOutput.writeInt( s.getDistance() );
        dataOutput.writeUTF( s.getTravelMode() );
        dataOutput.writeUTF( s.getPath().toText() );
      }
    } else {
      dataOutput.writeInt( 0 );
    }

    // travel mode
    if( travelMode != null ) {
      dataOutput.writeUTF( travelMode );
    } else {
      dataOutput.writeUTF( emptyString );
    }

    // profile
    if( profile != null ) {
      dataOutput.writeUTF( profile );
    } else {
      dataOutput.writeUTF( emptyString );
    }

    // month
    if( month != null ) {
      dataOutput.writeInt( month );
    } else {
      dataOutput.writeInt( -1 );
    }

    // day of week
    if( dayOfWeek != null ) {
      dataOutput.writeInt( dayOfWeek );
    } else {
      dataOutput.writeInt( -1 );
    }

    // start hour
    if( startHour != null ) {
      dataOutput.writeInt( startHour );
    } else {
      dataOutput.writeInt( -1 );
    }

    // start minutes
    if( startMinutes != null ) {
      dataOutput.writeInt( startMinutes );
    } else {
      dataOutput.writeInt( 0 );
    }

    // day of year
    if( dayOfYear != null ) {
      dataOutput.writeInt( dayOfYear );
    } else {
      dataOutput.writeInt( -1 );
    }

    // year
    if( year != null ) {
      dataOutput.writeInt( year );
    } else {
      dataOutput.writeInt( -1 );
    }

    // site times
    if( siteTimes != null ) {
      dataOutput.writeInt( siteTimes.size() );
      for( Integer t : siteTimes ) {
        dataOutput.writeInt( t );
      }
    } else {
      dataOutput.writeInt( 0 );
    }
  }


  /**
   * Deserialize the fields of this object from dataInput.
   *
   * @param dataInput
   * @throws IOException
   */

  @Override
  public void readFields( DataInput dataInput ) throws IOException {
    if( dataInput == null ) {
      throw new NullPointerException();
    }

    // serial
    final String sString = dataInput.readUTF();
    if( !sString.equals( emptyString ) ) {
      vcSerial = sString;
    } else {
      vcSerial = null;
    }

    // sites
    final int numSites = dataInput.readInt();
    if( numSites > 0 ) {
      sites = new ArrayList<>( numSites );
      for( int i = 0; i < numSites; i++ ) {
        sites.add( dataInput.readUTF() );
      }
    } else {
      sites = null;
    }

    // steps
    final int numSteps = dataInput.readInt();
    if( numSites > 0 ) {
      steps = new ArrayList<>( numSteps );
      for( int i = 0; i < numSteps; i++ ) {
        final Step step = new Step();
        step.setOrigin( dataInput.readUTF() );
        step.setDestination( dataInput.readUTF() );
        step.setDuration( dataInput.readInt() );
        step.setDistance( dataInput.readInt() );
        step.setTravelMode( dataInput.readUTF() );
        final WKTReader reader = new WKTReader();
        try {
          step.setPath( reader.read( dataInput.readUTF() ) );
        } catch( ParseException e ) {
          // nothing here
        }
        steps.add( step );
      }
    } else {
      steps = null;
    }

    // travel mode
    final String tmString = dataInput.readUTF();
    if( !tmString.equals( emptyString ) ) {
      travelMode = tmString;
    } else {
      travelMode = null;
    }

    // profile
    final String prof = dataInput.readUTF();
    if( !prof.equals( emptyString ) ) {
      profile = prof;
    } else {
      profile = null;
    }

    // month
    final int month = dataInput.readInt();
    if( month != -1 ) {
      this.month = month;
    } else {
      this.month = null;
    }

    // day of week
    final int dow = dataInput.readInt();
    if( dow != -1 ) {
      this.dayOfWeek = dow;
    } else {
      this.dayOfWeek = null;
    }

    // hour
    final int hour = dataInput.readInt();
    if( hour != -1 ) {
      this.startHour = hour;
    } else {
      this.startHour = null;
    }

    // minutes
    final int minutes = dataInput.readInt();
    this.startMinutes = minutes;

    // day of year
    final int doy = dataInput.readInt();
    if( doy != -1 ) {
      this.dayOfYear = doy;
    } else {
      this.dayOfYear = null;
    }

    // year
    final int year = dataInput.readInt();
    if( year != -1 ) {
      this.year = year;
    } else {
      this.year = null;
    }

    // site times
    final int numTimes = dataInput.readInt();
    this.siteTimes = new ArrayList<>( numTimes );
    for( int i = 0; i < numTimes; i++ ) {
      this.siteTimes.add( dataInput.readInt() );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param text
   */

  public void fromText( Text text ) {
    // reset the current parameters
    vcSerial = null;
    sites.clear();
    travelMode = null;
    profile = null;
    steps.clear();

    /*
    The underlying byte array used by Text to store the data is never cleaned
    up: for performance reasons, Text uses a length attribute to keep track of
    the length of the current data.

    All the methods that allow you to get data (find, toString and so on) will
    look at this length attribute and only consider the data contained in the 0
    until length interval.... Actually, all the methods except getBytes, which
    will return you the complete underlying byte array, without considering the
    actual length!

    An alternative is to use text.copyBytes(), which will return you a slice of
    the byte array, cut down according to length. Earlier versions of Text did
    not include this method so another way out is to recreate the Text instance
    every time you need to set a new value.
    */
    final byte[] bytes = text.copyBytes();
    final int length = bytes.length;

    int start = 0;
    int key = 0;
    for( int i = 0; i < length; i++ ) {
      //while( i < length - 1 && bytes[i] != ';' ) {
      while( i < length && bytes[i] != ';' ) {
        i++;
      }
      if( i <= length ) {
        String s = new String( copyOfRange( bytes, start, i ), UTF_8 );
        if( s.startsWith( "\"" ) ) {
          s = s.substring( 1 );
          start++;
        }
        if( s.endsWith( "\"" ) ) {
          s = s.substring( 0, s.length() - 1 );
        }

        if( key == TripAttribute.VC_SERIAL.ordinal() ) {
          vcSerial = s;

        } else if( key == TripAttribute.STEP_LABELS.ordinal() ) {
          final StringTokenizer tk = new StringTokenizer( s, "-" );
          while( tk.hasMoreTokens() ) {
            processStep( tk.nextToken( "-" ) );
          }

        } else if( key == TripAttribute.PATHS.ordinal() ) {
          final StringTokenizer tk = new StringTokenizer( s, "-" );
          final List<Geometry> gl = new ArrayList<>();

          while( tk.hasMoreTokens() ) {
            final String n = tk.nextToken( "-" );
            try {
              final WKTReader textReader = new WKTReader();
              final Geometry g = textReader.read( n );
              gl.add( g );
            } catch( ParseException e ) {
              // nothing here
            }
          }
        } else if( key == TripAttribute.DURATION.ordinal() ) {
          try {
            final Double d = Double.parseDouble( s );
            //travelTime = d.intValue();
          } catch( NumberFormatException e ) {
            //travelTime = null;
          }

        } else if( key == TripAttribute.DISTANCE.ordinal() ) {
          try {
            final Double d = Double.parseDouble( s );
            //distance = d.intValue();
          } catch( NumberFormatException e ) {
            //distance = null;
          }
        } else if( key == TripAttribute.TRAVEL_MODE.ordinal() ) {
          travelMode = s;
        } else if( key == TripAttribute.PROFILE.ordinal() ) {
          profile = s;
        } else if( key == TripAttribute.MONTH.ordinal() ) {
          // Postgres values 1-12, Java values 0 -11
          month = Integer.parseInt( s ) - 1;
        } else if( key == TripAttribute.DAY_OF_WEEK.ordinal() ) {
          // Postgres values 0-6, Java values 1-7
          dayOfWeek = Integer.parseInt( s ) + 1;
        } else if( key == TripAttribute.HOUR.ordinal() ) {
          startHour = Integer.parseInt( s );
        } else if( key == TripAttribute.MINUTES.ordinal() ) {
          startMinutes = Integer.parseInt( s );
        } else if( key == TripAttribute.DAY_OF_YEAR.ordinal() ) {
          dayOfYear = Integer.parseInt( s );
        } else if( key == TripAttribute.YEAR.ordinal() ) {
          year = Integer.parseInt( s );
        }

        i++; // skip ';' character
        start = i;
        key++;
      } else {
        throw new IllegalStateException
          ( String.format( "Invalid line to parse: \"%s\"",
                           text.toString() ) );
      }
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @return
   */

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer();

    if( vcSerial != null ) {
      sb.append( vcSerial );
    }
    sb.append( '\t' );

    if( sites != null ) {
      for( int i = 0; i < sites.size() - 1; i++ ) {
        sb.append
          ( format( "(%s,%s)",
                    sites.get( i ),
                    sites.get( i + 1 ) ) );
        if( i < steps.size() - 1 ) {
          sb.append( "-" );
        }
      }
    }
    sb.append( '\t' );

    if( steps != null ) {
      for( int s = 0; s < steps.size(); s++ ) {
        final Geometry geom = steps.get( s ).getPath();
        final String wkt = geom == null ? "" : geom.toText();
        sb.append( wkt );

        if( s < steps.size() - 1 ) {
          sb.append( '-' );
        }
      }
    }
    sb.append( '\t' );


    if( travelMode != null ) {
      sb.append( travelMode );
    }
    sb.append( '\t' );

    if( profile != null ) {
      sb.append( profile );
    }

    // write the value of the objective function
    sb.append( '\t' );

    if( this.steps == null ) {
      sb.append( 0 );
    } else {
      sb.append( this.steps.size() );
    }
    sb.append( '\t' );

    sb.append( this.getDuration() );
    sb.append( '\t' );

    sb.append( this.getTravelTime() );
    sb.append( '\t' );

    //sb.append( this.getWaitingTime() );
    //sb.append( '\t' );

    sb.append( this.getDistance() );
    sb.append( '\t' );

    final TripValue tv = new TripValue( this );
    sb.append( tv.getNumScenicRoutes() );
    sb.append( '\t' );

    sb.append( tv.getSmoothness()[0] );
    sb.append( '\t' );

    sb.append( tv.getSmoothness()[1] );
    sb.append( '\t' );

    // print the hour of arrival in the site time
    if( siteTimes != null && !siteTimes.isEmpty() ) {
      sb.append( "(" );
      for( Integer t : siteTimes ) {
        sb.append( t );
        sb.append( "," );
      }
      sb.append( ")\t" );
    }

    // missing: shape optimization

    return sb.toString();
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param s
   */

  public void processStep( String s ) {
    if( s != null ) {
      if( s.startsWith( "(" ) ) {
        s = s.substring( 1 );
      }
      if( s.endsWith( ")" ) ) {
        s = s.substring( 0, s.length() - 1 );
      }
      s = s.trim();

      int index = s.indexOf( "," );
      String origin = null;
      String destination = null;
      if( index != -1 ) {
        origin = s.substring( 0, index );
      }
      if( index < s.length() - 1 ) {
        destination = s.substring( index + 1, s.length() );
      }

      if( sites == null ) {
        sites = new ArrayList<>();
      }

      if( origin != null && origin.length() > 0 ) {
        if( !sites.contains( origin ) ) { // remove duplicated sites
          //!sites.get( sites.size() - 1 ).equals( origin ) ) {
          // add the step only if it is different from the last one
          sites.add( origin );
        }
      }

      if( destination != null && destination.length() > 0 ) {
        if( !sites.contains( destination ) ) {
          //!sites.get( sites.size() - 1 ).equals( destination ) ) {
          // add the step only if it is different from the last one
          sites.add( destination );
        }
      }
    }
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param requiredPoi
   * @param dayOfYear
   * @param year
   * @param start
   * @return
   */

  public boolean checkFeature
  ( String requiredPoi, Integer dayOfYear, Integer year, boolean start ) {

    if( requiredPoi == null ) {
      throw new NullPointerException();
    }

    final boolean checkType =
      this.getTravelMode().toLowerCase().equals( "walking" ) &&
      ( this.profile.equals( "vrcard2-24" ) ||
        this.profile.equals( "24 Ore" ) );

    final boolean checkPoi;
    if( start ) {
      checkPoi = this.getSites().get( 0 ).equals( requiredPoi );
    } else {
      checkPoi = this.getSites().contains( requiredPoi );
    }

    final boolean checkDate;
    if( dayOfYear != null && year != null ) {
      checkDate = this.getDayOfYear().equals( dayOfYear ) &&
                  this.getYear().equals( year );
    } else {
      checkDate = true;
    }

    return checkPoi && checkType && checkDate;
  }


  /**
   * MISSING_COMMENT
   *
   * @param duration
   * @param durationOffset
   * @return
   */

  public boolean checkFeatureDuration
  ( int duration,
    int durationOffset ) {

    if( this.getDuration() <= duration - durationOffset //||
      //    this.getDuration() >= duration + durationOffset
      ) {
      return true;
    } else {
      return false;
    }
  }

  // ===========================================================================

  @Override
  public boolean equals( Object o ) {
    if( this == o ) return true;
    if( !( o instanceof TripWritable ) ) return false;

    TripWritable that = (TripWritable) o;

    if( vcSerial != null ? !vcSerial.equals( that.vcSerial ) : that.vcSerial != null )
      return false;
    if( sites != null ? !sites.equals( that.sites ) : that.sites != null )
      return false;
    if( travelMode != null ? !travelMode.equals( that.travelMode ) : that.travelMode != null )
      return false;
    if( profile != null ? !profile.equals( that.profile ) : that.profile != null )
      return false;
    if( month != null ? !month.equals( that.month ) : that.month != null )
      return false;
    if( dayOfWeek != null ? !dayOfWeek.equals( that.dayOfWeek ) : that.dayOfWeek != null )
      return false;
    if( startHour != null ? !startHour.equals( that.startHour ) : that.startHour != null )
      return false;
    if( startMinutes != null ? !startMinutes.equals( that.startMinutes ) : that.startMinutes != null )
      return false;
    if( dayOfYear != null ? !dayOfYear.equals( that.dayOfYear ) : that.dayOfYear != null )
      return false;
    if( year != null ? !year.equals( that.year ) : that.year != null )
      return false;
    if( visitingTime != null ? !visitingTime.equals( that.visitingTime ) : that.visitingTime != null )
      return false;
    if( siteTimes != null ? !siteTimes.equals( that.siteTimes ) : that.siteTimes != null )
      return false;
    return steps != null ? steps.equals( that.steps ) : that.steps == null;
  }

  @Override
  public int hashCode() {
    int result = vcSerial != null ? vcSerial.hashCode() : 0;
    result = 31 * result + ( sites != null ? sites.hashCode() : 0 );
    result = 31 * result + ( travelMode != null ? travelMode.hashCode() : 0 );
    result = 31 * result + ( profile != null ? profile.hashCode() : 0 );
    result = 31 * result + ( month != null ? month.hashCode() : 0 );
    result = 31 * result + ( dayOfWeek != null ? dayOfWeek.hashCode() : 0 );
    result = 31 * result + ( startHour != null ? startHour.hashCode() : 0 );
    result = 31 * result + ( startMinutes != null ? startMinutes.hashCode() : 0 );
    result = 31 * result + ( dayOfYear != null ? dayOfYear.hashCode() : 0 );
    result = 31 * result + ( year != null ? year.hashCode() : 0 );
    result = 31 * result + ( visitingTime != null ? visitingTime.hashCode() : 0 );
    result = 31 * result + ( siteTimes != null ? siteTimes.hashCode() : 0 );
    result = 31 * result + ( steps != null ? steps.hashCode() : 0 );
    return result;
  }

  public static byte[] convertToByteArray( double value ) {
    byte[] bytes = new byte[8];
    ByteBuffer buffer = ByteBuffer.allocate( bytes.length );
    buffer.putDouble( value );
    return buffer.array();
  }
}
