package it.univr.veronacard.shadoop.core;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.vividsolutions.jts.algorithm.Angle.angleBetween;
import static it.univr.veronacard.entities.ScenicRoutes.isScenicSite;
import static it.univr.utils.Statistics.computeMean;
import static it.univr.utils.Statistics.computeStandardDeviation;
import static java.lang.Math.abs;

//import static com.vividsolutions.jump.geom.Angle.*;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class TripValue implements Writable {

  // === Attributes ============================================================

  final int better = 1;
  final int worse = -1;
  final int equal = 0;

  // 15 minutes is the unit of measure for time
  final static int timeUnit = 15 * 60;
  // 500 meters is the unit of measure for distance
  final static int walkingDistanceUnit = 100;
  final static int drivingDistanceUnit = 100;

  // === Properties ============================================================

  private Integer numSteps;
  private Integer duration;
  private Integer travelTime;
  private Integer waitingTime;
  private Integer distance;
  private List<String> stepLabels;
  private List<Geometry> geometries;

  // === Methods ===============================================================

  public TripValue() {
    this.numSteps = null;
    this.duration = null;
    this.distance = null;
  }

  public TripValue( TripWritable trip ) {
    if( trip.getSteps() != null ) {
      this.numSteps = trip.getSteps().size();
    } else {
      this.numSteps = 0;
    }

    this.duration = trip.getDuration();
    this.travelTime = trip.getTravelTime();
    //this.waitingTime = trip.getWaitingTime();

    this.distance = trip.getDistance();
    this.geometries = trip.getGeometries();
    if( trip.getSites() != null ) {
      this.stepLabels = new ArrayList<>( trip.getSites() );
    } else {
      this.stepLabels = Collections.emptyList();
    }
  }

  public Integer getNumSteps() {
    return numSteps;
  }

  public void setNumSteps( Integer numSteps ) {
    this.numSteps = numSteps;
  }

  public Integer getDuration() {
    return duration;
  }

  public void setDuration( Integer duration ) {
    this.duration = duration;
  }

  public Integer getTravelTime() {
    return travelTime;
  }

  public void setTravelTime( Integer travelTime ) {
    this.travelTime = travelTime;
  }

  public Integer getWaitingTime() {
    return waitingTime;
  }

  public void setWaitingTime( Integer waitingTime ) {
    this.waitingTime = waitingTime;
  }

  public Integer getDistance() {
    return distance;
  }

  public void setDistance( Integer distance ) {
    this.distance = distance;
  }

  public List<Geometry> getGeometries() {
    return geometries;
  }

  public void setGeometries( List<Geometry> geometries ) {
    this.geometries = geometries;
  }

  public void addGeometry( Geometry g ) {
    if( this.geometries == null ) {
      this.geometries = new ArrayList<>();
    }
    this.geometries.add( g );
  }

  public List<String> getStepLabels() {
    return stepLabels;
  }

  public void setStepLabels( List<String> stepLabels ) {
    this.stepLabels = stepLabels;
  }

  public void addStepLabel( String label ) {
    if( this.stepLabels == null ) {
      this.stepLabels = new ArrayList<>();
    }
    this.stepLabels.add( label );
  }

  // ===========================================================================

  public boolean dominate
    ( TripValue other,
      int minDuration,
      int maxDuration,
      String travelMode ) {

    if( other == null ) {
      return true;
    }

    if( this.equals( other ) ) {
      return false;
    }

    int[] objValues = new int[7];
    objValues[0] = compareNumSteps( other );
    objValues[1] = compareDuration( other, minDuration, maxDuration );
    //objValues[1] = equal;
    objValues[2] = compareTravelTime( other );
    //objValues[3] = compareWaitingTime( other );
    objValues[3] = equal;
    objValues[4] = compareDistance( other, travelMode );
    objValues[5] = compareNumScenicRoutes( other );
    //objValues[5] = equal;
    //objValues[6] = compareGeometries( other );
    objValues[6] = compareSmoothness( other );
    //objValues[6] = equal;

    boolean e = true;
    boolean b = false;
    for( int i = 0; i < objValues.length; i++ ) {
      e = e && objValues[i] >= equal;
      b = b || objValues[i] == better;
    }
    return e && b;
  }

  // ===========================================================================

  /**
   * The current trip value is better than the value {@code other}, if the
   * number of its steps is greater than the number of steps in {@code other}.
   * Conversely, the two trip values are equal, if they have the same number of
   * steps. Finally, in the current trip value is worse than the other one, in
   * the last opposite case.
   *
   * @param other
   * @return
   */
  private int compareNumSteps( TripValue other ) {
    if( other == null ) {
      throw new NullPointerException();
    }

    final int aSteps, bSteps;
    if( this.numSteps == null ) {
      aSteps = 0;
    } else {
      aSteps = this.numSteps;
    }
    if( other.numSteps == null ) {
      bSteps = 0;
    } else {
      bSteps = other.numSteps;
    }
    if( aSteps > bSteps ) {
      return better;
    } else if( aSteps == bSteps ) {
      return equal;
    } else {
      return worse;
    }
  }


  /**
   * The current trip value is better than the value {@code other}, if its
   * duration is contained in the interval {@code minDuration} and {@code
   * maxDuration}, while the duration of the other value does not. Conversely,
   * it is worse if its duration is not contained in the desired interval, while
   * the other duration does. Finally, if the duration of the two values are
   * both inside the desired interval, or both outside the desired interval, the
   * better value is the one with the smaller distance from the {@code
   * maxDuration} value.
   *
   * @param other
   * @param maxDuration
   * @return
   */
  private int compareDuration
  ( TripValue other,
    int minDuration,
    int maxDuration ) {

    if( other == null ) {
      throw new NullPointerException();
    }

    final int aDuration;
    if( this.duration == null ) {
      aDuration = 0;
    } else {
      aDuration = this.duration;
    }

    final int bDuration;
    if( other.duration == null ) {
      bDuration = 0;
    } else {
      bDuration = other.duration;
    }

    if( aDuration >= minDuration && aDuration <= maxDuration &&
        ( bDuration < minDuration || bDuration > maxDuration ) ) {
      return better;

    } else if( bDuration >= minDuration && bDuration <= maxDuration &&
               ( aDuration < minDuration || aDuration > maxDuration ) ) {
      return worse;

    } else if( ( aDuration < minDuration || aDuration > maxDuration ) &&
               ( bDuration < minDuration || bDuration > maxDuration ) ) {

      if( aDuration < minDuration && bDuration < minDuration ) {
        final int aDifference = minDuration - aDuration;
        final int bDifference = minDuration - bDuration;
        final int compare = aDifference - bDifference;

        if( compare < 0 && abs( compare ) > timeUnit ) {
          return better;
        } else if( abs( compare ) <= timeUnit ) {
          return equal;
        } else { // compare > 0 && abs( compare ) > timeUnit
          return worse;
        }

      } else if( aDuration > maxDuration && bDuration > maxDuration ) {
        final int aDifference = ( aDuration - maxDuration );
        final int bDifference = ( bDuration - maxDuration );
        final int compare = aDifference - bDifference;

        if( compare < 0 && abs( compare ) > timeUnit ) {
          return better;
        } else if( abs( compare ) <= timeUnit ) {
          return equal;
        } else { // compare > 0 && abs( compare ) > timeUnit
          return worse;
        }

      } else {
        final int aDifference = abs( aDuration - maxDuration );
        final int bDifference = abs( bDuration - maxDuration );
        final int compare = aDifference - bDifference;

        if( compare < 0 && abs( compare ) > timeUnit ) {
          return better;
        } else if( abs( compare ) <= timeUnit ) {
          return equal;
        } else { // compare > 0 && abs( compare ) > timeUnit
          return worse;
        }
      }

    } else {
      final int aDifference = abs( aDuration - maxDuration );
      final int bDifference = abs( bDuration - maxDuration );
      final int compare = aDifference - bDifference;

      if( compare < 0 && abs( compare ) > timeUnit ) {
        return better;
      } else if( abs( compare ) <= timeUnit ) {
        return equal;
      } else { // compare > 0 && abs( compare ) > timeUnit
        return worse;
      }
    }
  }


  /**
   * The current trip value is better than the value {@code other}, if its
   * travel time is less than the travel time of {@code other}. The two values
   * are equal if their travel time is the same. Finally, the current value is
   * worse than the value {@code other}, if its travel time is greater than the
   * travel time of {@code other}.
   *
   * @param other
   * @return
   */

  private int compareTravelTime( TripValue other ) {
    if( other == null ) {
      throw new NullPointerException();
    }

    final int aTravelTime;
    if( this.travelTime == null ) {
      aTravelTime = 0;
    } else {
      aTravelTime = this.travelTime;
    }

    final int bTravelTime;
    if( other.travelTime == null ) {
      bTravelTime = 0;
    } else {
      bTravelTime = other.travelTime;
    }

    final int compare = aTravelTime - bTravelTime;

    if( compare < 0 && abs( compare ) > timeUnit ) {
      return better;
    } else if( abs( compare ) <= timeUnit ) {
      return equal;
    } else {
      return worse;
    }
  }


  /**
   * The current trip value is better than the value {@code other}, if its
   * waiting time is less than the waiting time of {@code other}. The two values
   * are equal if their waiting time is the same. Finally, the current value is
   * worse than the value {@code other}, if its waiting time is greater than the
   * waiting time of {@code other}.
   *
   * @param other
   * @return
   */

  private int compareWaitingTime( TripValue other ) {
    if( other == null ) {
      throw new NullPointerException();
    }

    final int aWaitingTime;
    if( this.getWaitingTime() == null ) {
      aWaitingTime = 0;
    } else {
      aWaitingTime = this.getWaitingTime();
    }

    final int bWaitingTime;
    if( other.getWaitingTime() == null ) {
      bWaitingTime = 0;
    } else {
      bWaitingTime = other.getWaitingTime();
    }

    final int compare = aWaitingTime - bWaitingTime;

    if( compare < 0 && abs( compare ) > timeUnit ) {
      return better;
    } else if( abs( compare ) <= timeUnit ) {
      return equal;
    } else { // compare > 0 && abs( compare ) > timeUnit
      return worse;
    }
  }


  /**
   * The current trip value is better than the value {@code other} if its
   * distance is less than the other distance. Conversely, the two values are
   * equal if their distance is the same. Finally, in the current trip value is
   * worse than the other one, in the last opposite case.
   *
   * @param other
   * @param travelMode
   * @return
   */
  private int compareDistance( TripValue other, String travelMode ) {
    if( other == null ) {
      throw new NullPointerException();
    }

    final double aDistance;
    if( this.distance == null ) {
      aDistance = 0;
    } else {
      aDistance = this.distance;
    }
    final int aSteps;
    if( this.getNumSteps() == null ) {
      aSteps = 0;
    } else {
      aSteps = this.getNumSteps();
    }

    final double bDistance;
    if( other.distance == null ) {
      bDistance = 0;
    } else {
      bDistance = other.distance;
    }
    final int bSteps;
    if( other.getNumSteps() == null ) {
      bSteps = 0;
    } else {
      bSteps = other.getNumSteps();
    }

    final int distanceUnit;
    if( travelMode.toLowerCase().equals( "walking" ) ) {
      distanceUnit = walkingDistanceUnit;
    } else {
      distanceUnit = drivingDistanceUnit;
    }

    if( aSteps != 0 && bSteps != 0 ) {
      final double compare = aDistance / aSteps - bDistance / bSteps;

      if( compare < 0 && abs( compare ) > distanceUnit ) {
        return better;
      } else if( abs( compare ) <= distanceUnit ) {
        return equal;
      } else {
        return worse;
      }
    } else if( aSteps == 0 && bSteps != 0 ) {
      return worse;
    } else if( aSteps != 0 && bSteps == 0 ) {
      return better;
    } else {
      return equal;
    }//*/
  }


  /**
   * Returns the number of scenic routes in the current trip.
   *
   * @return
   */

  public int getNumScenicRoutes(){
    int scenicRoutes = 0;
    if( this.stepLabels != null && this.stepLabels.size() > 0 ) {
      for( int i = 0; i < this.stepLabels.size() - 1; i++ ) {
        final String orig = this.stepLabels.get( i );
        final String dest = this.stepLabels.get( i + 1 );
        if( isScenicSite( orig ) && isScenicSite( dest ) ) {
          scenicRoutes++;
        }
      }
    }
    return scenicRoutes;
  }


  /**
   * The current trip value is better than the value {@code other} if the number
   * of contained scenic routes is grater than the number of scenic routes
   * contained in {@code other}. Conversely, the two values are equal if they
   * contains the same number of scenic routes. Finally, the current value is
   * worse if it contains less scenic routes.
   *
   * @param other
   * @return
   */
  private int compareNumScenicRoutes( TripValue other ) {
    if( other == null ) {
      throw new NullPointerException();
    }

    final int aScenicRoutes = this.getNumScenicRoutes();
    final int bScenicRoutes = other.getNumScenicRoutes();

    if( aScenicRoutes > bScenicRoutes ) {
      return better;
    } else if( aScenicRoutes == bScenicRoutes ) {
      return equal;
    } else {
      return worse;
    }
  }


  /**
   * Returns the smoothness value.
   *
   * @return
   */

  public double[] getSmoothness(){
    final List<Double> angles = computesAngularAttributes( this.getGeometries() );
    final double mean = computeMean( angles );
    final double stnDev = computeStandardDeviation( angles );
    return new double[]{ mean, stnDev };
  }

  /**
   * The current trip value is better than the value {@code other} if it has
   * less smoothness. The two trip values are the same if they have the same
   * smoothness. Finally, the current trip value is worse than the other one, in
   * the last opposite case.
   *
   * @param other
   * @return
   */

  private int compareSmoothness( TripValue other ) {
    if( other == null ) {
      throw new NullPointerException();
    }

    final List<Geometry> aGeomList = this.getGeometries();
    final List<Geometry> bGeomList = other.getGeometries();

    final boolean aEmpty;
    if( aGeomList == null || aGeomList.isEmpty() ) {
      aEmpty = true;
    } else {
      aEmpty = false;
    }

    final boolean bEmpty;
    if( bGeomList == null || bGeomList.isEmpty() ) {
      bEmpty = true;
    } else {
      bEmpty = false;
    }

    if( aEmpty == true && bEmpty == true ) {
      return equal;

    } else if( aEmpty == true && bEmpty == false ) {
      return worse;

    } else if( aEmpty == false && bEmpty == true ) {
      return better;

    } else {
      final List<Double> aAngleList = computesAngularAttributes( aGeomList );
      final List<Double> bAngleList = computesAngularAttributes( bGeomList );

      final double aMean = computeMean( aAngleList );
      final double aStnDev = computeStandardDeviation( aAngleList );

      final double bMean = computeMean( bAngleList );
      final double bStnDev = computeStandardDeviation( bAngleList );

      if( aMean >= bMean && aStnDev < bStnDev ) {
        return better;
      } else if( bMean >= aMean && bStnDev < aStnDev ) {
        return worse;
      } else {
        return equal;
      }
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param geomList
   * @return
   */

  public static List<Double> computesAngularAttributes( List<Geometry> geomList ) {
    if( geomList == null ) {
      throw new NullPointerException();
    }

    final List<Coordinate> coordinates = new ArrayList<>();
    for( Geometry g : geomList ) {
      coordinates.addAll( Arrays.asList( g.getCoordinates() ) );
    }

    final List<Double> angleList = new ArrayList<>( geomList.size() );
    for( int i = 0; i < coordinates.size() - 2; i++ ) {
      angleList.add
        ( angleBetween
            ( coordinates.get( i ),
              coordinates.get( i + 1 ),
              coordinates.get( i + 2 ) ) );
    }

    return angleList;
  }


  /**
   * The current trip value is better than the value {@code other} if it has
   * less overlaps in the paths (does not returns along the same paths more
   * times). The two trip values are the same if they have the same number of
   * returns in the path. Finally, the current trip value is worse than the
   * other one, in the last opposite case.
   *
   * @param other
   * @return
   * @deprecated
   */
  private int compareGeometries( TripValue other ) {
    if( other == null ) {
      throw new NullPointerException();
    }

    final boolean aEmpty;
    if( this.geometries == null || this.geometries.isEmpty() ) {
      aEmpty = true;
    } else {
      aEmpty = false;
    }

    final boolean bEmpty;
    if( other.geometries == null || other.geometries.isEmpty() ) {
      bEmpty = true;
    } else {
      bEmpty = false;
    }

    if( aEmpty == true && bEmpty == true ) {
      return equal;

    } else if( aEmpty == true && bEmpty == false ) {
      return worse;

    } else if( aEmpty == false && bEmpty == true ) {
      return better;

    } else {
      final Set<String> aSet;
      if( this.stepLabels == null ) {
        aSet = Collections.emptySet();
      } else {
        aSet = new HashSet<>( this.stepLabels );
      }

      final Set<String> bSet;
      if( other.stepLabels == null ) {
        bSet = Collections.emptySet();
      } else {
        bSet = new HashSet<>( other.stepLabels );
      }

      if( aSet.equals( bSet ) ) {

        /*int aSelfOverlaps = 0;
        for( int i = 0 ; i < this.geometries.size(); i++ ){
          final LineString aCurr = (LineString) this.geometries.get( i );

          final List<LineString> als = new ArrayList<>();
          for( int j = 0; j < this.geometries.size(); j++ ){
            if( i != j ){
              als.add( (LineString) this.geometries.get( j ) );
            }
          }

          final MultiLineString aGeometry =
            new MultiLineString
              ( als.toArray( new LineString[als.size()] ),
                new GeometryFactory() );

          if( aCurr.overlaps( aGeometry )){
            aSelfOverlaps++;
          }
        }//*/


        /*int bSelfOverlaps = 0;
        for( int i = 0 ; i < other.geometries.size(); i++ ){
          final LineString bCurr = (LineString) other.geometries.get( i );

          final List<LineString> bls = new ArrayList<>();
          for( int j = 0; j < other.geometries.size(); j++ ){
            if( i != j ){
              bls.add( (LineString) other.geometries.get( j ) );
            }
          }

          final MultiLineString bGeometry =
            new MultiLineString
              ( bls.toArray( new LineString[bls.size()] ),
                new GeometryFactory() );

          if( bCurr.overlaps( bGeometry )){
            bSelfOverlaps++;
          }
        }//*/

        /*if( aSelfOverlaps < bSelfOverlaps ){
          return better;
        } else if( aSelfOverlaps == bSelfOverlaps ){
          return equal;
        } else {
          return worse;
        }*/

        final List<LineString> als = new ArrayList<>();
        double aPathArea = 0;
        for( Geometry g : this.geometries ) {
          if( g != null && g instanceof LineString ) {
            final LineString al = (LineString) g;
            aPathArea += al.buffer( 0.01 ).getArea();
            als.add( al );

          }
        }

        final List<LineString> bls = new ArrayList<>();
        double bPathArea = 0;
        for( Geometry g : other.geometries ) {
          if( g != null && g instanceof LineString ) {
            final LineString bl = (LineString) g;
            bPathArea += bl.buffer( 0.01 ).getArea();
            bls.add( bl );
          }
        }

        // check the self-overlap regions
        final MultiLineString aGeometry =
          new MultiLineString
            ( als.toArray( new LineString[als.size()] ),
              new GeometryFactory() );


        final MultiLineString bGeometry =
          new MultiLineString
            ( bls.toArray( new LineString[bls.size()] ),
              new GeometryFactory() );

        final double aArea = aGeometry.buffer( 0.01 ).getArea();
        final double bArea = bGeometry.buffer( 0.01 ).getArea();

        final double aValue = aArea / aPathArea;
        final double bValue = bArea / bPathArea;

        if( aValue < bValue ) {
          return better;
        } else if( aValue == bValue ) {
          return equal;
        } else {
          return worse;
        }//*/
      } else {
        return equal;
      }
    }
  }

  // ===========================================================================


  @Override
  public boolean equals( Object o ) {
    if( this == o ) return true;
    if( !( o instanceof TripValue ) ) return false;

    TripValue tripValue = (TripValue) o;

    if( distance != null ? !distance.equals( tripValue.distance ) : tripValue.distance != null )
      return false;
    if( duration != null ? !duration.equals( tripValue.duration ) : tripValue.duration != null )
      return false;
    if( numSteps != null ? !numSteps.equals( tripValue.numSteps ) : tripValue.numSteps != null )
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = numSteps != null ? numSteps.hashCode() : 0;
    result = 31 * result + ( duration != null ? duration.hashCode() : 0 );
    result = 31 * result + ( distance != null ? distance.hashCode() : 0 );
    return result;
  }

  // ===========================================================================


  @Override
  public void write( DataOutput dataOutput ) throws IOException {
    if( dataOutput == null ) {
      throw new NullPointerException();
    }
    if( numSteps != null ) {
      dataOutput.writeInt( numSteps );
    } else {
      dataOutput.writeInt( 0 );
    }

    if( duration != null ) {
      dataOutput.writeInt( duration );
    } else {
      dataOutput.writeInt( 0 );
    }

    if( travelTime != null ) {
      dataOutput.writeInt( travelTime );
    } else {
      dataOutput.writeInt( 0 );
    }

    if( waitingTime != null ) {
      dataOutput.writeInt( waitingTime );
    } else {
      dataOutput.writeInt( 0 );
    }

    if( distance != null ) {
      dataOutput.writeInt( distance );
    } else {
      dataOutput.writeInt( 0 );
    }

    if( stepLabels != null && !stepLabels.isEmpty() ) {
      dataOutput.writeInt( stepLabels.size() );
      for( String s : stepLabels ) {
        dataOutput.writeUTF( s );
      }
    } else {
      dataOutput.writeInt( 0 );
    }

    if( geometries != null && !geometries.isEmpty() ) {
      dataOutput.writeInt( geometries.size() );
      for( Geometry g : geometries ) {
        final String wkt = g == null ? "" : g.toText();
        dataOutput.writeUTF( wkt );
      }
    } else {
      dataOutput.writeInt( 0 );
    }
  }

  @Override
  public void readFields( DataInput dataInput ) throws IOException {
    if( dataInput == null ) {
      throw new NullPointerException();
    }

    numSteps = dataInput.readInt();
    duration = dataInput.readInt();
    travelTime = dataInput.readInt();
    waitingTime = dataInput.readInt();
    distance = dataInput.readInt();

    final int nsl = dataInput.readInt();
    stepLabels = new ArrayList<>( nsl );
    for( int i = 0; i < nsl; i++ ) {
      stepLabels.add( dataInput.readUTF() );
    }

    final int ng = dataInput.readInt();
    geometries = new ArrayList<>();
    for( int i = 0; i < ng; i++ ) {
      final String wkt = dataInput.readUTF();
      final WKTReader textReader = new WKTReader();
      try {
        geometries.add( textReader.read( wkt ) );
      } catch( ParseException e ) {
        // nothing here
      }
    }
  }


  public String toString() {
    final StringBuffer sb = new StringBuffer();

    if( numSteps != null ) {
      sb.append( numSteps );
    } else {
      sb.append( 0 );
    }
    sb.append( '\t' );

    if( duration != null ) {
      sb.append( duration );
    } else {
      sb.append( 0 );
    }
    sb.append( '\t' );

    if( travelTime != null ) {
      sb.append( travelTime );
    } else {
      sb.append( 0 );
    }
    sb.append( '\t' );

    if( waitingTime != null ) {
      sb.append( waitingTime );
    } else {
      sb.append( 0 );
    }
    sb.append( '\t' );

    if( distance != null ) {
      sb.append( distance );
    } else {
      sb.append( 0 );
    }
    sb.append( '\t' );

    if( stepLabels != null ) {
      for( String s : stepLabels ) {
        sb.append( s );
        sb.append( "," );
      }
    }
    sb.append( '\t' );

    if( geometries != null ) {
      for( int i = 0; i < geometries.size(); i++ ) {
        final String wkt = geometries.get( i ).toText();
        sb.append( wkt );
        sb.append( "," );
      }
    }
    sb.append( '\t' );
    return sb.toString();
  }


}
