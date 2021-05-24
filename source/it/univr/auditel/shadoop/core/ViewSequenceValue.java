package it.univr.auditel.shadoop.core;

import it.univr.auditel.entities.GroupView;
import it.univr.auditel.entities.ProgramRecord;
import it.univr.auditel.entities.UserPreference;
//new
import it.univr.auditel.entities.ChannelTransition;
//endnew
import it.univr.auditel.entities.Utils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static it.univr.auditel.entities.Utils.*;
import static java.lang.Double.*;
import static java.lang.Math.abs;
import static java.util.Calendar.DATE;
import static java.util.Calendar.MONTH;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class ViewSequenceValue implements Writable {

  // === Attributes ============================================================

  final int better = 1;
  final int worse = -1;
  final int equal = 0;

  // 5 minutes is the unit of measure for time
  private static long deltaTime = 5 * 60 * 1000;

  // === Properties ============================================================

  private int duration;
  private List<String> channelSequence;
  private Map<String, Double> userPreferences;
  private Map<String, Integer> missedProgramSeconds;

  // === Methods ===============================================================


  /**
   * MISSING_COMMENT
   */

  public ViewSequenceValue() {
    duration = 0;
    channelSequence = new ArrayList<>();
    userPreferences = new HashMap<>();
    missedProgramSeconds = new HashMap<>();
  }


  /**
   * MISSING_COMMENT
   *
   * @param sequence
   * @param preferenceMap
   * @param schedulingMap
   */

  public ViewSequenceValue
  ( ViewSequenceWritable sequence,
    Map<String, List<UserPreference>> preferenceMap,
    Map<Date, Map<String, List<ProgramRecord>>> schedulingMap ) {

    if( sequence == null ) {
      throw new NullPointerException();
    }
    if( preferenceMap == null ) {
      throw new NullPointerException();
    }
    if( schedulingMap == null ) {
      throw new NullPointerException();
    }

    if( !sequence.isEmpty() ) {
      final GroupView start = sequence.getSequence().get( 0 );
      final GroupView end = sequence.getSequence().get( sequence.size() - 1 );
      duration = (int)
        ( ( end.getIntervalEnd().getTime() - start.getIntervalStart().getTime() ) /
          ( 1000 * 60 ) );

      channelSequence = new ArrayList<>();
      userPreferences = new HashMap<>();
      missedProgramSeconds = new HashMap<>();

      for( GroupView v : sequence.getSequence() ) {
        channelSequence.add( v.getEpgChannelId() );

        for( String user : v.getGroup().getUsers() ) {
          if( preferenceMap.get( user ) != null ) {
            final List<UserPreference> upl = preferenceMap.get( user );

            Double maxPref = Double.MIN_VALUE;
            Double currPref = null;
            for( UserPreference up : upl ) {
              if( up.getChannelId().equals( v.getEpgChannelId() ) &&
                  up.getTimeSlot().equals( v.getTimeSlot() )
                // TODO &&
                //up.getDayOfWeek().equals( v.getda )
                ) {
                currPref = up.getPreference();
              } else {
                maxPref = Double.max( maxPref, up.getPreference() );
              }
            }

            Double p = userPreferences.get( user );
            if( p == null ) {
              p = 0.0;
            }
            if( currPref != null ) {
              p += currPref;
            } else {
              p += maxPref;
            }
            userPreferences.put( user, p );
          }
        }

        final ProgramRecord r = findProgram( v, schedulingMap );
        if( r != null ){
          final int seconds =
            (int) ( ( v.getIntervalStart().getTime() -
                      r.getStartTime().getTime() ) / 1000 +
                    ( r.getEndTime().getTime() -
                      v.getIntervalEnd().getTime() ) / 1000 );

          missedProgramSeconds.put( r.getProgramId(), seconds );
        }
      }
    }
  }


  /**
   * Duration in minutes.
   *
   * @return
   */
  public long getDuration() {
    return duration;
  }

  public void setDuration( int duration ) {
    this.duration = duration;
  }

  public List<String> getChannelSequence() {
    return channelSequence;
  }

  public void addChannel( String channel ) {
    if( channelSequence == null ) {
      channelSequence = new ArrayList<>();
    }
    channelSequence.add( channel );
  }

  public void setChannelSequence( List<String> channelSequence ) {
    this.channelSequence = channelSequence;
  }

  public Map<String, Double> getUserPreferences() {
    return userPreferences;
  }

  public void setUserPreferences( Map<String, Double> userPreferences ) {
    this.userPreferences = userPreferences;
  }

  public void addUserPreference( String user, Double preference ) {
    if( userPreferences == null ) {
      userPreferences = new HashMap<>();
    }
    userPreferences.put( user, preference );
  }

  public Map<String, Integer> getMissedProgramSeconds() {
    return missedProgramSeconds;
  }

  public void setMissedProgramSeconds( Map<String, Integer> missedProgramSeconds ) {
    this.missedProgramSeconds = missedProgramSeconds;
  }

  public void addMissedProgramSeconds( String program, Integer seconds ){
    if( missedProgramSeconds == null ){
      missedProgramSeconds = new HashMap<>(  );
    }
    missedProgramSeconds.put( program, seconds );
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param dataOutput
   * @throws IOException
   */

  @Override
  public void write( DataOutput dataOutput ) throws IOException {
    if( dataOutput == null ) {
      throw new NullPointerException();
    }

    dataOutput.writeInt( duration );

    if( channelSequence != null && !channelSequence.isEmpty() ) {
      dataOutput.writeInt( channelSequence.size() );
      for( String s : channelSequence ) {
        dataOutput.writeUTF( s );
      }
    } else {
      dataOutput.writeInt( 0 );
    }

    if( userPreferences != null && !userPreferences.isEmpty() ) {
      dataOutput.writeInt( userPreferences.size() );
      for( Map.Entry<String, Double> p : userPreferences.entrySet() ) {
        dataOutput.writeUTF( p.getKey() );
        dataOutput.writeDouble( p.getValue() );
      }
    } else {
      dataOutput.writeInt( 0 );
    }

    if( missedProgramSeconds != null && !missedProgramSeconds.isEmpty()){
      dataOutput.writeInt( missedProgramSeconds.size() );
      for( Map.Entry<String,Integer> m : missedProgramSeconds.entrySet() ){
        dataOutput.writeUTF( m.getKey() );
        dataOutput.writeInt( m.getValue() );
      }
    } else {
      dataOutput.writeInt( 0 );
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param dataInput
   * @throws IOException
   */

  @Override
  public void readFields( DataInput dataInput ) throws IOException {
    if( dataInput == null ) {
      throw new NullPointerException();
    }

    duration = dataInput.readInt();

    final int size = dataInput.readInt();
    channelSequence = new ArrayList<>();
    for( int i = 0; i < size; i++ ) {
      channelSequence.add( dataInput.readUTF() );
    }

    final int pSize = dataInput.readInt();
    userPreferences = new HashMap<>();
    for( int i = 0; i < pSize; i++ ) {
      userPreferences.put( dataInput.readUTF(), dataInput.readDouble() );
    }

    final int mSize = dataInput.readInt();
    missedProgramSeconds = new HashMap<>();
    for( int i = 0; i < mSize; i++ ){
      missedProgramSeconds.put( dataInput.readUTF(), dataInput.readInt() );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @return
   */

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder();
    b.append( duration );
    b.append( "\t" );

    if( channelSequence != null && !channelSequence.isEmpty() ) {
      final Iterator<String> it = channelSequence.iterator();
      while( it.hasNext() ) {
        b.append( it.next() );
        if( it.hasNext() ) {
          b.append( "," );
        }
      }
    }
    b.append( "\t" );

    if( userPreferences != null && !userPreferences.isEmpty() ) {
      final Iterator<String> kit = userPreferences.keySet().iterator();
      while( kit.hasNext() ) {
        final String key = kit.next();
        b.append( key );
        b.append( "-" );
        b.append( userPreferences.get( key ) );

        if( kit.hasNext() ) {
          b.append( "," );
        }
      }
    }

    b.append( "\t" );

    if( missedProgramSeconds != null && ! missedProgramSeconds.isEmpty() ){
      final Iterator<String> mit = missedProgramSeconds.keySet().iterator();
      while( mit.hasNext() ){
        final String key = mit.next();
        b.append( key );
        b.append( "-" );
        b.append( missedProgramSeconds.get( key ) );

        if( mit.hasNext() ){
          b.append( "," );
        }
      }
    }

    b.append( "\t" );

    return b.toString();
  }

  // ===========================================================================

  @Override
  public boolean equals( Object o ) {
    if( this == o ) return true;
    if( !( o instanceof ViewSequenceValue ) ) return false;

    ViewSequenceValue that = (ViewSequenceValue) o;

    if( better != that.better ) return false;
    if( duration != that.duration ) return false;
    if( equal != that.equal ) return false;
    if( worse != that.worse ) return false;
    if( channelSequence != null ? !channelSequence.equals( that.channelSequence ) : that.channelSequence != null )
      return false;
    if( userPreferences != null ? !userPreferences.equals( that.userPreferences ) : that.userPreferences != null )
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = better;
    result = 31 * result + worse;
    result = 31 * result + equal;
    result = 31 * result + duration;
    result = 31 * result + ( channelSequence != null ? channelSequence.hashCode() : 0 );
    result = 31 * result + ( userPreferences != null ? userPreferences.hashCode() : 0 );
    return result;
  }


  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param other
   * @param minDuration
   * @param maxDuration
   * @return
   */

  public boolean dominate
  ( ViewSequenceValue other,
    int minDuration,
    int maxDuration ) {

    if( other == null ) {
      return true;
    }

    if( this.equals( other ) ) {
      return false;
    }

    int[] objValues = new int[4];
    objValues[0] = compareDuration( other, minDuration, maxDuration );
    objValues[1] = compareHistory( other );
    objValues[2] = comparePreference( other );
    objValues[3] = compareSingleDuration( other );

    boolean e = true;
    boolean b = false;
    for( int i = 0; i < objValues.length; i++ ) {
      e = e && objValues[i] >= equal;
      b = b || objValues[i] == better;
    }
    return e && b;
  }


  /**
   * MISSING_COMMENT
   *
   * @param other
   * @param minDuration
   * @param maxDuration
   * @return
   */

  private int compareDuration
  ( ViewSequenceValue other,
    int minDuration,
    int maxDuration ) {

    if( other == null ) {
      throw new NullPointerException();
    }

    if( other == null ) {
      throw new NullPointerException();
    }

    final int aDuration = this.duration;
    final int bDuration = this.duration;


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

        if( compare < 0 && abs( compare ) > deltaTime ) {
          return better;
        } else if( abs( compare ) <= deltaTime ) {
          return equal;
        } else { // compare > 0 && abs( compare ) > timeUnit
          return worse;
        }

      } else if( aDuration > maxDuration && bDuration > maxDuration ) {
        final int aDifference = ( aDuration - maxDuration );
        final int bDifference = ( bDuration - maxDuration );
        final int compare = aDifference - bDifference;

        if( compare < 0 && abs( compare ) > deltaTime ) {
          return better;
        } else if( abs( compare ) <= deltaTime ) {
          return equal;
        } else { // compare > 0 && abs( compare ) > timeUnit
          return worse;
        }

      } else {
        final int aDifference = abs( aDuration - maxDuration );
        final int bDifference = abs( bDuration - maxDuration );
        final int compare = aDifference - bDifference;

        if( compare < 0 && abs( compare ) > deltaTime ) {
          return better;
        } else if( abs( compare ) <= deltaTime ) {
          return equal;
        } else { // compare > 0 && abs( compare ) > timeUnit
          return worse;
        }
      }

    } else {
      final int aDifference = abs( aDuration - maxDuration );
      final int bDifference = abs( bDuration - maxDuration );
      final int compare = aDifference - bDifference;

      if( compare < 0 && abs( compare ) > deltaTime ) {
        return better;
      } else if( abs( compare ) <= deltaTime ) {
        return equal;
      } else { // compare > 0 && abs( compare ) > timeUnit
        return worse;
      }
    }

  }


  /**
   * MISSING_COMMENT
   *
   * @param other
   * @return
   */

  private int compareHistory( ViewSequenceValue other ) {
    return equal;
  }


  /**
   * MISSING_COMMENT
   *
   * @param other
   * @return
   */

  private int comparePreference( ViewSequenceValue other ) {

    if( other == null || other.userPreferences == null ) {
      return better;
    }

    if( this.userPreferences == null &&
        other.userPreferences != null ){
      if( other.userPreferences.isEmpty() ){
        return equal;
      } else {
        return worse;
      }
    }

    double aSum = 0;
    for( double p : userPreferences.values() ){
      aSum += aSum;
    }
    double aAvg = aSum / userPreferences.size();
    double aStd = 0;
    for( double p : userPreferences.values() ){
      aStd += Math.pow( ( p - aAvg ), 2 );
    }
    aStd = Math.sqrt( aStd / userPreferences.size() );

    double bSum = 0;
    for( double p : other.userPreferences.values() ){
      bSum += bSum;
    }
    double bAvg = bSum / other.userPreferences.size();
    double bStd = 0;
    for( double p : other.userPreferences.values() ){
      bStd += Math.pow( ( p - bAvg ), 2 );
    }
    bStd = Math.sqrt( bStd / other.userPreferences.size() );

    if( aAvg == bAvg && aStd == bStd ){
      return equal;
    } else if( ( aAvg == bAvg && aStd > bStd ) ||
               ( aAvg > bAvg && aStd == bStd ) ||
               ( aAvg > bAvg && aStd > bStd )){
      return better;
    } else if( ( bAvg == aAvg && bStd > aStd ) ||
               ( bAvg > aAvg && bStd == aStd ) ||
               ( bAvg > aAvg && bStd > aStd )){
      return worse;
    } else {
      return equal;
    }

    // check that the value is related to the same group of users
    /*if( !this.userPreferences.keySet().containsAll( other.userPreferences.keySet() ) ||
        !other.userPreferences.keySet().containsAll( this.userPreferences.keySet() ) ) {

      // compare the individual average satisfaction
      double otherMin = MAX_VALUE;
      double otherMax = MIN_VALUE;
      for( double d : other.userPreferences.values() ) {
        otherMin = min( otherMin, d );
        otherMax = max( otherMax, d );
      }

      double thisMin = MAX_VALUE;
      double thisMax = MIN_VALUE;
      for( double d : this.userPreferences.values() ) {
        thisMin = min( thisMin, d );
        thisMax = max( thisMax, d );
      }

      if( thisMin > otherMin || thisMin == otherMin && thisMax > otherMax ) {
        return better;
      } else if( thisMin < otherMin ) {
        return worse;
      } else {
        return equal;
      }

    } else {
      // the preference of at least a user in the group is greater and the
      // other are equals
      int bp = 0;
      int wp = 0;
      int ep = 0;
      final Iterator<String> it = this.userPreferences.keySet().iterator();
      while( it.hasNext() ) {
        final String key = it.next();
        if( this.userPreferences.get( key ) > other.userPreferences.get( key ) ) {
          bp++;
        } else if( this.userPreferences.get( key ) < other.userPreferences.get( key ) ) {
          wp++;
        } else {
          ep++;
        }
      }

      if( bp > 0 && ( ep + bp ) == this.userPreferences.keySet().size() ) {
        return better;
      } else if( wp > 0 && ( wp + bp ) == this.userPreferences.keySet().size() ) {
        return worse;
      } else {
        return equal;
      }
    }//*/
  }


  /**
   * MISSING_COMMENT
   *
   * @param other
   * @return
   */

  private int compareSingleDuration( ViewSequenceValue other ) {
    if( other == null || other.missedProgramSeconds == null ) {
      return better;
    }

    if( this.missedProgramSeconds == null &&
        other.missedProgramSeconds != null ){
      return worse;
    }

    // todo: compare the summation for now
    double thisValue = 0.0;
    for( int m : this.missedProgramSeconds.values() ){
      thisValue += m;
    }

    double otherValue = 0.0;
    for( int m : other.missedProgramSeconds.values() ){
      otherValue += m;
    }

    if( thisValue < otherValue ){
      return better;
    } else if( thisValue == otherValue ){
      return equal;
    } else {
      return worse;
    }
  }
}
