package it.univr.auditel.shadoop.core;

import it.univr.auditel.entities.GContext;
import it.univr.auditel.entities.GroupView;
import it.univr.auditel.entities.ProgramRecord;
import it.univr.auditel.entities.UserPreference;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import static it.univr.auditel.entities.Utils.findProgram;
import static java.lang.Math.*;
import static java.lang.Math.abs;

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

  private List<String> channelSequence;

  // Fd
  private int duration;
  // Fe
  private int missedSeconds;
  // Fh
  // Fs
  private double groupPreference;
  // Fm
  private double minMaxFairness;
  // Fj
  private double jainFairness;

  // === Methods ===============================================================


  /**
   * MISSING_COMMENT
   */

  public ViewSequenceValue(){
    duration = 0;
    channelSequence = new ArrayList<>();
    //userPreferences = new HashMap<>();
    //userWeights = new HashMap<>();
    //genreSeqPreferences = new ArrayList<>();
    //missedProgramSeconds = new HashMap<>();
  }

  public ViewSequenceValue( ViewSequenceValue other ){
    this.channelSequence = new ArrayList<>();
    for( String c : other.channelSequence ){
      this.channelSequence.add( c );
    }

    this.duration = other.duration;
    this.missedSeconds = other.missedSeconds;
    this.groupPreference = other.groupPreference;
    this.minMaxFairness = other.minMaxFairness;
    this.jainFairness = other.jainFairness;
  }

  /**
   * MISSING_COMMENT
   *
   * @param sequence
   * @param preferenceMap
   * @param genreSeqPreferenceMap
   * @param schedulingMap
   */

  public ViewSequenceValue
  ( ViewSequenceWritable sequence,
    Map<String, List<UserPreference>> preferenceMap,
    Map<String, Map<String, Double>> genreSeqPreferenceMap,
    Map<GContext, Map<GContext, Double>> groupTypeEvolutionMap,
    Map<Long, Map<String, List<ProgramRecord>>> schedulingMap,
    boolean auditel,
    boolean dynamic ){

    if( sequence == null ){
      throw new NullPointerException();
    }
    if( preferenceMap == null ){
      throw new NullPointerException();
    }
    if( genreSeqPreferenceMap == null ){
      throw new NullPointerException();
    }
    if( schedulingMap == null ){
      throw new NullPointerException();
    }
    if( groupTypeEvolutionMap == null ){
      throw new NullPointerException();
    }//*/

    if( !sequence.isEmpty() ){
      final GroupView start = sequence.getSequence().get( 0 );
      final GroupView end = sequence.getSequence().get( sequence.size() - 1 );
      duration = (int)
        ((end.getIntervalEnd().getTime() - start.getIntervalStart().getTime()) /
          (1000 * 60));

      // -- user weight => group evolution -------------------------------------
      final Map<String, Double> userWeights = new HashMap<>();
      if( dynamic ){
        // determine the possible evolutions
        final GContext gc = new GContext();
        gc.setAgeClassSet( end.getGroup().getTypeSet() );
        gc.setTimeSlot( end.getTimeSlot() );
        final Map<GContext, Double> evolutions = groupTypeEvolutionMap.get( gc );

        // user weight
        if( evolutions == null || !dynamic ){
          for( String u : end.getGroup().getUsers() ){
            userWeights.put( u, 1.0 / end.getGroup().getUsers().size() );
          }
        } else{
          for( Map.Entry<GContext, Double> e : evolutions.entrySet() ){
            for( String u : end.getGroup().getUsers() ){
              final String type = end.getGroup().getTypeByUser( u );
              if( e.getKey().getAgeClassSet().contains( type ) ){
                if( !userWeights.containsKey( u ) ){
                  userWeights.put( u, e.getValue() );
                } else{
                  userWeights.put( u, userWeights.get( u ) + e.getValue() );
                }
              }
            }
          }
        }//*/
      }

      // -----------------------------------------------------------------------

      final Calendar c = Calendar.getInstance();
      c.setTime( start.getIntervalStart() );
      final int dayOfWeek = c.get( Calendar.DAY_OF_WEEK );
      final String wdwe;
      if( dayOfWeek == Calendar.SATURDAY ||
        dayOfWeek == Calendar.SUNDAY ){
        wdwe = "weekend";
      } else{
        wdwe = "weekday";
      }

      // --- user preference ---------------------------------------------------

      channelSequence = new ArrayList<>();
      final Map<String, List<Double>> userPreferences = new HashMap<>();

      for( GroupView v : sequence.getSequence() ){
        channelSequence.add( v.getEpgChannelId() );

        for( String user : v.getGroup().getUsers() ){
          final Double userWeight = userWeights.get( user ) != null ?
            userWeights.get( user ) : 1.0 / v.getGroup().getUsers().size();

          if( preferenceMap.get( user ) != null ){
            final List<UserPreference> upl = preferenceMap.get( user );

            //Double maxPref = Double.MIN_VALUE;
            Double currPref = new Double( 0.001 );

            for( UserPreference up : upl ){
              if( dynamic ){
                if( up.getChannelId().equals( v.getEpgChannelId() ) &&
                  up.getTimeSlot().equals( v.getTimeSlot() ) &&
                  //up.getDayOfWeek().equals( wdwe )
                  up.getGroupTypeSet().containsAll( v.getGroup().getTypeSet() ) &&
                  v.getGroup().getTypeSet().containsAll( up.getGroupTypeSet() ) ){

                  currPref = up.getPreference();

                } else {
                  if( up.getChannelId().equals( v.getEpgChannelId() ) ){
                    currPref = max( currPref, up.getPreference() );
                  } else {
                    currPref = min( currPref, up.getPreference() );
                  }
                }
              } else{ // static no context
                if( up.getChannelId().equals( v.getEpgChannelId() ) ){
                  currPref = min( currPref, up.getPreference() );
                }
              }
            }

            List<Double> pl = userPreferences.get( user );
            if( pl == null ){
              pl = new ArrayList<>();
            }
            pl.add( currPref * userWeight );
            userPreferences.put( user, pl );
          }
        }

        final ProgramRecord r = findProgram( v, schedulingMap, auditel );
        if( r != null && auditel ){
          final int seconds =
            (int) ((v.getIntervalStart().getTime() -
              r.getStartTime().getTime()) / 1000 +
              (r.getEndTime().getTime() -
                v.getIntervalEnd().getTime()) / 1000);

          // missedProgramSeconds.put( r.getProgramId(), seconds );
          missedSeconds += max( 0, seconds );
        } else if( r != null ){
          final long seconds = r.getDuration() * 60 -
            ( v.getIntervalEnd().getTime() / 1000 -
            v.getIntervalStart().getTime() / 1000 );

          missedSeconds += max( 0, seconds );
        }
      }

      //List<Double> genreSeqPreferences = new ArrayList<>();
      for( int i = 0; i < channelSequence.size() - 1; i++ ){
        final String current = channelSequence.get( i );
        final String next = channelSequence.get( i + 1 );
        final Double genrePref;

        if( genreSeqPreferenceMap.get( current ) != null &&
          genreSeqPreferenceMap.get( current ).get( next ) != null ){
          genrePref = genreSeqPreferenceMap.get( current ).get( next );
        } else{
          genrePref = 0.0;
        }

        for( String u : userPreferences.keySet() ){
          final List<Double> p = userPreferences.get( u );
          p.set( i, p.get( i ) * genrePref );
        }
      }

      double minPref = Double.MAX_VALUE;
      double maxPref = Double.MIN_VALUE;
      double numFair = 0;
      double denFair = 0;

      for( String u : userPreferences.keySet() ){
        for( double d : userPreferences.get( u ) ){
          groupPreference += d;
          minPref = min( minPref, d );
          maxPref = max( maxPref, d );
          numFair += d;
          denFair += pow( d, 2 );
        }
      }

      minMaxFairness = minPref / maxPref;
      jainFairness = pow( numFair, 2 ) /
        ( userPreferences.keySet().size() * denFair );
    }
  }


  /**
   * Duration in minutes.
   *
   * @return
   */
  public long getDuration(){
    return duration;
  }

  public void setDuration( int duration ){
    this.duration = duration;
  }

  public List<String> getChannelSequence(){
    return channelSequence;
  }

  public void addChannel( String channel ){
    if( channelSequence == null ){
      channelSequence = new ArrayList<>();
    }
    channelSequence.add( channel );
  }

  public void setChannelSequence( List<String> channelSequence ){
    this.channelSequence = channelSequence;
  }

  /*public Map<String, List<Double>> getUserPreferences(){
    return userPreferences;
  }

  public void setUserPreferences( Map<String, List<Double>> userPreferences ){
    this.userPreferences = userPreferences;
  }//*/

  public int getMissedSeconds(){
    return missedSeconds;
  }

  public void setMissedSeconds( int missedSeconds ){
    this.missedSeconds = missedSeconds;
  }

  public double getGroupPreference(){
    return groupPreference;
  }

  public void setGroupPreference( double groupPreference ){
    this.groupPreference = groupPreference;
  }

  public double getMinMaxFairness(){
    return minMaxFairness;
  }

  public void setMinMaxFairness( double minMaxFairness ){
    this.minMaxFairness = minMaxFairness;
  }

  public double getJainFairness(){
    return jainFairness;
  }

  public void setJainFairness( double jainFairness ){
    this.jainFairness = jainFairness;
  }


  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param dataOutput
   * @throws IOException
   */

  @Override
  public void write( DataOutput dataOutput ) throws IOException{
    if( dataOutput == null ){
      throw new NullPointerException();
    }

    // Fd
    dataOutput.writeInt( duration );

    if( channelSequence != null && !channelSequence.isEmpty() ){
      dataOutput.writeInt( channelSequence.size() );
      for( String s : channelSequence ){
        dataOutput.writeUTF( s );
      }
    } else{
      dataOutput.writeInt( 0 );
    }

    /*if( userPreferences != null && !userPreferences.isEmpty() ){
      dataOutput.writeInt( userPreferences.size() );
      for( Map.Entry<String, Double> p : userPreferences.entrySet() ){
        dataOutput.writeUTF( p.getKey() );
        dataOutput.writeDouble( p.getValue() );
      }
    } else{
      dataOutput.writeInt( 0 );
    }

    if( missedProgramSeconds != null && !missedProgramSeconds.isEmpty() ){
      dataOutput.writeInt( missedProgramSeconds.size() );
      for( Map.Entry<String, Integer> m : missedProgramSeconds.entrySet() ){
        dataOutput.writeUTF( m.getKey() );
        dataOutput.writeInt( m.getValue() );
      }
    } else{
      dataOutput.writeInt( 0 );
    }//*/

    // Fe
    dataOutput.writeInt( missedSeconds );
    // Fh
    // Fs
    dataOutput.writeDouble( groupPreference );
    // Fm
    dataOutput.writeDouble( minMaxFairness );
    // Fj
    dataOutput.writeDouble( jainFairness );
  }

  /**
   * MISSING_COMMENT
   *
   * @param dataInput
   * @throws IOException
   */

  @Override
  public void readFields( DataInput dataInput ) throws IOException{
    if( dataInput == null ){
      throw new NullPointerException();
    }

    duration = dataInput.readInt();

    final int size = dataInput.readInt();
    channelSequence = new ArrayList<>();
    for( int i = 0; i < size; i++ ){
      channelSequence.add( dataInput.readUTF() );
    }

    missedSeconds = dataInput.readInt();
    groupPreference = dataInput.readDouble();
    minMaxFairness = dataInput.readDouble();
    jainFairness = dataInput.readDouble();

    /*final int pSize = dataInput.readInt();
    userPreferences = new HashMap<>();
    for( int i = 0; i < pSize; i++ ){
      userPreferences.put( dataInput.readUTF(), dataInput.readDouble() );
    }

    final int mSize = dataInput.readInt();
    missedProgramSeconds = new HashMap<>();
    for( int i = 0; i < mSize; i++ ){
      missedProgramSeconds.put( dataInput.readUTF(), dataInput.readInt() );
    }//*/
  }


  /**
   * MISSING_COMMENT
   *
   * @return
   */

  @Override
  public String toString(){
    final StringBuilder b = new StringBuilder();
    b.append( duration );
    b.append( "\t" );

    if( channelSequence != null && !channelSequence.isEmpty() ){
      final Iterator<String> it = channelSequence.iterator();
      while( it.hasNext() ){
        b.append( it.next() );
        if( it.hasNext() ){
          b.append( "," );
        }
      }
    }
    b.append( "\t" );

    b.append( missedSeconds );
    b.append( "\t" );

    b.append( groupPreference );
    b.append( "\t" );

    b.append( minMaxFairness );
    b.append( "\t" );

    b.append( jainFairness );
    b.append( "\t" );

    return b.toString();
  }

  // ===========================================================================

  @Override
  public boolean equals( Object o ){
    if( this == o ) return true;
    if( o == null || getClass() != o.getClass() ) return false;
    ViewSequenceValue that = (ViewSequenceValue) o;
    return duration == that.duration &&
      Integer.compare( that.missedSeconds, missedSeconds ) == 0 &&
      Double.compare( that.groupPreference, groupPreference ) == 0 &&
      Double.compare( that.minMaxFairness, minMaxFairness ) == 0 &&
      Double.compare( that.jainFairness, jainFairness ) == 0 &&
      Objects.equals( channelSequence, that.channelSequence );
  }

  @Override
  public int hashCode(){
    return Objects.hash
      ( channelSequence,
        duration,
        missedSeconds,
        groupPreference,
        minMaxFairness,
        jainFairness );
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
    int maxDuration,
    boolean dynamic ){

    if( other == null ){
      return true;
    }

    if( this.equals( other ) ){
      return false;
    }

    int[] objValues = new int[ 6 ];
    for( int i = 0; i < 6; i++ ){
      objValues[ i ] = 0;
    }

    if( dynamic ){

      // Fd
      objValues[ 0 ] = compareDuration( other, minDuration, maxDuration );
      // Fe
      objValues[ 1 ] = compareSingleDuration( other );
      // Fh
      objValues[ 2 ] = compareHistory( other );

      // Fs
      objValues[ 3 ] = comparePreference( other );
      // Fm
      objValues[ 4 ] = compareMinMaxFairness( other );
      // Fj
      objValues[ 5 ] = compareJainFairness( other );

    } else{ // baseline method
      // Fs
      objValues[ 3 ] = comparePreference( other );
      // Fm
      objValues[ 4 ] = compareMinMaxFairness( other );
      // Fj
      objValues[ 5 ] = compareJainFairness( other );
    }


    boolean e = true;
    boolean b = false;
    for( int i = 0; i < objValues.length; i++ ){
      e = e && objValues[ i ] >= equal;
      b = b || objValues[ i ] == better;
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
    int maxDuration ){

    if( other == null ){
      throw new NullPointerException();
    }

    if( other == null ){
      throw new NullPointerException();
    }

    final int aDuration = this.duration;
    final int bDuration = this.duration;


    if( aDuration >= minDuration && aDuration <= maxDuration &&
      (bDuration < minDuration || bDuration > maxDuration) ){
      return better;

    } else if( bDuration >= minDuration && bDuration <= maxDuration &&
      (aDuration < minDuration || aDuration > maxDuration) ){
      return worse;

    } else if( (aDuration < minDuration || aDuration > maxDuration) &&
      (bDuration < minDuration || bDuration > maxDuration) ){

      if( aDuration < minDuration && bDuration < minDuration ){
        final int aDifference = minDuration - aDuration;
        final int bDifference = minDuration - bDuration;
        final int compare = aDifference - bDifference;

        if( compare < 0 && abs( compare ) > deltaTime ){
          return better;
        } else if( abs( compare ) <= deltaTime ){
          return equal;
        } else{ // compare > 0 && abs( compare ) > timeUnit
          return worse;
        }

      } else if( aDuration > maxDuration && bDuration > maxDuration ){
        final int aDifference = (aDuration - maxDuration);
        final int bDifference = (bDuration - maxDuration);
        final int compare = aDifference - bDifference;

        if( compare < 0 && abs( compare ) > deltaTime ){
          return better;
        } else if( abs( compare ) <= deltaTime ){
          return equal;
        } else{ // compare > 0 && abs( compare ) > timeUnit
          return worse;
        }

      } else{
        final int aDifference = abs( aDuration - maxDuration );
        final int bDifference = abs( bDuration - maxDuration );
        final int compare = aDifference - bDifference;

        if( compare < 0 && abs( compare ) > deltaTime ){
          return better;
        } else if( abs( compare ) <= deltaTime ){
          return equal;
        } else{ // compare > 0 && abs( compare ) > timeUnit
          return worse;
        }
      }

    } else{
      final int aDifference = abs( aDuration - maxDuration );
      final int bDifference = abs( bDuration - maxDuration );
      final int compare = aDifference - bDifference;

      if( compare < 0 && abs( compare ) > deltaTime ){
        return better;
      } else if( abs( compare ) <= deltaTime ){
        return equal;
      } else{ // compare > 0 && abs( compare ) > timeUnit
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

  private int compareHistory( ViewSequenceValue other ){
    return equal;
  }


  /**
   * MISSING_COMMENT
   *
   * @param other
   * @return
   */

  private int comparePreference( ViewSequenceValue other ){

    if( this.groupPreference == other.groupPreference ){
      return equal;
    } else if( this.groupPreference > other.groupPreference ){
      return better;
    } else{
      return equal;
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param other
   * @return
   */

  private int compareMinMaxFairness( ViewSequenceValue other ){
    if( this.minMaxFairness == this.minMaxFairness ){
      return equal;
    } else if( this.minMaxFairness > other.minMaxFairness ){
      return better;
    } else{
      return worse;
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param other
   * @return
   */

  private int compareJainFairness( ViewSequenceValue other ){
    if( this.jainFairness == other.jainFairness ){
      return equal;
    } else if( this.jainFairness > other.jainFairness ){
      return better;
    } else{
      return worse;
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @param other
   * @return
   */

  private int compareSingleDuration( ViewSequenceValue other ){
    if( this.missedSeconds < other.missedSeconds ){
      return better;
    } else if( this.missedSeconds == other.missedSeconds ){
      return equal;
    } else{
      return worse;
    }
  }
}
