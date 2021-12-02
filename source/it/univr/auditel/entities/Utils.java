package it.univr.auditel.entities;

import org.apache.commons.lang.time.DateUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.util.Calendar.DATE;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class Utils {

  public static long delta = 5 * 60 * 1000;

  private Utils(){
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param userAgeClasses
   * @param group
   */

  public static void determineGroupType
  ( Map<String, String> userAgeClasses,
    Group group ){

    if( userAgeClasses == null ){
      throw new NullPointerException();
    }
    if( group == null ){
      throw new NullPointerException();
    }

    for( String u : group.getUsers() ){
      final String c = userAgeClasses.get( u );
      if( c != null ){
        group.addType( u, c );
      }
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param view
   */

  public static void determineViewTimeSlot( GroupView view ){
    if( view == null ){
      throw new NullPointerException();
    }

    /*
     * '00:00:00','02:00:00','LateFringe'
     * '02:00:00','07:00:00','GraveyardSlot'
     * '07:00:00','09:00:00','EarlyMorning'
     * '09:00:00','12:00:00','Morning'
     * '12:00:00','15:00:00','DayTime'
     * '15:00:00','18:00:00','EarlyFringe'
     * '18:00:00','20:30:00','PrimeAccess'
     * '20:30:00','22:30:00','PrimeTime'
     * '22:30:00','23:59:59','LateFringe'
     */

    final Calendar cal = new GregorianCalendar();
    cal.setTime( view.getIntervalStart() );
    final int dow = cal.get( Calendar.DAY_OF_WEEK );
    if( dow >= Calendar.MONDAY && dow <= Calendar.FRIDAY ){

    }
    final int hod = cal.get( Calendar.HOUR_OF_DAY );
    final int mod = cal.get( Calendar.MINUTE );

    if( hod >= 0 && hod < 2 ){
      view.setTimeSlot( "LateFringe" );
    } else if( hod >= 2 && hod < 7 ){
      view.setTimeSlot( "GraveyardSlot" );
    } else if( hod >= 7 && hod < 9 ){
      view.setTimeSlot( "EarlyMorning" );
    } else if( hod >= 9 && hod < 12 ){
      view.setTimeSlot( "Morning" );
    } else if( hod >= 12 && hod < 15 ){
      view.setTimeSlot( "DayTime" );
    } else if( hod >= 15 && hod < 18 ){
      view.setTimeSlot( "EarlyFringe" );
    } else if( hod >= 18 && hod < 20 ){
      view.setTimeSlot( "PrimeAccess" );
    } else if( hod >= 20 && hod < 22 ){
      if( hod == 20 && mod < 30 ){
        view.setTimeSlot( "PrimeAccess" );
      } else{
        view.setTimeSlot( "PrimeTime" );
      }
    } else if( hod >= 22 ){
      if( hod == 22 && mod < 30 ){
        view.setTimeSlot( "PrimeTime" );
      } else{
        view.setTimeSlot( "LateFringe" );
      }
    }

    view.getGroup().setTimeSlot( view.getTimeSlot() );
  }

  // ===========================================================================

  /**
   * The method returns the program corresponding to the group view {@code
   * view}.
   *
   * @param view
   * @param schedulingMap
   * @param auditel
   * @return
   */

  public static ProgramRecord findProgram
  ( GroupView view,
    Map<Long, Map<String, List<ProgramRecord>>> schedulingMap,
    boolean auditel ){

    if( view == null ){
      throw new NullPointerException();
    }
    if( schedulingMap == null ){
      throw new NullPointerException();
    }

    ProgramRecord result = null;

    if( ! auditel ){
      result = new ProgramRecord();
      result.setProgramId( view.getProgramId() );
      result.setChannelId( view.getEpgChannelId() );

      final List<ProgramRecord> channelScheduling =
        schedulingMap.get( new Long( 0 ) ).get( view.getEpgChannelId() );
      if( channelScheduling != null ){
        final Iterator<ProgramRecord> it = channelScheduling.iterator();
        while( it.hasNext() && result == null ){
          final ProgramRecord r = it.next();
          if( r.getProgramId().equals( view.getProgramId() ) ){
            result.setDuration( r.getDuration() );
          }
        }
      }

      if( result.getDuration() == null ){
        result.setDuration
          ( (view.getIntervalEnd().getTime() -
            view.getIntervalStart().getTime()) / (1000 * 60) );
      }

    } else{ // auditel

      final Long key = DateUtils.truncate( view.getIntervalStart(), DATE ).getTime();
      final Map<String, List<ProgramRecord>> daySchedule = schedulingMap.get( key );

      if( daySchedule != null ){
        final List<ProgramRecord> channelScheduling =
          daySchedule.get( view.getEpgChannelId() );

        if( channelScheduling != null ){
          final Iterator<ProgramRecord> it = channelScheduling.iterator();
          while( it.hasNext() && result == null ){
            final ProgramRecord r = it.next();
            if( r.getProgramId().equals( view.getProgramId() ) ){

              if( auditel && temporalIntersect
                ( view.getIntervalStart(), view.getIntervalEnd(),
                  r.getStartTime(), r.getEndTime() ) ){
                result = r;

              }
            }
          }

        } else{
          final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd" );
          System.out.printf
            ( "[Warn]: schedule not found for channel \"%s\" in day: \"%s\".%n",
              view.getEpgChannelId(), f.format( key ) );
        }
      } else{
        final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd" );
        System.out.printf
          ( "[Warn]: schedule not found in day: \"%s\".%n",
            f.format( key ) );

      }
    }

    return result;
  }


  /**
   * The method returns the first program of the channel with the right start
   * time.
   *
   * @param channel
   * @param startTime
   * @param schedulingMap
   * @return
   */

  public static ProgramRecord findCandidateProgram
  ( String channel,
    Date startTime,
    Map<Long, Map<String, List<ProgramRecord>>> schedulingMap ){

    if( channel == null ){
      throw new NullPointerException();
    }
    if( startTime == null ){
      throw new NullPointerException();
    }
    if( schedulingMap == null ){
      throw new NullPointerException();
    }

    List<ProgramRecord> candidates = new ArrayList<>();
    final Long key = DateUtils.truncate( startTime, DATE ).getTime();
    List<ProgramRecord> channelScheduling = getChannelDaySchedule( channel, key, schedulingMap );
    candidates = searchCandidateProgram( channelScheduling, startTime );

    /*final Long key = DateUtils.truncate( startTime, DATE ).getTime();
    final Map<String, List<ProgramRecord>> daySchedule = schedulingMap.get( key );
    if( daySchedule != null ) {
      final List<ProgramRecord> channelScheduling = daySchedule.get( channel );

      if( channelScheduling != null ) {
        final Iterator<ProgramRecord> it = channelScheduling.iterator();
        while( it.hasNext() ) {
          final ProgramRecord r = it.next();

          // if( r.getStartTime().getTime() - startTime.getTime() >= delta ) {
          // if( r.getStartTime().getTime() - startTime.getTime() <= delta ) {
          if( r.getStartTime().getTime() <= startTime.getTime() &&
              r.getEndTime().getTime() > startTime.getTime() ) {
            candidates.add( r );
          }
        }
      /*} else {
        final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd" );
        System.out.printf
          ( "[Warn]: schedule not found for channel \"%s\" in day: \"%s\".%n",
            channel, f.format( key ) );
      }
    } else {
      final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd" );
      System.out.printf
        ( "[Warn]: schedule not found in day: \"%s\".%n",
          f.format( key ) );
    }//*/

    final Calendar midDay = Calendar.getInstance();
    midDay.setTime( startTime );
    midDay.set( Calendar.HOUR_OF_DAY, 12 );
    midDay.set( Calendar.MINUTE, 0 );
    midDay.set( Calendar.SECOND, 0 );

    if( candidates.isEmpty() &&
      (startTime.before( midDay.getTime() ) ||
        startTime.equals( midDay.getTime() )) ){

      final Calendar previous = Calendar.getInstance();
      previous.setTime( startTime );
      previous.add( DATE, -1 );
      final Long pkey = DateUtils.truncate( previous.getTime(), DATE ).getTime();

      channelScheduling = getChannelDaySchedule( channel, pkey, schedulingMap );
      candidates = searchCandidateProgram( channelScheduling, startTime );

    } else if( candidates.isEmpty() && startTime.after( midDay.getTime() ) ){
      final Calendar next = Calendar.getInstance();
      next.setTime( startTime );
      next.add( DATE, 1 );
      final Long nkey = DateUtils.truncate( next.getTime(), DATE ).getTime();

      channelScheduling = getChannelDaySchedule( channel, nkey, schedulingMap );
      candidates = searchCandidateProgram( channelScheduling, startTime );
    }

    if( !candidates.isEmpty() ){
      Collections.sort( candidates, new Comparator<ProgramRecord>() {
        @Override
        public int compare( ProgramRecord o1, ProgramRecord o2 ){
          if( (o1 == null && o2 == null) ||
            (o1.getStartTime() == null && o2.getStartTime() == null) ){
            return 0;
          } else if( (o1 != null && o2 == null) ||
            (o1.getStartTime() != null && o2.getStartTime() == null) ){
            return 1;
          } else if( (o1 == null && o2 != null) ||
            (o1.getStartTime() == null && o2.getStartTime() != null) ){
            return -1;
          } else{
            return o1.getStartTime().compareTo( o2.getStartTime() );
          }
        }
      } );

      return candidates.get( candidates.size() - 1 );

    } else{
      final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd" );
      System.out.printf
        ( "[Warn]: no candidate program found for day: \"%s\" in channel %s.%n",
          f.format( key ), channel );
      return null;
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param channel
   * @param key
   * @param schedulingMap
   * @return
   */

  private static List<ProgramRecord> getChannelDaySchedule
  ( String channel,
    Long key,
    Map<Long, Map<String, List<ProgramRecord>>> schedulingMap ){

    if( channel == null ){
      throw new NullPointerException();
    }
    if( key == null ){
      throw new NullPointerException();
    }
    if( schedulingMap == null ){
      throw new NullPointerException();
    }

    final Map<String, List<ProgramRecord>> daySchedule = schedulingMap.get( key );
    if( daySchedule != null ){
      final List<ProgramRecord> channelScheduling = daySchedule.get( channel );

      if( channelScheduling != null ){
        return channelScheduling;

      } else{
        final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd" );
        System.out.printf
          ( "[Warn]: schedule not found for channel \"%s\" in day: \"%s\".%n",
            channel, f.format( key ) );
        return null;
      }
    } else{
      final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd" );
      System.out.printf
        ( "[Warn]: schedule not found in day: \"%s\".%n",
          f.format( key ) );
      return null;
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param channelScheduling
   * @param startTime
   * @return
   */

  private static List<ProgramRecord> searchCandidateProgram
  ( List<ProgramRecord> channelScheduling,
    Date startTime ){

    if( channelScheduling == null ){
      throw new NullPointerException();
    }
    if( startTime == null ){
      throw new NullPointerException();
    }

    final List<ProgramRecord> candidates = new ArrayList<>();

    final Iterator<ProgramRecord> it = channelScheduling.iterator();
    while( it.hasNext() ){
      final ProgramRecord r = it.next();

      // if( r.getStartTime().getTime() - startTime.getTime() >= delta ) {
      //if(  Math.abs( r.getStartTime().getTime() - startTime.getTime() ) <= delta ) {
      if( r.getStartTime().getTime() <= startTime.getTime() &&
        r.getEndTime().getTime() > startTime.getTime() ){
        candidates.add( r );
      }
    }

    return candidates;
  }

  // ===========================================================================

  /**
   * The method determines if two temporal intervals defined by <code>(start1,
   * end1)</code> and <code>(start2, end2)</code>, respectively, have a not
   * empty intersection.
   *
   * @param start1
   * @param end1
   * @param start2
   * @param end2
   * @return
   */

  public static boolean temporalIntersect
  ( Date start1,
    Date end1,
    Date start2,
    Date end2 ){

    if( start1 == null ){
      throw new NullPointerException();
    }
    if( end1 == null ){
      throw new NullPointerException();
    }
    if( start2 == null ){
      throw new NullPointerException();
    }
    if( end2 == null ){
      throw new NullPointerException();
    }

    if( start1.getTime() <= end2.getTime() &&
      end1.getTime() >= start2.getTime() ){
      return true;
    } else{
      return false;
    }
  }
}
