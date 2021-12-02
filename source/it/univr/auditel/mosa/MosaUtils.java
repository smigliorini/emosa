package it.univr.auditel.mosa;

import it.univr.auditel.entities.*;
import it.univr.auditel.shadoop.core.ViewSequenceValue;
import it.univr.auditel.shadoop.core.ViewSequenceWritable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static it.univr.auditel.entities.Utils.*;
import static java.lang.Math.*;
import static java.lang.Math.exp;
import static java.lang.Math.min;
import static java.util.Calendar.DATE;
import static org.apache.commons.lang.time.DateUtils.truncate;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class MosaUtils {

  // === Properties ============================================================

  private static int PREV_SITE = 0;
  private static int CHOSEN_SITE = 1;
  private static int NEXT_SITE = 2;

  // === Methods ===============================================================

  private MosaUtils(){
    // nothing here
  }

  // ===========================================================================

  /**
   * The method returns {@code true} if the group view sequence {@code a}
   * dominates the group view sequence {@code b} w.r.t. the predefined objective
   * functions.
   *
   * @param a
   * @param b
   * @param minDuration
   * @param maxDuration
   * @param preferenceMap
   * @return
   */

  public static boolean dominate
  ( ViewSequenceWritable a,
    ViewSequenceWritable b,
    int minDuration,
    int maxDuration,
    Map<String, List<UserPreference>> preferenceMap,
    Map<String, Map<String, Double>> genreSeqPreferenceMap,
    Map<GContext, Map<GContext, Double>> groupTypeEvolutionMap,
    Map<Long, Map<String, List<ProgramRecord>>> scheduling,
    boolean auditel,
    boolean dynamic ){

    if( a == null ){
      throw new NullPointerException();
    }
    if( b == null ){
      throw new NullPointerException();
    }
    if( preferenceMap == null ){
      throw new NullPointerException();
    }
    if( genreSeqPreferenceMap == null ){
      throw new NullPointerException();
    }
    if( groupTypeEvolutionMap == null ){
      throw new NullPointerException();
    }

    if( scheduling == null ){
      throw new NullPointerException();
    }

    final ViewSequenceValue va = new ViewSequenceValue
      ( a, preferenceMap, genreSeqPreferenceMap,
        groupTypeEvolutionMap, scheduling, auditel, dynamic );
    final ViewSequenceValue vb = new ViewSequenceValue
      ( b, preferenceMap, genreSeqPreferenceMap,
        groupTypeEvolutionMap, scheduling, auditel, dynamic );
    return va.dominate( vb, minDuration, maxDuration, dynamic );
  }


  /**
   * The method computes the points that forms the estimated Pareto-front using
   * the trips in the Pareto-set given as parameter.
   *
   * @param paretoSet
   * @param preferenceMap
   * @return
   */

  public static Set<ViewSequenceValue> computeParetoFront
  ( Set<ViewSequenceWritable> paretoSet,
    Map<String, List<UserPreference>> preferenceMap,
    Map<String, Map<String, Double>> genreSeqPreferenceMap,
    Map<GContext, Map<GContext, Double>> groupTypeEvolutionMap,
    Map<Long, Map<String, List<ProgramRecord>>> schedulingMap,
    boolean auditel,
    boolean dynamic ){

    if( paretoSet == null ){
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

    final Set<ViewSequenceValue> paretoFront = new HashSet<>( paretoSet.size() );
    for( ViewSequenceWritable t : paretoSet ){
      paretoFront.add( new ViewSequenceValue
        ( t, preferenceMap, genreSeqPreferenceMap,
          groupTypeEvolutionMap, schedulingMap, auditel, dynamic ) );
    }
    return paretoFront;
  }


  /**
   * The method returns the energy for the trip {@code trip} considering the
   * estimated Pareto-front computed using the given Pareto-set.
   *
   * @param sequence
   * @param paretoSet
   * @param minDuration
   * @param maxDuration
   * @param preferenceMap
   * @return
   */

  public static double energy
  ( ViewSequenceWritable sequence,
    Set<ViewSequenceWritable> paretoSet,
    int minDuration,
    int maxDuration,
    Map<String, List<UserPreference>> preferenceMap,
    Map<String, Map<String, Double>> genreSeqPreferenceMap,
    Map<GContext, Map<GContext, Double>> groupTypeEvolutionMap,
    Map<Long, Map<String, List<ProgramRecord>>> schedulingMap,
    boolean auditel,
    boolean dynamic ){

    if( sequence == null ){
      throw new NullPointerException();
    }
    if( paretoSet == null ){
      throw new NullPointerException();
    }
    if( preferenceMap == null ){
      throw new NullPointerException();
    }
    if( genreSeqPreferenceMap == null ){
      throw new NullPointerException();
    }
    if( groupTypeEvolutionMap == null ){
      throw new NullPointerException();
    }
    if( schedulingMap == null ){
      throw new NullPointerException();
    }

    final Set<ViewSequenceValue> paretoFront =
      computeParetoFront
        ( paretoSet, preferenceMap, genreSeqPreferenceMap,
          groupTypeEvolutionMap, schedulingMap, auditel, dynamic );
    return energy_
      ( sequence, paretoFront,
        minDuration, maxDuration,
        preferenceMap, genreSeqPreferenceMap,
        groupTypeEvolutionMap, schedulingMap, auditel, dynamic );
  }


  /**
   * The method returns the energy for the trip {@code trip} considering the
   * estimated Pareto-front given as parameter.
   *
   * @param sequence
   * @param paretoFront
   * @param minDuration
   * @param maxDuration
   * @param preferenceMap
   * @param schedulingMap
   * @return
   */

  private static double energy_
  ( ViewSequenceWritable sequence,
    Set<ViewSequenceValue> paretoFront,
    int minDuration,
    int maxDuration,
    Map<String, List<UserPreference>> preferenceMap,
    Map<String, Map<String, Double>> genreSeqPreferenceMap,
    Map<GContext, Map<GContext, Double>> groupTypeEvolutionMap,
    Map<Long, Map<String, List<ProgramRecord>>> schedulingMap,
    boolean auditel,
    boolean dynamic ){

    if( sequence == null ){
      throw new NullPointerException();
    }
    if( paretoFront == null ){
      throw new NullPointerException();
    }
    if( preferenceMap == null ){
      throw new NullPointerException();
    }
    if( genreSeqPreferenceMap == null ){
      throw new NullPointerException();
    }
    if( groupTypeEvolutionMap == null ){
      throw new NullPointerException();
    }
    if( schedulingMap == null ){
      throw new NullPointerException();
    }

    final ViewSequenceValue svalue =
      new ViewSequenceValue
        ( sequence, preferenceMap, genreSeqPreferenceMap,
          groupTypeEvolutionMap, schedulingMap, auditel, dynamic );

    int energy = 0;
    for( ViewSequenceValue d : paretoFront ){
      if( d.dominate( svalue, minDuration, maxDuration, dynamic ) ){
        energy++;
      }
    }
    return energy;
  }


  /**
   * The method returns the energy difference between the current solution
   * {@code currentSolution} and the new solution {@code newSolution} with
   * respect to the estimated Pareto-front {@code paretoFront}.
   *
   * @param currentSolution
   * @param newSolution
   * @param paretoSet
   * @param minDuration
   * @param maxDuration
   * @param preferenceMap
   * @param schedulingMap
   * @return
   */

  public static double energyDifference_
  ( ViewSequenceWritable currentSolution,
    ViewSequenceWritable newSolution,
    Set<ViewSequenceWritable> paretoSet,
    int minDuration,
    int maxDuration,
    Map<String, List<UserPreference>> preferenceMap,
    Map<String, Map<String, Double>> genreSeqPreferenceMap,
    Map<GContext, Map<GContext, Double>> groupTypeEvolutionMap,
    Map<Long, Map<String, List<ProgramRecord>>> schedulingMap,
    boolean auditel,
    boolean dynamic ){

    if( currentSolution == null ){
      throw new NullPointerException();
    }
    if( newSolution == null ){
      throw new NullPointerException();
    }
    if( paretoSet == null ){
      throw new NullPointerException();
    }
    if( preferenceMap == null ){
      throw new NullPointerException();
    }
    if( genreSeqPreferenceMap == null ){
      throw new NullPointerException();
    }
    if( groupTypeEvolutionMap == null ){
      throw new NullPointerException();
    }
    if( schedulingMap == null ){
      throw new NullPointerException();
    }

    final Set<ViewSequenceWritable> pSet = new HashSet<>( paretoSet.size() + 2 );
    pSet.addAll( paretoSet );
    pSet.add( currentSolution );
    pSet.add( newSolution );
    final Set<ViewSequenceValue> paretoFront =
      computeParetoFront
        ( pSet, preferenceMap, genreSeqPreferenceMap,
          groupTypeEvolutionMap, schedulingMap, auditel, dynamic );

    final double currEnergy = energy_
      ( currentSolution, paretoFront,
        minDuration, maxDuration,
        preferenceMap, genreSeqPreferenceMap,
        groupTypeEvolutionMap, schedulingMap, auditel, dynamic );
    final double newEnergy = energy_
      ( newSolution, paretoFront,
        minDuration, maxDuration,
        preferenceMap, genreSeqPreferenceMap,
        groupTypeEvolutionMap, schedulingMap, auditel, dynamic );

    final double energyDiff = (newEnergy - currEnergy) / paretoFront.size();
    return energyDiff;
  }


  /**
   * The method returns the energy difference between the current solution
   * {@code currentSolution} and the new solution {@code newSolution} with
   * respect to the estimated Pareto-front {@code paretoFront}.
   *
   * @param currentSolution
   * @param newSolution
   * @param paretoFront
   * @param minDuration
   * @param maxDuration
   * @param preferenceMap
   * @param schedulingMap
   * @return
   */

  public static double energyDifference
  ( ViewSequenceWritable currentSolution,
    ViewSequenceWritable newSolution,
    Set<ViewSequenceValue> paretoFront,
    int minDuration,
    int maxDuration,
    Map<String, List<UserPreference>> preferenceMap,
    Map<String, Map<String, Double>> genreSeqPreferenceMap,
    Map<GContext, Map<GContext, Double>> groupTypeEvolutionMap,
    Map<Long, Map<String, List<ProgramRecord>>> schedulingMap,
    boolean auditel,
    boolean dynamic ){

    if( currentSolution == null ){
      throw new NullPointerException();
    }
    if( newSolution == null ){
      throw new NullPointerException();
    }
    if( paretoFront == null ){
      throw new NullPointerException();
    }
    if( preferenceMap == null ){
      throw new NullPointerException();
    }
    if( genreSeqPreferenceMap == null ){
      throw new NullPointerException();
    }
    if( groupTypeEvolutionMap == null ){
      throw new NullPointerException();
    }
    if( schedulingMap == null ){
      throw new NullPointerException();
    }


    final Set<ViewSequenceWritable> pSet = new HashSet<>( 2 );
    pSet.add( currentSolution );
    pSet.add( newSolution );
    paretoFront.addAll
      ( computeParetoFront
        ( pSet, preferenceMap, genreSeqPreferenceMap,
          groupTypeEvolutionMap, schedulingMap, auditel, dynamic ) );

    final double currEnergy = energy_
      ( currentSolution, paretoFront,
        minDuration, maxDuration,
        preferenceMap, genreSeqPreferenceMap,
        groupTypeEvolutionMap, schedulingMap, auditel, dynamic );
    final double newEnergy = energy_
      ( newSolution, paretoFront,
        minDuration, maxDuration,
        preferenceMap, genreSeqPreferenceMap,
        groupTypeEvolutionMap, schedulingMap, auditel, dynamic );

    final double energyDiff = (newEnergy - currEnergy) / paretoFront.size();
    return energyDiff;
  }


  /**
   * The method computes the probability of acceptance of the new solution
   * {@code newSolution} in place of the current solution {@code
   * currentSolution}.
   *
   * @param currentSolution
   * @param newSolution
   * @param paretoSet
   * @param temperature
   * @param minDuration
   * @param maxDuration
   * @param preferenceMap
   * @param schedulingMap
   * @return
   */

  public static double acceptanceProbability_
  ( ViewSequenceWritable currentSolution,
    ViewSequenceWritable newSolution,
    Set<ViewSequenceWritable> paretoSet,
    double temperature,
    int minDuration,
    int maxDuration,
    Map<String, List<UserPreference>> preferenceMap,
    Map<String, Map<String, Double>> genreSeqPreferenceMap,
    Map<GContext, Map<GContext, Double>> groupTypeEvolutionMap,
    Map<Long, Map<String, List<ProgramRecord>>> schedulingMap,
    boolean auditel,
    boolean dynamic ){

    if( currentSolution == null ){
      throw new NullPointerException();
    }
    if( newSolution == null ){
      throw new NullPointerException();
    }
    if( paretoSet == null ){
      throw new NullPointerException();
    }
    if( preferenceMap == null ){
      throw new NullPointerException();
    }
    if( genreSeqPreferenceMap == null ){
      throw new NullPointerException();
    }
    if( groupTypeEvolutionMap == null ){
      throw new NullPointerException();
    }
    if( schedulingMap == null ){
      throw new NullPointerException();
    }

    double energyDifference = energyDifference_
      ( currentSolution, newSolution,
        paretoSet,
        minDuration, maxDuration,
        preferenceMap, genreSeqPreferenceMap,
        groupTypeEvolutionMap, schedulingMap, auditel, dynamic );
    return min( 1, exp( -energyDifference / temperature ) );
  }

  /**
   * The method computes the probability of acceptance of the new solution
   * {@code newSolution} in place of the current solution {@code
   * currentSolution}.
   *
   * @param currentSolution
   * @param newSolution
   * @param paretoFront
   * @param temperature
   * @param minDuration
   * @param maxDuration
   * @param preferenceMap
   * @param schedulingMap
   * @return
   */

  public static double acceptanceProbability
  ( ViewSequenceWritable currentSolution,
    ViewSequenceWritable newSolution,
    Set<ViewSequenceValue> paretoFront,
    double temperature,
    int minDuration,
    int maxDuration,
    Map<String, List<UserPreference>> preferenceMap,
    Map<String, Map<String, Double>> genreSeqPreferenceMap,
    Map<GContext, Map<GContext, Double>> groupTypeEvolutionMap,
    Map<Long, Map<String, List<ProgramRecord>>> schedulingMap,
    boolean auditel,
    boolean dynamic ){

    if( currentSolution == null ){
      throw new NullPointerException();
    }
    if( newSolution == null ){
      throw new NullPointerException();
    }
    if( paretoFront == null ){
      throw new NullPointerException();
    }
    if( preferenceMap == null ){
      throw new NullPointerException();
    }
    if( genreSeqPreferenceMap == null ){
      throw new NullPointerException();
    }
    if( groupTypeEvolutionMap == null ){
      throw new NullPointerException();
    }
    if( schedulingMap == null ){
      throw new NullPointerException();
    }

    // --- fix for duration ----------------------------------------------------
    if( currentSolution.getDuration() > maxDuration &&
      newSolution.getDuration() <= maxDuration ){
      return 1;
    }

    if( newSolution.getDuration() > maxDuration &&
      currentSolution.getDuration() <= maxDuration ){
      return 0;
    }

    // -------------------------------------------------------------------------

    double energyDifference = energyDifference
      ( currentSolution, newSolution, paretoFront,
        minDuration, maxDuration, preferenceMap, genreSeqPreferenceMap,
        groupTypeEvolutionMap, schedulingMap, auditel, dynamic );
    return min( 1, exp( -energyDifference / temperature ) );
  }


  /**
   * The method updates the Pareto-set {@code paretoSet} with the new solution
   * {@code newSol}: the new solution is added to the Pareto-set if it is not
   * dominated by any other solution in the set. Moreover, any solution in the
   * Pareto-set which is dominated by the new solution is discarded.
   *
   * @param paretoSet
   * @param newSol
   * @param minDuration
   * @param maxDuration
   * @param preferenceMap
   * @param schedulingMap
   */

  public static void updateParetoSet
  ( Set<ViewSequenceWritable> paretoSet,
    ViewSequenceWritable newSol,
    Integer minDuration,
    Integer maxDuration,
    Map<String, List<UserPreference>> preferenceMap,
    Map<String, Map<String, Double>> genreSeqPreferenceMap,
    Map<GContext, Map<GContext, Double>> groupTypeEvolutionMap,
    Map<Long, Map<String, List<ProgramRecord>>> schedulingMap,
    boolean auditel,
    boolean dynamic ){

    if( paretoSet == null ){
      throw new NullPointerException();
    }
    if( newSol == null ){
      throw new NullPointerException();
    }
    if( preferenceMap == null ){
      throw new NullPointerException();
    }
    if( genreSeqPreferenceMap == null ){
      throw new NullPointerException();
    }
    if( groupTypeEvolutionMap == null ){
      throw new NullPointerException();
    }
    if( schedulingMap == null ){
      throw new NullPointerException();
    }

    final ViewSequenceValue v =
      new ViewSequenceValue
        ( newSol, preferenceMap, genreSeqPreferenceMap,
          groupTypeEvolutionMap, schedulingMap, auditel, dynamic );
    final List<ViewSequenceWritable> dominatedSet = new ArrayList<>();
    boolean isDominated = false;

    for( ViewSequenceWritable t2 : paretoSet ){
      final ViewSequenceValue v2 =
        new ViewSequenceValue
          ( t2, preferenceMap, genreSeqPreferenceMap,
            groupTypeEvolutionMap, schedulingMap, auditel, dynamic );
      if( v.dominate( v2, minDuration, maxDuration, dynamic ) ){
        dominatedSet.add( t2 );
      }
      if( v2.dominate( v, minDuration, maxDuration, dynamic ) ){
        isDominated = true;
      }
    }

    if( dominatedSet.size() > 0 ){
      paretoSet.removeAll( dominatedSet );
    }
    if( !isDominated && !paretoSet.contains( newSol ) ){
      paretoSet.add( newSol );
    }
  }

  // ===========================================================================

  /**
   * The method perturbate the given trip {@code trip} by applying one of the
   * possible operations.
   *
   * @param sequence
   * @param scheduling
   * @param maxDuration
   * @param minDuration
   * @param generator
   * @return
   */

  public static ViewSequenceWritable perturbate
  ( ViewSequenceWritable sequence,
    Map<Long, Map<String, List<ProgramRecord>>> scheduling,
    Map<String, List<UserPreference>> preferenceMap,
    int maxDuration,
    int minDuration,
    Random generator,
    boolean auditel ){

    if( sequence == null ){
      throw new NullPointerException();
    }
    if( scheduling == null ){
      throw new NullPointerException();
    }
    if( preferenceMap == null ){
      throw new NullPointerException();
    }
    if( generator == null ){
      throw new NullPointerException();
    }

    if( sequence.size() == 1 ){
      // no remove or swap are possible!
      final int c = generator.nextInt( 2 );
      final ViewSequenceWritable ns;

      switch( c ){
        case 0:
          ns = addChannel( sequence, scheduling, generator, auditel );
          if( ns.size() == 0 ){
            System.out.printf( "[Warn]: Empty sequence generated.%n" );
          }
          return ns;
        case 1://*/
          ns = replaceChannel( sequence, scheduling, generator, auditel );
          if( ns.size() == 0 ){
            System.out.printf( "[Warn]: Empty sequence generated.%n" );
          }
          return ns;//*/
        default:
          System.out.printf( "[Warn]: No perturbation." );
          return sequence;
      }//*/
    } else if( sequence.size() > 1 ){
      final int c = generator.nextInt( 3 );
      final ViewSequenceWritable ns;

      switch( c ){
        case 0:
          ns = addChannel( sequence, scheduling, generator, auditel );
          if( ns.size() == 0 ){
            System.out.printf( "[Warn]: Empty sequence generated.%n" );
          }
          return ns;
        case 1:
          ns = removeChannel( sequence, scheduling, generator );
          if( ns.size() == 0 ){
            System.out.printf( "[Warn]: Empty sequence generated.%n" );
          }
          return ns;
        case 2://*/
          ns = replaceChannel( sequence, scheduling, generator, auditel );
          if( ns.size() == 0 ){
            System.out.printf( "[Warn]: Empty sequence generated.%n" );
          }
          return ns;
        default:
          System.out.printf( "[Warn]: No perturbation." );
          return sequence;
      }//*/
    } else{
      System.out.printf( "[Warn]: No perturbation empty sequence." );
      return sequence;
    }
  }


  // ===========================================================================

  /**
   * The method modifies the given view sequence {@code sequence} by replacing
   * one of its views with another available one.
   *
   * @param sequence
   * @param scheduling
   * @param generator
   * @param auditel
   * @return
   */

  public static ViewSequenceWritable replaceChannel
  ( ViewSequenceWritable sequence,
    Map<Long, Map<String, List<ProgramRecord>>> scheduling,
    Random generator,
    boolean auditel ){

    if( sequence == null && !sequence.isEmpty() ){
      throw new NullPointerException();
    }
    if( scheduling == null ){
      throw new NullPointerException();
    }
    if( generator == null ){
      throw new NullPointerException();
    }

    final int replaceIndex = generator.nextInt( sequence.size() );
    final GroupView oldView = sequence.getView( replaceIndex );

    // --- first part of the sequence ------------------------------------------
    final ViewSequenceWritable result = new ViewSequenceWritable();
    for( int i = 0; i < replaceIndex; i++ ){
      result.addView( new GroupView( sequence.getView( i ) ) );
    }

    final Long keyDate;
    final Date keyTimestamp;
    if( auditel ){
      keyTimestamp = sequence.getView( 0 ).getIntervalStart();
      keyDate = truncate( keyTimestamp, DATE ).getTime();
    } else{
      keyDate = new Long( 0 );
      keyTimestamp = null;
    }


    final Map<String, List<ProgramRecord>> channelMap = scheduling.get( keyDate );
    if( channelMap != null && channelMap.keySet().size() > 1 ){
      final List<String> channels = new ArrayList<>( channelMap.keySet() );
      // remove from the choices the current channel
      channels.remove( sequence.getView( replaceIndex ).getEpgChannelId() );
      final String newChannel = channels.get( generator.nextInt( channels.size() ) );

      final List<ProgramRecord> programs = channelMap.get( newChannel );

      final List<ProgramRecord> candidates = new ArrayList<>();
      if( auditel ){
        for( ProgramRecord curr : programs ){
          if( abs( curr.getStartTime().getTime() - keyTimestamp.getTime() ) <= delta ){
            candidates.add( curr );
          }
        }
      } else{
        for( ProgramRecord curr : programs ){
          candidates.add( curr );
        }
      }

      // --- second part of the sequence ---------------------------------------

      if( !candidates.isEmpty() ){
        Collections.sort( candidates, new Comparator<ProgramRecord>() {
          @Override
          public int compare( ProgramRecord o1, ProgramRecord o2 ){
            if( (o1 == null && o2 == null) ||
              (o1.getStartTime() == null && o2.getStartTime() == null) ){
              return 0;
            } else if( (o1 != null && o2 == null) ||
              (o1.getStartTime() != null && o2 == null) ){
              return 1;
            } else if( (o1 == null && o2 != null) ||
              (o1.getStartTime() == null && o2.getStartTime() != null) ){
              return -1;
            } else{
              return o1.getStartTime().compareTo( o2.getStartTime() );
            }
          }
        } );
        final ProgramRecord r;
        r = candidates.get( 0 );

        final GroupView v = new GroupView();
        v.setGroup( oldView.getGroup() );
        v.setEpgChannelId( r.getChannelId() );
        v.setProgramId( r.getProgramId() );

        // determine the start and end time based on the other views in the sequence
        final long start;

        if( auditel ){
          if( result.size() > 0 && replaceIndex > 0 ){
            start = max
              ( result.getView( replaceIndex - 1 ).getIntervalEnd().getTime(),
                r.getStartTime().getTime() );
          } else{
            start = r.getStartTime().getTime();
          }
        } else{
          if( result.size() > 0 && replaceIndex > 0 ){
            if( replaceIndex > 0 ){
              start = result.getView( replaceIndex - 1 )
                .getIntervalEnd().getTime();
            } else{
              start = result.getView( replaceIndex + 1 )
                .getIntervalStart().getTime() -
                r.getDuration() * 60 * 1000;
            }
          } else{
            start = new Date().getTime();
          }
        }

        final long end;
        if( auditel ){
          if( result.size() > 0 && replaceIndex < result.size() ){
            end = min
              ( result.getView( result.size() - 1 ).getIntervalStart().getTime(),
                r.getEndTime().getTime() );
          } else{
            end = r.getEndTime().getTime();
          }
        } else{
          if( result.size() > 0 && replaceIndex < result.size() ){
            if( replaceIndex > 0 ){
              end = result.getView( replaceIndex - 1 )
                .getIntervalStart().getTime() +
                r.getDuration() * 60 * 1000;
            } else{
              end = result.getView( replaceIndex - 1 )
                .getIntervalStart().getTime() - r.getDuration() * 60 * 1000;
            }
          } else{
            end = new Date().getTime() + r.getDuration() * 60 * 1000;
          }
        }

        v.setIntervalStart( new Date( start ) );
        v.setIntervalEnd( new Date( end ) );
        determineViewTimeSlot( v );

        result.addView( v );
      } else{
        result.addView( new GroupView( oldView ) );
      }

      for( int i = replaceIndex + 1; i < sequence.size(); i++ ){
        if( auditel ){
          final ProgramRecord r = findCandidateProgram
            ( sequence.getView( i ).getEpgChannelId(),
              result.getView( result.size() - 1 ).getIntervalEnd(),
              scheduling );
          if( r != null ){
            final GroupView v = new GroupView();
            v.setGroup( oldView.getGroup() );
            v.setEpgChannelId( r.getChannelId() );
            v.setProgramId( r.getProgramId() );
            v.setIntervalStart( new Date( r.getStartTime().getTime() ) );
            v.setIntervalEnd( new Date( r.getEndTime().getTime() ) );
            determineViewTimeSlot( v );
            result.addView( v );
          }
        } else{
          final GroupView prevView = sequence.getSequence().get( replaceIndex );

          final GroupView v = new GroupView();
          v.setGroup( oldView.getGroup() );
          v.setEpgChannelId( oldView.getEpgChannelId() );
          v.setProgramId( oldView.getProgramId() );
          v.setIntervalStart( prevView.getIntervalEnd() );
          v.setIntervalEnd( new Date( prevView.getIntervalEnd().getTime() +
            oldView.getIntervalEnd().getTime() -
            oldView.getIntervalStart().getTime() ) );
          determineViewTimeSlot( v );
          result.addView( v );
        }
      }

      if( result.getSequence().size() == 0 ){
        System.out.printf( "Empty sequence generated.%n" );
      }
      if( !checkSequence( result ) ){
        System.out.println( "An invalid sequence has been generated.%n" );
      }
      return result;

    } else{
      return sequence;
    }
  }

  private static boolean checkSequence( ViewSequenceWritable sequence ){
    if( sequence == null ){
      throw new NullPointerException();
    }

    boolean result = true;
    for( int i = 0; i < sequence.getSequence().size() - 1 && result; i++ ){
      result = result &&
        sequence.getSequence().get( i ).getIntervalEnd().getTime() <=
          sequence.getSequence().get( i + 1 ).getIntervalStart().getTime();
    }

    return result;
  }

  /**
   * The method modifies the given sequence {@code sequence} by adding a new
   * view not contained in the sequence.
   *
   * @param sequence
   * @param scheduling
   * @param generator
   * @param auditel
   * @return
   */

  public static ViewSequenceWritable addChannel
  ( ViewSequenceWritable sequence,
    Map<Long, Map<String, List<ProgramRecord>>> scheduling,
    Random generator,
    boolean auditel ){

    if( sequence == null && !sequence.isEmpty() ){
      throw new NullPointerException();
    }
    if( scheduling == null ){
      throw new NullPointerException();
    }
    if( generator == null ){
      throw new NullPointerException();
    }

    final int b = generator.nextInt( 1 );
    final int insertIndex;
    if( b == 0 ){
      insertIndex = 0;
    } else{
      insertIndex = sequence.getSequence().size();
    }

    // --- first part of the sequence ------------------------------------------
    final ViewSequenceWritable result = new ViewSequenceWritable();
    for( int i = 0; i < insertIndex; i++ ){
      result.addView( new GroupView( sequence.getView( i ) ) );
    }

    // --- central part of the sequence -----------------------------------------

    final Date keyTimestamp;
    final Long keyDate;
    if( auditel ){
      if( insertIndex == 0 ){
        keyTimestamp = sequence.getView( insertIndex ).getIntervalStart();
        keyDate = truncate( keyTimestamp, DATE ).getTime();
      } else{
        keyTimestamp = sequence.getView( insertIndex - 1 ).getIntervalEnd();
        keyDate = truncate( keyTimestamp, DATE ).getTime();
      }
    } else{
      keyDate = new Long( 0 );
      keyTimestamp = null;
    }

    final List<Long> l = new ArrayList<>( scheduling.keySet() );
    Collections.sort( l );
    final Map<String, List<ProgramRecord>> channelMap = scheduling.get( keyDate );
    if( channelMap != null && channelMap.keySet().size() > 1 ){
      final List<String> channels = new ArrayList<>( channelMap.keySet() );
      final String newChannel = channels.get( generator.nextInt( channels.size() ) );

      final List<ProgramRecord> programs = channelMap.get( newChannel );

      final List<ProgramRecord> candidates = new ArrayList<>();
      for( ProgramRecord curr : programs ){
        if( auditel ){
          if( insertIndex == 0 ){
            // check the end time
            if( curr.getEndTime().getTime() <= keyTimestamp.getTime()
            ){
              candidates.add( curr );
            }
          } else{
            // check the start time
            if( curr.getStartTime().getTime() >= keyTimestamp.getTime()
            ){
              candidates.add( curr );
            }
          }
        } else {
          candidates.add( curr );
        }
      }

      if( !candidates.isEmpty() ){
        Collections.sort( candidates, new Comparator<ProgramRecord>() {
          @Override
          public int compare( ProgramRecord o1, ProgramRecord o2 ){
            if( (o1 == null && o2 == null) ||
              (o1.getStartTime() == null && o2.getStartTime() == null) ){
              return 0;
            } else if( (o1 != null && o2 == null) ||
              (o1.getStartTime() != null && o2 == null) ){
              return 1;
            } else if( (o1 == null && o2 != null) ||
              (o1.getStartTime() == null && o2.getStartTime() != null) ){
              return -1;
            } else{
              return o1.getStartTime().compareTo( o2.getStartTime() );
            }
          }
        } );

        final ProgramRecord r;
        if( insertIndex == 0 ){
          r = candidates.get( candidates.size() - 1 );
        } else{
          r = candidates.get( 0 );
        }

        final GroupView v = new GroupView();
        v.setGroup( sequence.getView( 0 ).getGroup() );
        v.setEpgChannelId( r.getChannelId() );
        v.setProgramId( r.getProgramId() );

        // determine the start and end time based on the other views in the sequence
        final long start;
        if( auditel ){
          if( result.size() > 0 && insertIndex > 0 ){
            start = max
              ( result.getView( insertIndex - 1 ).getIntervalEnd().getTime(),
                r.getStartTime().getTime() );
          } else{
            start = r.getStartTime().getTime();
          }
        } else{
          if( result.size() > 0 && insertIndex > 0 ){
            if( insertIndex > 0 ){
              start = result.getView( insertIndex - 1 )
                .getIntervalEnd().getTime();
            } else{
              start = result.getView( insertIndex + 1 )
                .getIntervalStart().getTime() -
                r.getDuration() * 60 * 1000;
            }
          } else{
            start = new Date().getTime();
          }
        }


        final long end;
        if( auditel ){
          if( result.size() > 0 && insertIndex < result.size() ){
            end = min
              ( result.getView( result.size() - 1 ).getIntervalStart().getTime(),
                r.getEndTime().getTime() );
          } else{
            end = r.getEndTime().getTime();
          }
        }  else{
          if( result.size() > 0 && insertIndex < result.size() ){
            if( insertIndex > 0 ){
              end = result.getView( insertIndex - 1 )
                .getIntervalStart().getTime() +
                r.getDuration() * 60 * 1000;
            } else{
              end = result.getView( insertIndex - 1 )
                .getIntervalStart().getTime() - r.getDuration() * 60 * 1000;
            }
          } else{
            end = new Date().getTime() + r.getDuration() * 60 * 1000;
          }
        }

        v.setIntervalStart( new Date( start ) );
        v.setIntervalEnd( new Date( end ) );
        determineViewTimeSlot( v );

        result.addView( v );
      }

      // --- second part of the sequence ---------------------------------------


      for( int i = insertIndex; i < sequence.size(); i++ ){
        result.addView( new GroupView( sequence.getView( i ) ) );
      }
      if( result.getSequence().size() == 0 ){
        System.out.printf( "Empty sequence generated.%n" );
      }

      if( !checkSequence( result ) ){
        System.out.println( "An invalid sequence has been generated.%n" );
      }
      return result;

    } else{
      return sequence;
    }
  }

  /**
   * The method modifies the given view sequence {@code sequence} by removing
   * one of its views.
   *
   * @param sequence
   * @param schedulingMap
   * @param generator
   * @return
   */

  public static ViewSequenceWritable removeChannel
  ( ViewSequenceWritable sequence,
    Map<Long, Map<String, List<ProgramRecord>>> schedulingMap,
    Random generator ){

    if( sequence == null && !sequence.isEmpty() ){
      throw new NullPointerException();
    }
    if( schedulingMap == null ){
      throw new NullPointerException();
    }
    if( generator == null ){
      throw new NullPointerException();
    }

    if( sequence.size() == 1 ){
      return sequence;

    } else{
      final int removeIndex = generator.nextInt( sequence.size() );

      // --- first part of the sequence ------------------------------------------
      final ViewSequenceWritable result = new ViewSequenceWritable();

      for( int i = 0; i < removeIndex; i++ ){
        //for (int i = 0; i < removeIndex - 1; i++) {
        result.addView( new GroupView( sequence.getView( i ) ) );
      }

      // --- second part of the sequence ---------------------------------------

      // the other views remain unchanged
      for( int i = removeIndex + 1; i < sequence.size(); i++ ){
        //for (int i = removeIndex + 2; i < sequence.size(); i++) {
        result.addView( new GroupView( sequence.getView( i ) ) );
      }


      if( !checkSequence( result ) ){
        System.out.println( "An invalid sequence has been generated.%n" );
      }
      return result;
    }

  }


  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param currentSol
   * @param schedulingMap
   * @param preferenceMap
   * @param minDuration
   * @param maxDuration
   * @param paretoSet
   * @param paretoFront
   * @param generator
   * @param initialTemperature
   * @param finalTemperature
   * @param alpha
   * @param maxPerturbations
   * @return
   */

  public static Set<ViewSequenceWritable> performSa
  ( ViewSequenceWritable currentSol,
    Map<Long, Map<String, List<ProgramRecord>>> schedulingMap,
    Map<String, List<UserPreference>> preferenceMap,
    Map<String, Map<String, Double>> genreSeqPreferenceMap,
    Map<GContext, Map<GContext, Double>> groupTypeEvolutionMap,
    Integer minDuration,
    Integer maxDuration,
    Set<ViewSequenceWritable> paretoSet,
    Set<ViewSequenceValue> paretoFront,
    Random generator,
    double initialTemperature,
    double finalTemperature,
    double alpha,
    long maxPerturbations,
    boolean auditel,
    boolean dynamic ){

    if( currentSol == null ){
      throw new NullPointerException();
    }
    if( schedulingMap == null ){
      throw new NullPointerException();
    }
    if( preferenceMap == null ){
      throw new NullPointerException();
    }
    if( genreSeqPreferenceMap == null ){
      throw new NullPointerException();
    }
    if( paretoSet == null ){
      throw new NullPointerException();
    }
    if( paretoFront == null ){
      throw new NullPointerException();
    }
    if( generator == null ){
      throw new NullPointerException();
    }

    double temperature = initialTemperature;
    int c = 0;
    while( temperature > finalTemperature ){
      for( long j = 0; j < maxPerturbations; j++ ){
        if( !checkSequence( currentSol ) ){
          System.out.printf( "Negative duration.%n" );
        }

        final ViewSequenceWritable perturbSol =
          perturbate( currentSol,
            schedulingMap,
            preferenceMap,
            maxDuration,
            minDuration,
            generator,
            auditel );

        if( perturbSol.equals( currentSol ) ){
          continue;
        }

        if( perturbSol.getDuration() < 0 ){
          System.out.printf( "Negative duration!%n" );
        }

        if( perturbSol.getSequence().size() > 1 ){
          Long minOffset = Long.MAX_VALUE;
          Long maxOffset = Long.MIN_VALUE;

          for( int k = 0; k < perturbSol.getSequence().size() - 1; k++ ){
            final Long offset = perturbSol.getSequence().get( k + 1 ).getIntervalStart().getTime() -
              perturbSol.getSequence().get( k ).getIntervalEnd().getTime();
            minOffset = Math.min( minOffset, offset );
            maxOffset = Math.max( maxOffset, offset );
          }
          if( minOffset < 0 || maxOffset < 0 ){
            System.out.printf( "[LOG] Min offset in sequence: %s min. Max offset in sequence: %s min.%n",
              minOffset / 60 / 1000,
              maxOffset / 60 / 10000 );
          }
          if( minOffset < 0 ){
            System.out.printf( "[LOG] %s.%n", perturbSol.toString() );
          }
        }

        final double p = acceptanceProbability
          ( currentSol,
            perturbSol,
            paretoFront,
            temperature,
            minDuration,
            maxDuration,
            preferenceMap,
            genreSeqPreferenceMap,
            groupTypeEvolutionMap,
            schedulingMap,
            auditel,
            dynamic );

        final double u = generator.nextDouble();
        if( u < p ){
          // update the Pareto-set
          updateParetoSet
            ( paretoSet, perturbSol,
              minDuration, maxDuration,
              preferenceMap, genreSeqPreferenceMap,
              groupTypeEvolutionMap,
              schedulingMap,
              auditel,
              dynamic );

          // accept perturbSol in place of currentSol
          currentSol = perturbSol;
        }
      }

      c++;
      temperature = initialTemperature - alpha * c;
    }

    return paretoSet;
  }
}
