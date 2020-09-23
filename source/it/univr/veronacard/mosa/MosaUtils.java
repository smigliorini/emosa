package it.univr.veronacard.mosa;

import it.univr.veronacard.shadoop.core.Step;
import it.univr.veronacard.shadoop.core.TripValue;
import it.univr.veronacard.shadoop.core.TripWritable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static it.univr.veronacard.entities.ScenicRoutes.getScenicSites;
import static it.univr.veronacard.entities.ScenicRoutes.isScenicSite;
import static java.lang.Math.exp;
import static java.lang.Math.min;

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

  private MosaUtils() {
    // nothing here
  }

  // ===========================================================================

  /**
   * The method returns {@code true} if the trip {@code a} dominates the trip
   * {@code b} w.r.t. the predefined objective functions.
   *
   * @param a
   * @param b
   * @param maxDuration
   * @return
   */

  public static boolean dominate
  ( TripWritable a,
    TripWritable b,
    int minDuration,
    int maxDuration,
    String travelMode ) {

    if( a == null ) {
      throw new NullPointerException();
    }
    if( b == null ) {
      throw new NullPointerException();
    }

    final TripValue va = new TripValue( a );
    final TripValue vb = new TripValue( b );
    return va.dominate( vb, minDuration, maxDuration, travelMode );
  }


  /**
   * The method computes the points that forms the estimated Pareto-front using
   * the trips in the Pareto-set given as parameter.
   *
   * @param paretoSet
   * @return
   */

  public static Set<TripValue> computeParetoFront
  ( Set<TripWritable> paretoSet ) {

    if( paretoSet == null ) {
      throw new NullPointerException();
    }

    final Set<TripValue> paretoFront = new HashSet<>( paretoSet.size() );
    for( TripWritable t : paretoSet ) {
      paretoFront.add( new TripValue( t ) );
    }
    return paretoFront;
  }


  /**
   * The method returns the energy for the trip {@code trip} considering the
   * estimated Pareto-front computed using the given Pareto-set.
   *
   * @param trip
   * @param paretoSet
   * @param minDuration
   * @param maxDuration
   * @param travelMode
   * @return
   */

  public static double energy
  ( TripWritable trip,
    Set<TripWritable> paretoSet,
    int minDuration,
    int maxDuration,
    String travelMode ) {

    if( trip == null ) {
      throw new NullPointerException();
    }
    if( paretoSet == null ) {
      throw new NullPointerException();
    }

    final Set<TripValue> paretoFront = computeParetoFront( paretoSet );
    return energy_( trip, paretoFront, minDuration, maxDuration, travelMode );
  }

  /**
   * The method returns the energy for the trip {@code trip} considering the
   * estimated Pareto-front given as parameter.
   *
   * @param trip
   * @param paretoFront
   * @param maxDuration
   * @return
   */

  private static double energy_
  ( TripWritable trip,
    Set<TripValue> paretoFront,
    int minDuration,
    int maxDuration,
    String travelMode ) {

    if( trip == null ) {
      throw new NullPointerException();
    }
    if( paretoFront == null ) {
      throw new NullPointerException();
    }
    if( travelMode == null ) {
      throw new NullPointerException();
    }

    final TripValue tvalue = new TripValue( trip );

    int energy = 0;
    for( TripValue d : paretoFront ) {
      if( d.dominate( tvalue, minDuration, maxDuration, travelMode ) ) {
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
   * @param travelMode
   * @return
   */

  public static double energyDifference_
  ( TripWritable currentSolution,
    TripWritable newSolution,
    Set<TripWritable> paretoSet,
    int minDuration,
    int maxDuration,
    String travelMode ) {

    if( currentSolution == null ) {
      throw new NullPointerException();
    }
    if( newSolution == null ) {
      throw new NullPointerException();
    }
    if( paretoSet == null ) {
      throw new NullPointerException();
    }
    if( travelMode == null ) {
      throw new NullPointerException();
    }

    final Set<TripWritable> pSet = new HashSet<>( paretoSet.size() + 2 );
    pSet.addAll( paretoSet );
    pSet.add( currentSolution );
    pSet.add( newSolution );
    final Set<TripValue> paretoFront = computeParetoFront( pSet );

    final double currEnergy = energy_
      ( currentSolution, paretoFront, minDuration, maxDuration, travelMode );
    final double newEnergy = energy_
      ( newSolution, paretoFront, minDuration, maxDuration, travelMode );

    final double energyDiff = ( newEnergy - currEnergy ) / paretoFront.size();
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
   * @return
   */

  public static double energyDifference
  ( TripWritable currentSolution,
    TripWritable newSolution,
    Set<TripValue> paretoFront,
    int minDuration,
    int maxDuration,
    String travelMode ) {

    if( currentSolution == null ) {
      throw new NullPointerException();
    }
    if( newSolution == null ) {
      throw new NullPointerException();
    }
    if( paretoFront == null ) {
      throw new NullPointerException();
    }
    if( travelMode == null ) {
      throw new NullPointerException();
    }


    final Set<TripWritable> pSet = new HashSet<>( 2 );
    pSet.add( currentSolution );
    pSet.add( newSolution );
    paretoFront.addAll( computeParetoFront( pSet ) );

    final double currEnergy = energy_
      ( currentSolution, paretoFront, minDuration, maxDuration, travelMode );
    final double newEnergy = energy_
      ( newSolution, paretoFront, minDuration, maxDuration, travelMode );

    final double energyDiff = ( newEnergy - currEnergy ) / paretoFront.size();
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
   * @param travelMode
   * @return
   */

  public static double acceptanceProbability_
  ( TripWritable currentSolution,
    TripWritable newSolution,
    Set<TripWritable> paretoSet,
    double temperature,
    int minDuration,
    int maxDuration,
    String travelMode ) {

    if( currentSolution == null ) {
      throw new NullPointerException();
    }
    if( newSolution == null ) {
      throw new NullPointerException();
    }
    if( paretoSet == null ) {
      throw new NullPointerException();
    }

    double energyDifference = energyDifference_
      ( currentSolution, newSolution, paretoSet,
        minDuration, maxDuration, travelMode );
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
   * @param travelMode
   * @return
   */

  public static double acceptanceProbability
  ( TripWritable currentSolution,
    TripWritable newSolution,
    Set<TripValue> paretoFront,
    double temperature,
    int minDuration,
    int maxDuration,
    String travelMode ) {

    if( currentSolution == null ) {
      throw new NullPointerException();
    }
    if( newSolution == null ) {
      throw new NullPointerException();
    }
    if( paretoFront == null ) {
      throw new NullPointerException();
    }

    // --- fix for duration ----------------------------------------------------
    if( currentSolution.getDuration() > maxDuration &&
        newSolution.getDuration() <= maxDuration ) {
      return 1;
    }

    if( newSolution.getDuration() > maxDuration &&
        currentSolution.getDuration() <= maxDuration ) {
      return 0;
    }

    // -------------------------------------------------------------------------

    double energyDifference = energyDifference
      ( currentSolution, newSolution, paretoFront,
        minDuration, maxDuration, travelMode );
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
   */

  public static void updateParetoSet
  ( Set<TripWritable> paretoSet,
    TripWritable newSol,
    //VcProfileEnum profile,
    Integer minDuration,
    Integer maxDuration ) {

    if( paretoSet == null ) {
      throw new NullPointerException();
    }
    if( newSol == null ) {
      throw new NullPointerException();
    }
    /*if( profile == null ) {
      throw new NullPointerException();
    }//*/

    //if( profile == getProfile( newSol.getProfile() )) {
    final TripValue v = new TripValue( newSol );
    final List<TripWritable> dominatedSet = new ArrayList<>();
    boolean isDominated = false;

    for( TripWritable t2 : paretoSet ) {
      final TripValue v2 = new TripValue( t2 );
      if( v.dominate( v2,
                      minDuration,
                      maxDuration,
                      //getMinDuration( profile ),
                      //getMaxDuration( profile ),
                      t2.getTravelMode() ) ) {
        dominatedSet.add( t2 );
      }
      if( v2.dominate( v,
                       minDuration,
                       maxDuration,
                       //getMinDuration( profile ),
                       //getMaxDuration( profile ),
                       t2.getTravelMode() ) ) {
        isDominated = true;
      }
    }

    if( dominatedSet.size() > 0 ) {
      paretoSet.removeAll( dominatedSet );
    }
    if( !isDominated && !paretoSet.contains( newSol ) ) {
      paretoSet.add( newSol );
    }
    //}
  }

  // ===========================================================================

  /**
   * The method perturbate the given trip {@code trip} by applying one of the
   * possible operations.
   *
   * @param trip
   * @param steps
   * @param generator
   * @return
   */

  public static TripWritable perturbate
  ( TripWritable trip,
    Map<String, List<Step>> steps,
    int maxDuration,
    int minDuration,
    Random generator,
    String requiredPoi ) {

    if( trip == null ) {
      throw new NullPointerException();
    }
    if( steps == null ) {
      throw new NullPointerException();
    }
    if( generator == null ) {
      throw new NullPointerException();
    }

    if( trip.getSteps().size() == steps.size() ) {
      // all steps have been added: only removes are possible
      return removeSite( trip, steps, generator, requiredPoi );

    } else if( trip.getSteps().size() <= 2 ) {
      final int c = generator.nextInt( 1 );
      switch( c ) {
        case 0:
          return addSite( trip, steps, generator, requiredPoi );
        //case 1:
        //  return addScenicRoute( trip, steps, generator );
        default:
          return trip;
      }

    } else {

      if( trip.getDuration() > maxDuration ) {
        final int c = generator.nextInt( 3 );
        switch( c ) {
          case 0:
            return removeSite( trip, steps, generator, requiredPoi );
          case 1:
            return replaceSite( trip, steps, generator, requiredPoi );
          case 2:
            return swapSites( trip, steps, generator, requiredPoi );
          default:
            return trip;
        }

      } else if( trip.getDuration() < minDuration ) {
        final int c = generator.nextInt( 3 );
        switch( c ) {
          case 0:
            return addSite( trip, steps, generator, requiredPoi );
          case 1:
            return replaceSite( trip, steps, generator, requiredPoi );
          case 2:
            //return addScenicRoute( trip, steps, generator );
            return swapSites( trip, steps, generator, requiredPoi );
          default:
            return trip;
        }

      } else {
        final int c = generator.nextInt( 4 );
        switch( c ) {
          case 0:
            return removeSite( trip, steps, generator, requiredPoi );
          case 1:
            return replaceSite( trip, steps, generator, requiredPoi );
          case 2:
            return addSite( trip, steps, generator, requiredPoi );
          //case 3:
          //  return addScenicRoute( trip, steps, generator );
          case 4:
            return swapSites( trip, steps, generator, requiredPoi );
          default:
            return trip;
        }
      }
    }
  }


  // ===========================================================================

  /**
   * The method returns a new step for the trip {@code trip} using the possible
   * steps in {@code steps}. It returns null if all the steps are already
   * present in the trip.
   *
   * @param trip
   * @param steps
   * @param generator
   * @return
   */
  private static String findNewSite
  ( TripWritable trip,
    Map<String, List<Step>> steps,
    Random generator ) {

    if( trip == null ) {
      throw new NullPointerException();
    }
    if( steps == null ) {
      throw new NullPointerException();
    }
    if( generator == null ) {
      throw new NullPointerException();
    }

    final List<String> possibleSites = new ArrayList<>( steps.keySet() );
    possibleSites.sort( new Comparator<String>() {
      @Override
      public int compare( String o1, String o2 ) {
        if( o1 == null && o2 == null ) {
          return 0;
        } else if( o1 != null && o2 == null ) {
          return 1;
        } else if( o1 == null && o2 != null ) {
          return -1;
        } else {
          return o1.compareTo( o2 );
        }
      }
    } );
    // remove all sites that are already in the trip
    possibleSites.removeAll( trip.getSites() );
    // remove all sites that delimit a scenic route
    possibleSites.removeAll( getScenicSites() );

    if( possibleSites.size() == 0 ) {
      return null;
    }

    int newSiteIndex = generator.nextInt( possibleSites.size() );
    return possibleSites.get( newSiteIndex );
  }

  /**
   * The method returns a new scenic route for the trip {@code trip} using the
   * possible scenic routes in {@code steps}. It returns null if all the scenic
   * routes are already present in the trip.
   *
   * @param trip
   * @param steps
   * @param generator
   * @return
   */
  private static Step findNewScenicRoute
  ( TripWritable trip,
    Map<String, List<Step>> steps,
    Random generator ) {

    if( trip == null ) {
      throw new NullPointerException();
    }
    if( steps == null ) {
      throw new NullPointerException();
    }
    if( generator == null ) {
      throw new NullPointerException();
    }

    // build the available scenic routes
    final Map<String, List<Step>> scenicRoutes = new HashMap<>();
    for( String scenicOrigin : getScenicSites() ) {
      final int oIndex = trip.getSites().indexOf( scenicOrigin );

      if( oIndex != -1 ) {
        final List<Step> scenicSteps = new ArrayList<>();
        for( Step availableStep : steps.get( scenicOrigin ) ) {
          final String destination = availableStep.getDestination();
          final int dIndex = trip.getSites().indexOf( destination );
          if( isScenicSite( destination ) && dIndex != -1 ) {
            scenicSteps.add( availableStep );
          }
        }
        if( !scenicSteps.isEmpty() ) {
          scenicRoutes.put( scenicOrigin, scenicSteps );
        }
      }
    }

    if( scenicRoutes.size() == 0 ) {
      return null;
    }

    final int oIndex = scenicRoutes.size() == 1
      ? 0 : generator.nextInt( scenicRoutes.size() );
    final String orig = scenicRoutes.keySet().toArray
      ( new String[scenicRoutes.keySet().size()] )[oIndex];

    int dIndex = scenicRoutes.get( orig ).size() == 1
      ? 0 : generator.nextInt( scenicRoutes.get( orig ).size() );

    return scenicRoutes.get( orig ).get( dIndex );
  }


  /**
   * The method modifies the given trip {@code trip} by replacing one of its
   * sites with another available one. If the parameter {@code requiredPoi} is
   * different from {@code null} such step together with the start one cannot be
   * replaced in the list of sites.
   *
   * @param trip
   * @param possibleSteps
   * @param generator
   * @return
   */

  public static TripWritable replaceSite
  ( TripWritable trip,
    Map<String, List<Step>> possibleSteps,
    Random generator,
    String requiredPoi ) {

    if( trip == null ) {
      throw new NullPointerException();
    }
    if( possibleSteps == null ) {
      throw new NullPointerException();
    }
    if( generator == null ) {
      throw new NullPointerException();
    }

    if( trip.getNumSites() <= 1 ) {
      // the first step cannot be replaced
      return trip;
    }

    int siteToChangeIndex;

    if( requiredPoi == null ) {
      if( trip.getNumSites() == 2 ) {
        siteToChangeIndex = 1;
      } else {
        siteToChangeIndex =
          generator.nextInt( trip.getNumSites() - 2 ) + 1;
      }
    } else {
      siteToChangeIndex =
        generator.nextInt( trip.getNumSites() - 1 );
    }//*/

    final TripWritable t = new TripWritable( trip );
    if( trip.getSites().get( siteToChangeIndex ).equals( requiredPoi ) ) {
      return t;
    }

    final String newSite = findNewSite( t, possibleSteps, generator );
    if( newSite == null ) {
      return t;
    }

    t.removeSite( siteToChangeIndex );
    t.addSite( siteToChangeIndex, newSite );
    t.buildSteps( possibleSteps );

    return t;
  }

  /**
   * The method modifies the given trip {@code trip} by swapping one of its
   * sites with the consecutive one.
   *
   * @param trip
   * @param possibleSteps
   * @param generator
   * @return
   */

  public static TripWritable swapSites
  ( TripWritable trip,
    Map<String, List<Step>> possibleSteps,
    Random generator,
    String requiredPoi ) {

    if( trip == null ) {
      throw new NullPointerException();
    }
    if( possibleSteps == null ) {
      throw new NullPointerException();
    }
    if( generator == null ) {
      throw new NullPointerException();
    }

    final int siteToChangeIndex;

    if( requiredPoi == null ) {
      // the first step cannot be replaced
      if( trip.getNumSites() <= 2 ) {
        return trip;
      }
      if( trip.getNumSites() == 3 ) {
        siteToChangeIndex = 1;
      } else {
        siteToChangeIndex =
          generator.nextInt( trip.getNumSites() - 3 ) + 1;
      }

    } else {
      siteToChangeIndex =
        generator.nextInt( trip.getNumSites() - 1 );
    }//*/

    final TripWritable t = new TripWritable( trip );
    final String firstStep = t.getSites().get( siteToChangeIndex );

    t.removeSite( siteToChangeIndex );
    t.addSite( siteToChangeIndex + 1, firstStep );
    t.buildSteps( possibleSteps );

    return t;
  }

  /**
   * The method modifies the given trip {@code trip} by adding a new site not
   * contained in the trip.
   *
   * @param trip
   * @param steps
   * @param generator
   * @return
   */

  public static TripWritable addSite
  ( TripWritable trip,
    Map<String, List<Step>> steps,
    Random generator,
    String requiredPoi ) {

    if( trip == null ) {
      throw new NullPointerException();
    }
    if( steps == null ) {
      throw new NullPointerException();
    }
    if( generator == null ) {
      throw new NullPointerException();
    }

    if( trip.getNumSites() == 0 ) {
      return trip;
    }

    final int currSiteIndex;

    if( requiredPoi == null ) {
      if( trip.getNumSites() == 1 ) {
        //currSiteIndex = 0;
        currSiteIndex = 1;
      } else {
        //currSiteIndex = generator.nextInt( trip.getNumSites() - 1 );
        currSiteIndex = generator.nextInt( trip.getNumSites() - 1 ) + 1;
      }
    } else {
      currSiteIndex = generator.nextInt( trip.getNumSites() );
    }//*/

    final TripWritable t = new TripWritable( trip );
    final String siteToAdd = findNewSite( t, steps, generator );
    if( siteToAdd == null ) {
      return t;
    }

    t.addSite( currSiteIndex, siteToAdd );
    t.buildSteps( steps );

    return t;
  }

  /**
   * The method modifies the given trip {@code trip} by removing one of its
   * steps.
   *
   * @param trip
   * @param steps
   * @param generator
   * @return
   */

  public static TripWritable removeSite
  ( TripWritable trip,
    Map<String, List<Step>> steps,
    Random generator,
    String requiredPoi ) {

    if( trip == null ) {
      throw new NullPointerException();
    }
    if( steps == null ) {
      throw new NullPointerException();
    }
    if( generator == null ) {
      throw new NullPointerException();
    }

    if( trip.getNumSites() < 2 ) {
      return trip;
    }

    final int initialIndex;
    if( requiredPoi == null ) {
      initialIndex = 1;
    } else {
      initialIndex = 0;
    }

    final Map<String, Integer> removableSites = new HashMap<>();
    for( int i = initialIndex; i < trip.getSites().size(); i++ ) {
      if( !isScenicSite( trip.getSites().get( i ) ) &&
          ( requiredPoi != null &&
            !trip.getSites().get( i ).equals( requiredPoi ) ) ) {
        removableSites.put( trip.getSites().get( i ), i );
      }
    }

    if( removableSites.size() == 0 ) {
      return trip;
    }


    final int siteToRemoveIndex;
    final int numChoices = removableSites.keySet().size();
    final String[] keys =
      removableSites.keySet().toArray( new String[numChoices] );

    if( removableSites.keySet().size() == 1 ) {
      siteToRemoveIndex = removableSites.get( keys[0] );
    } else {
      final int choice = generator.nextInt( numChoices );
      final String key = keys[choice];
      siteToRemoveIndex = removableSites.get( key );
    }


    final TripWritable t = new TripWritable( trip );
    t.removeSite( siteToRemoveIndex );
    t.buildSteps( steps );

    return t;
  }

  /**
   * The method modifies the given trip {@code trip} by adding a new scenic
   * route not contained in the trip.
   *
   * @param trip
   * @param steps
   * @param generator
   * @return
   */

  public static TripWritable addScenicRoute
  ( TripWritable trip,
    Map<String, List<Step>> steps,
    Random generator ) {

    if( trip == null ) {
      throw new NullPointerException();
    }
    if( steps == null ) {
      throw new NullPointerException();
    }
    if( generator == null ) {
      throw new NullPointerException();
    }

    if( trip.getNumSites() == 0 ) {
      return trip;
    }

    final int scenicRouteIndex;
    if( trip.getNumSites() == 1 ) {
      scenicRouteIndex = 0;
    } else {
      scenicRouteIndex = generator.nextInt( trip.getNumSites() - 1 );
    }

    final TripWritable t = new TripWritable( trip );
    final Step scenicRouteToAdd = findNewScenicRoute( t, steps, generator );
    if( scenicRouteToAdd == null ) {
      return t;
    }

    t.addSite( scenicRouteIndex + 1, scenicRouteToAdd.getOrigin() );
    t.addSite( scenicRouteIndex + 2, scenicRouteToAdd.getDestination() );
    t.buildSteps( steps );

    return t;
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param currentSol
   * @param possibleSteps
   * @param numVisits
   * @param stayTimes
   * @param eto
   * @param travelMode
   * @param minDuration
   * @param maxDuration
   * @param paretoSet
   * @param paretoFront
   * @param generator
   * @param initialTemperature
   * @param finalTemperature
   * @param alpha
   * @param maxPerturbations
   * @param requiredPoi
   * @param historicalPercentage
   * @param deltaPerVisitor
   * @return
   */

  public static Set<TripWritable> performSa
  ( TripWritable currentSol,
    Map<String, Map<String, List<Step>>> possibleSteps,
    Map<String, Map<String, Integer>> numVisits,
    Map<String, Map<Integer, Integer>> stayTimes,
    Map<String, Map<Integer, Integer>> eto,
    String travelMode,
    Integer minDuration,
    Integer maxDuration,
    Set<TripWritable> paretoSet,
    Set<TripValue> paretoFront,
    Random generator,
    double initialTemperature,
    double finalTemperature,
    double alpha,
    long maxPerturbations,
    String requiredPoi,
    Double historicalPercentage,
    Integer deltaPerVisitor
  ) {

    if( currentSol == null ) {
      throw new NullPointerException();
    }
    if( possibleSteps == null ) {
      throw new NullPointerException();
    }
    if( numVisits == null ) {
      throw new NullPointerException();
    }
    if( stayTimes == null ) {
      throw new NullPointerException();
    }
    if( eto == null ) {
      throw new NullPointerException();
    }
    if( paretoSet == null ) {
      throw new NullPointerException();
    }
    if( paretoFront == null ) {
      throw new NullPointerException();
    }
    if( generator == null ) {
      throw new NullPointerException();
    }
    if( minDuration == null ) {
      throw new NullPointerException();
    }
    if( maxDuration == null ) {
      throw new NullPointerException();
    }
    if( deltaPerVisitor == null ) {
      throw new NullPointerException();
    }

    double temperature = initialTemperature;
    int c = 0;
    while( temperature > finalTemperature ) {
      for( long j = 0; j < maxPerturbations; j++ ) {
        final TripWritable perturbSol =
          perturbate( currentSol,
                      possibleSteps.get( travelMode ),
                      //getMaxDuration( profile ),
                      maxDuration,
                      //getMinDuration( profile ),
                      minDuration,
                      generator,
                      requiredPoi );
        perturbSol.buildStepsFromCompleteMap( possibleSteps );
        perturbSol.computeVisitingTime
          ( numVisits,
            stayTimes,
            eto,
            historicalPercentage,
            deltaPerVisitor );

        if( perturbSol.equals( currentSol ) ) {
          continue;
        }

        int count = 0;
        for( String site : perturbSol.getSites() ) {
          if( site.equals( requiredPoi ) ) {
            count += 1;
          }
        }
        if( count != 1 ) {
          throw new IllegalArgumentException( "****** duplicated sites!!!!" );
        }


        /*if( currentSol.getDuration() < maxDuration &&
            perturbSol.getDuration() > maxDuration ) {
          continue;
        }

        if( currentSol.getDuration() > maxDuration &&
            perturbSol.getDuration() > maxDuration &&
            currentSol.getDuration() > perturbSol.getDuration() ) {
          continue;
        }//*/

        boolean severalDays = false;
        for( String s : perturbSol.getSites() ) {
          if( perturbSol.getSiteArrivingHour
            ( s, numVisits,
              stayTimes, eto,
              historicalPercentage,
              deltaPerVisitor ) > 24 ) {
            severalDays = true;
          }
        }
        if( severalDays ) {
          continue;
        }

        final double p = acceptanceProbability
          ( currentSol,
            perturbSol,
            paretoFront,
            temperature,
            minDuration,
            //getMaxDuration( profile ),
            maxDuration,
            travelMode );

        final double u = generator.nextDouble();
        if( u < p ) {
          // update the Pareto-set
          updateParetoSet( paretoSet, perturbSol, minDuration, maxDuration );

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
