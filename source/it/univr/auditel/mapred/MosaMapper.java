package it.univr.auditel.mapred;

import it.univr.auditel.entities.*;
import it.univr.auditel.mosa.MosaUtils;
import it.univr.auditel.shadoop.core.ViewSequenceValue;
import it.univr.auditel.shadoop.core.ViewSequenceWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static it.univr.auditel.TrsaAuditel.*;
import static it.univr.auditel.shadoop.core.FileReader.*;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.Math.round;


/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */

public class MosaMapper
  //extends Mapper<LongWritable, Text, Text, ViewSequenceWritable> {
  extends Mapper<LongWritable, Text, Text, Text> {

  // ===========================================================================

  private Map<GContext, Map<GContext, Double>> groupTypeEvolutionMap;
  private Map<String, List<UserPreference>> preferenceMap;
  private Map<Long, Map<String, List<ProgramRecord>>> schedulingMap;
  private Map<String, Map<String, Double>> genreSequenceMap;
  private GContext initialContext;
  private FileSystem hdfs;

  private List<ViewSequenceWritable> solutions;

  private Set<ViewSequenceValue> paretoFront;
  private Integer duration;
  private Integer durationOffset;
  private Long maxPerturbations;
  private Integer initialTemperature;
  //private Boolean dynamic;
  //private Double historicalPercentage;
  //private Integer deltaPerVisitor;

  private static double alpha = 0.88;
  private static double finalTemperature = 1;

  private int countGenres;
  private int countGenresTot;
  private int countFd;
  private int countFe;
  private int countFh;
  private int countFs;
  private int countFm;
  private int countFj;
  private int countTot;

  private boolean auditel;
  private boolean dynamic;

  // ===========================================================================

  @Override
  protected void setup( Context context )
    throws IOException, InterruptedException{

    // --- paper metrics -------------------------------------------------------
    countGenres = 0;
    countGenresTot = 0;
    countFd = 0;
    countFe = 0;
    countFh = 0;
    countFs = 0;
    countFm = 0;
    countFj = 0;
    countTot = 0;
    // -------------------------------------------------------------------------

    final Configuration configuration = context.getConfiguration();
    duration = parseInt( configuration.get( durationLabel ) );
    durationOffset = parseInt( configuration.get( durationOffsetLabel ) );
    maxPerturbations = parseLong( configuration.get( maxPerturbationsLabel ) );
    initialTemperature = parseInt( configuration.get( initialTemperatureLabel ) );
    auditel = Boolean.parseBoolean( configuration.get( auditelLabel ) );
    dynamic = Boolean.parseBoolean( configuration.get( dynamicLabel ) );

    hdfs = FileSystem.get( configuration );
    final URI[] cachedFiles = context.getCacheFiles();

    if( cachedFiles != null && cachedFiles.length == 5 ){
      final String groupTypeEvo = configuration.get( groupTypeEvoFileLabel );
      final String preference = configuration.get( userPreferenceFileLabel );
      final String scheduling = configuration.get( schedulingFileLabel );
      final String genreSequence = configuration.get( genreSequenceFileLabel );
      final String estPareto = configuration.get( paretoFileLabel );

      for( int i = 0; i < cachedFiles.length; i++ ){
        final URI uri = cachedFiles[ i ];
        if( uri.getPath().endsWith( groupTypeEvo ) ){
          groupTypeEvolutionMap = readGroupTypeEvolution( hdfs, uri );
        } else if( uri.getPath().endsWith( preference ) ){
          preferenceMap = readUserPreferences( hdfs, uri );
        } else if( uri.getPath().endsWith( scheduling ) ){
          if( scheduling.startsWith( "poi_" )){
            schedulingMap = readVisitingTime( hdfs, uri );
          } else if( scheduling.startsWith( "epg_" )){
            schedulingMap = readScheduling( hdfs, uri );
          }
        } else if( uri.getPath().endsWith( genreSequence ) ){
          genreSequenceMap = readGenreSequencePreferences( hdfs, uri );
        } else if( uri.getPath().endsWith( estPareto ) ){
          paretoFront = readParetoFrontFromHdfs( hdfs, uri );
        }
      }
    }

    initialContext = new GContext();
    final String a = configuration.get( ageClassesLabel );
    final StringTokenizer tk = new StringTokenizer( a, "," );
    while( tk.hasMoreTokens() ){
      initialContext.addAgeClass( tk.nextToken() );
    }

    initialContext.setTimeSlot( configuration.get( timeSlotLabel ) );

    solutions = new ArrayList<>();
  }

  @Override
  protected void map( LongWritable key, Text value, Context context )
    throws IOException, InterruptedException{

    final ViewSequenceWritable sequence = new ViewSequenceWritable();
    sequence.fromText( value );

    if( sequence.checkSequence( initialContext, duration, durationOffset ) &&
      sequence.getSequence() != null &&
      !sequence.isEmpty() &&
      !solutions.contains( sequence ) ){
      solutions.add( sequence );
    }
  }

  @Override
  protected void cleanup( Context context ) throws IOException, InterruptedException{
    final Random generator = new Random();

    final int endIndex = solutions.size();

    for( int i = 0; i < endIndex; i++ ){
      //for( ViewSequenceWritable sequence : solutions ){
      ViewSequenceWritable sequence = solutions.get( i );

      final GroupView start = sequence.getSequence().get( 0 );
      final GroupView end = sequence.getSequence().get( sequence.getSequence().size() - 1 );
      // duration in minutes
      final int dur =
        (int) ((end.getIntervalEnd().getTime() -
          start.getIntervalStart().getTime()) / (1000 * 60));//*/

      Set<ViewSequenceWritable> paretoSet = new HashSet<>();
      paretoSet.add( sequence );

      // performSa() updates the paretoSet
      paretoSet = MosaUtils.performSa
        ( sequence,
          schedulingMap,
          preferenceMap,
          genreSequenceMap,
          groupTypeEvolutionMap,
          // Math.max(0, dur - durationOffset),
          duration - durationOffset,//minDuration,
          // dur + durationOffset,
          duration + durationOffset,//maxDuration,
          paretoSet,
          paretoFront,
          generator,
          initialTemperature,
          finalTemperature,
          alpha,
          maxPerturbations,
          auditel,
          dynamic );//*/


      final List<ViewSequenceWritable> vlis = new ArrayList<>( paretoSet );

      // Only one choice so the result can be used to update ETO
      final Integer index =
        (int) round( (vlis.size() - 1) * generator.nextDouble() );
      final ViewSequenceWritable selected = vlis.get( index );

      /*for (int i = 0; i < selected.size() - 1; i++) {
        if (selected.getSequence().get(i + 1).getIntervalStart().getTime() <
            selected.getSequence().get(i).getIntervalEnd().getTime()) {
          System.out.printf("Negative duration.%n");
        }
      }//*/

      // --- paper metrics -----------------------------------------------------
      final Set<String> s1 = new HashSet<>();
      for( GroupView v : sequence.getSequence() ){
        s1.add( v.getEpgChannelId() );
      }
      final Set<String> s2 = new HashSet<>();
      for( GroupView v : selected.getSequence() ){
        s2.add( v.getEpgChannelId() );
      }
      final int ss1 = s1.size();
      s1.removeAll( s2 );
      countGenres = ss1 - s1.size();
      countGenresTot = ss1;

      if( Math.abs( selected.getDuration() - duration ) <=
        Math.abs( sequence.getDuration() - duration ) ){
        countFd += 1;
      }

      final ViewSequenceValue sequenceValue = new ViewSequenceValue
        ( sequence, preferenceMap, genreSequenceMap, groupTypeEvolutionMap,
          schedulingMap, auditel, dynamic );
      final ViewSequenceValue selectedValue = new ViewSequenceValue
        ( selected, preferenceMap, genreSequenceMap, groupTypeEvolutionMap,
          schedulingMap, auditel, dynamic );

      if( sequenceValue.getMissedSeconds() < selectedValue.getMissedSeconds() ){
        countFe += 1;
      }

      if( selectedValue.getGroupPreference() > sequenceValue.getGroupPreference() ){
        countFs += 1;
      }

      if( selectedValue.getMinMaxFairness() > sequenceValue.getMinMaxFairness() ){
        countFm += 1;
      }

      if( selectedValue.getJainFairness() > sequenceValue.getJainFairness() ){
        countFj += 1;
      }

      countTot += 1;


      final List<GroupView> v = selected.getSequence();
      final String k = keyToString( selected.getSequence() );

      context.write( new Text( k ), new Text( valueToString( v ) ) );
    }

    System.out.printf( "[LOG] Genre: %s, GenreTot: %s, Fd: %s, Fe: %s, Fs: %s, Fm: %s, Fj: %d, Tot: %s.%n",
      countGenres, countGenresTot,
      countFd, countFe, countFs, countFm, countFj,
      countTot );
  }

  /**
   * FAST FIX: The method produces the string output key.
   *
   * @param sequence
   * @return
   */
  private String keyToString( List<GroupView> sequence ){
    final StringBuilder b = new StringBuilder();
    b.append( "(" );
    if( sequence != null && !sequence.isEmpty() ){
      final GroupView start = sequence.get( 0 );
      final Iterator<String> it = start.getGroup().getTypeSet().iterator();
      while( it.hasNext() ){
        b.append( it.next() );
        if( it.hasNext() ){
          b.append( "-" );
        }
      }
      b.append( "," );
      b.append( start.getTimeSlot() );
      b.append( ")" );
    }
    return b.toString();
  }

  /**
   * FAST FIX: The method produces the string output value: the toString() method does not work!!!
   *
   * @param sequence
   * @return
   */
  private String valueToString( List<GroupView> sequence ){
    final StringBuilder b = new StringBuilder();

    if( sequence != null && !sequence.isEmpty() ){
      final Group group = sequence.get( 0 ).getGroup();

      final List<String> userList = new ArrayList<>( group.getUsers() );
      Collections.sort( userList );
      final Iterator<String> utk = userList.iterator();
      while( utk.hasNext() ){
        b.append( utk.next() );
        if( utk.hasNext() ){
          b.append( "-" );
        }
      }
      b.append( "," );

      b.append( group.getFamilyId() );
      b.append( "," );

      b.append( group.getGroupId() );
      b.append( "," );

      // changed for retrieving single user ages
      /*final Iterator<String> ttk = group.getTypeSet().iterator();
      while( ttk.hasNext() ) {
        b.append( ttk.next() );
        if( ttk.hasNext() ) {
          b.append( "-" );
        }
      }//*/
      final Iterator<String> utk2 = userList.iterator();
      while( utk2.hasNext() ){
        final String u = utk2.next();
        b.append( group.getTypeByUser( u ) );
        if( utk2.hasNext() ){
          b.append( "-" );
        }
      }
      b.append( "," );

      //b.append( group.getTimeSlot() );
      //b.append( "\t" );

      final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd HH:mm" );
      final Iterator<GroupView> git = sequence.iterator();
      while( git.hasNext() ){
        final GroupView view = git.next();
        b.append( view.getEpgChannelId() );
        b.append( "," );
        b.append( view.getProgramId() );
        b.append( "," );
        b.append( f.format( view.getIntervalStart() ) );
        b.append( "," );
        b.append( f.format( view.getIntervalEnd() ) );
        b.append( "," );
        b.append( view.getTimeSlot() );
        if( git.hasNext() ){
          b.append( "," );
        }
      }
    }
    return b.toString();
  }
}
