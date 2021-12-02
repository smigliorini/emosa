package it.univr.auditel;

import it.univr.auditel.entities.DynamicContext;
import it.univr.auditel.entities.GContext;
import it.univr.auditel.entities.Group;
import it.univr.auditel.entities.GroupView;
import it.univr.auditel.entities.ViewRecord;
import it.univr.auditel.shadoop.core.ViewSequenceWritable;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

import static it.univr.auditel.entities.Utils.*;
import static java.lang.Math.*;
import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class ProcessAuditelRowData {

  public static String directory = "C:\\workspace\\projects\\veronacard_analysis\\data\\auditel";
  public static String logFile = "log2_3min_groups_clean_timeslot_wdwe.csv";
  public static String userFile = "user_demographic_features.csv";
  public static String seqOutFile = "sequences.csv";
  public static String evolutionFile = "group_evolution.csv";
  public static String typeEvolutionFile = "group_type_evolution.csv";
  public static String seqHistoryOutFile = "sequences_history.csv";
  public static String seqQueryOutFile = "sequences_query.csv";
  public static String commandOutFile = "auditel_commands.sh";

  private static final String delimiter = ";";

  private static String[] header =
    {
      "id",
      "user",
      "family_id",
      "group_id",
      "program_id",
      "epg_channel_id",
      "starttime",
      "endtime"
    };

  private static long shortView = 2 * 60 * 1000;

  private static String separator = ",";

  // === Methods ===============================================================


  public static void main( String[] args ) throws ParseException, FileNotFoundException {

    final List<String> lines =
      readLines( new File( format( "%s\\%s", directory, logFile ) ), true );
    System.out.printf( "Number of individual views: %d.%n", lines.size() );

    final List<ViewRecord> records = discardShortViews( lines );
    System.out.printf( "Number of cleaned individual views: %d.%n", records.size() );

    final Map<Key, List<ViewRecord>> candidateGroupViews = candidateGroupViews( records );
    System.out.printf( "Number of candidate group views: %d.%n", candidateGroupViews.size() );

    final Map<String, String> userAges = readUserAges();
    System.out.printf( "Number of user ages read: %d.%n", userAges.size() );

    final Map<Integer, GroupView> views = refineGroupViews( candidateGroupViews, userAges );
    System.out.printf( "Number of refined group views: %d.%n", views.size() );
    int singleGroup = 0;
    int multiGroup = 0;
    for( GroupView view : views.values() ) {
      if( view.getGroup().getUsers().size() > 1 ) {
        multiGroup++;
      } else {
        singleGroup++;
      }
    }
    System.out.printf
      ( "Number of refined group views with more than one component: %d.%n", multiGroup );
    System.out.printf
      ( "Number of refined group views with a single component: %d.%n", singleGroup );

    final Collection<ViewSequenceWritable> sequences = groupViewSequences( views.values() );
    System.out.printf( "Number of group view sequences: %d.%n", sequences.size() );
    int singleSeq = 0;
    int multiSeq = 0;
    final List<ViewSequenceWritable> historySequences = new ArrayList<>();
    final List<ViewSequenceWritable> querySequences = new ArrayList<>();
    final Random generator = new Random( 3948382918L );
    for( ViewSequenceWritable s : sequences ) {
      if( s.size() > 1 ) {
        multiSeq++;
        final int p = generator.nextInt( 3 );
        if( p < 2 ) {
          historySequences.add( s );
        } else {
          querySequences.add( s );
        }
      } else {
        singleSeq++;
        historySequences.add( s );
      }
    }
    System.out.printf
      ( "Number of group view sequences with more than one view: %d.%n", multiSeq );
    System.out.printf
      ( "Number of group view sequences with a single view: %d.%n", singleSeq );

    final String content = printGroupViewSequences( sequences );
    writeFile( directory, seqOutFile, content );
    final String hcontent = printGroupViewSequences( historySequences );
    writeFile( directory, seqHistoryOutFile, hcontent );
    final String qcontent = printGroupViewSequences( querySequences );
    writeFile( directory, seqQueryOutFile, qcontent );

    final String commands = printCommands( querySequences );
    writeFile( directory, commandOutFile, commands );

    final Set<List<Group>> evolution = groupEvolution( views.values() );
    System.out.printf( "Number of possible group evolutions found: %d.%n", evolution.size() );
    final String econtent = printGroupEvolution( evolution );
    writeFile( directory, evolutionFile, econtent );

    final Set<DynamicContext> dynContexts = groupTypeEvolution( evolution );
    System.out.printf( "Number of possible group type evolutions found: %d.%n", dynContexts.size() );
    final String tcontent = printGroupTypeEvolution( dynContexts );
    writeFile( directory, typeEvolutionFile, tcontent );

    /*final List<String> elines = readLines( new File( directory, evolutionFile ), false );
    final List<List<Group>> evolution = buildGroupEvolution( elines, groups );
    System.out.printf( "Number of evolutions read: %d.%n", evolution.size() );*/
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @return
   */
  public static Map<String, String> readUserAges() {
    final List<String> lines = readLines( new File( directory, userFile ), true );
    final Map<String, String> ages = new HashMap<>();
    for( String l : lines ) {
      final StringTokenizer tk = new StringTokenizer( l, delimiter );
      final String key = tk.nextToken();
      for( int i = 1; i < 6; i++ ) {
        // "family_id",
        // "is_subscriber",
        // "gender",
        // "year_of_birth",
        // "age"
        tk.nextToken();
      }
      final String value = tk.nextToken();
      ages.put( key, value );
    }

    return ages;
  }

  // ===========================================================================

  /**
   * The method computes the possible group evolutions.
   *
   * @param views
   * @return
   */

  public static Set<List<Group>> groupEvolution( Collection<GroupView> views ) {
    if( views == null ) {
      throw new NullPointerException();
    }

    final Map<String, ViewSequenceWritable> map = new HashMap<>();

    // retrieve views of each users
    for( GroupView v : views ) {
      final Set<String> users = v.getGroup().getUsers();
      final Iterator<String> it = users.iterator();

      while( it.hasNext() ) {
        final String key = it.next();
        ViewSequenceWritable value = map.get( key );
        if( value == null ) {
          value = new ViewSequenceWritable();
        }
        value.addView( v );
        map.put( key, value );
      }
    }


    // order the views of each user
    for( Map.Entry<String, ViewSequenceWritable> e : map.entrySet() ) {
      Collections.sort( e.getValue().getSequence(), new Comparator<GroupView>() {
        @Override
        public int compare( GroupView o1, GroupView o2 ) {
          if( o1 == null && o2 == null ) {
            return 0;
          } else if( o1 != null && o2 == null ) {
            return -1;
          } else if( o1 == null && o2 != null ) {
            return 1;
          } else {
            int v = o1.getIntervalStart().compareTo( o2.getIntervalStart() );
            if( v == 0 ) {
              return o1.getIntervalEnd().compareTo( o2.getIntervalEnd() );
            } else {
              return v;
            }
          }
        }
      } );
    }

    final Set<ViewSequenceWritable> viewEvolution = new HashSet<>();

    for( Map.Entry<String, ViewSequenceWritable> e : map.entrySet() ) {
      final ViewSequenceWritable complete = e.getValue();
      if( complete != null && !complete.isEmpty() ) {
        final Iterator<GroupView> it = complete.getSequence().iterator();

        GroupView current = it.next();
        GroupView next = null;
        ViewSequenceWritable evolution = new ViewSequenceWritable();
        evolution.addView( current );

        while( it.hasNext() ) {
          next = it.next();

          if( next.getIntervalStart().getTime() -
              current.getIntervalEnd().getTime() < delta ) {
            evolution.addView( next );
          } else {
            viewEvolution.add( evolution );
            evolution = new ViewSequenceWritable();
            evolution.addView( next );
          }
          current = next;
        }
        viewEvolution.add( evolution );
      }
    }

    final Set<List<Group>> groupEvolution = new HashSet<>();
    final Iterator<ViewSequenceWritable> it = viewEvolution.iterator();
    while( it.hasNext() ) {
      final List<Group> g = new ArrayList<>();
      ViewSequenceWritable current = it.next();
      for( GroupView v : current.getSequence() ) {
        g.add( v.getGroup() );
      }
      groupEvolution.add( g );
    }
    return groupEvolution;
  }


  /**
   * MISSING_COMMENT
   *
   * @param evolutions
   * @return
   */

  public static Set<DynamicContext> groupTypeEvolution( Set<List<Group>> evolutions ) {
    if( evolutions == null ) {
      throw new NullPointerException();
    }

    final Map<GContext, Map<GContext, DynamicContext>> contexts = new HashMap<>();
    for( List<Group> e : evolutions ) {
      for( int i = 0; i < e.size() - 1; i++ ) {
        final GContext start = new GContext();
        start.setAgeClassSet( e.get( i ).getTypeSet() );
        start.setTimeSlot( e.get( i ).getTimeSlot() );

        final GContext end = new GContext();
        end.setAgeClassSet( e.get( i + 1 ).getTypeSet() );
        end.setTimeSlot( e.get( i + 1 ).getTimeSlot() );

        // dc.probability = dc.probability + 1;

        Map<GContext, DynamicContext> t = contexts.get( start );
        DynamicContext dc = null;
        if( t != null ) {
          dc = t.get( end );
          if( dc != null ) {
            dc.setProbability( dc.getProbability() + 1 );
          }
          if( dc == null ) {
            dc = new DynamicContext();
            dc.setStart( start );
            dc.setEnd( end );
            dc.setProbability( 1 );
          }
          t.put( end, dc );
        } else {
          dc = new DynamicContext();
          dc.setStart( start );
          dc.setEnd( end );
          dc.setProbability( 1 );
          t = new HashMap<>();
          t.put( end, dc );
          contexts.put( start, t );
        }
      }
    }

    final Set<DynamicContext> result = new HashSet<>();
    for( Map<GContext, DynamicContext> m : contexts.values() ) {
      int total = 0;
      for( DynamicContext c : m.values() ) {
        total += c.getProbability();
      }
      for( DynamicContext c : m.values() ) {
        c.setProbability( c.getProbability() / total );
        result.add( c );
      }
    }
    return result;
  }

  // ===========================================================================


  /**
   * The method identifies the sequence of views performed by the same group of
   * users.
   *
   * @param views
   * @return
   */

  public static Collection<ViewSequenceWritable> groupViewSequences
  ( Collection<GroupView> views ) {
    if( views == null ) {
      throw new NullPointerException();
    }

    // cluster groups composed by the same users
    final Map<Set<String>, ViewSequenceWritable> clusters = new HashMap<>();

    int t = 0;

    for( GroupView v : views ) {
      ViewSequenceWritable value = clusters.get( v.getGroup().getUsers() );
      if( value == null ) {
        value = new ViewSequenceWritable();
      } else if( value.size() == 1 ) {
        t++;
      }
      value.addView( v );

      clusters.put( v.getGroup().getUsers(), value );
    }
    System.out.printf( "Number of clusters (views performed by the same group of users): %d.%n", clusters.size() );
    System.out.printf( "Number of clusters with more than one view: %d.%n", t );

    // order the group visions
    final Map<Set<String>, ViewSequenceWritable> ordclusters = new HashMap<>();

    for( Map.Entry<Set<String>, ViewSequenceWritable> e : clusters.entrySet() ) {
      final ViewSequenceWritable gl = e.getValue();
      Collections.sort( gl.getSequence(), new Comparator<GroupView>() {
        @Override
        public int compare( GroupView o1, GroupView o2 ) {
          if( o1 == null && o2 == null ) {
            return 0;
          } else if( o1 == null && o2 != null ) {
            return -1;
          } else if( o1 != null && o2 == null ) {
            return 1;
          } else {
            final int c = o1.getIntervalStart().compareTo( o2.getIntervalStart() );
            if( c == 0 ) {
              return o1.getIntervalEnd().compareTo( o2.getIntervalEnd() );
            } else {
              return c;
            }
          }
        }
      } );
      ordclusters.put( e.getKey(), gl );
    }
    System.out.printf( "Number of ordered clusters: %d.%n", clusters.size() );

    // split the group visions if they are not subsequent (use a buffer of 10
    // minutes since vision less than 10 minutes have been discarded)
    int count = 0;
    final Map<Integer, ViewSequenceWritable> result = new HashMap<>();

    for( Map.Entry<Set<String>, ViewSequenceWritable> e : ordclusters.entrySet() ) {
      ViewSequenceWritable seq = new ViewSequenceWritable();
      GroupView curr = e.getValue().getSequence().get( 0 );
      GroupView next = null;

      for( int i = 0; i < e.getValue().size() - 1; i++ ) {
        curr = e.getValue().getSequence().get( i );
        next = e.getValue().getSequence().get( i + 1 );
        if( next.getIntervalStart().getTime() -
            curr.getIntervalEnd().getTime() < delta ) {
          seq.addView( curr );
        } else {
          seq.addView( curr );
          result.put( count, seq );
          count++;
          seq = new ViewSequenceWritable();
        }
      }
      seq.addView( curr );
      count++;
    }

    return result.values();
  }

  /**
   * The method refine the group views considering also the interval in which
   * the same program is viewed by a group of people, obtaining the list of
   * groups.
   *
   * @param candidateGroupViews
   * @param userAges
   * @return
   */

  public static Map<Integer, GroupView> refineGroupViews
  ( Map<Key, List<ViewRecord>> candidateGroupViews,
    Map<String, String> userAges ) {

    if( userAges == null ) {
      throw new NullPointerException();
    }
    if( candidateGroupViews == null ) {
      throw new NullPointerException();
    }

    final Map<Integer, GroupView> result = new HashMap<>();
    int groupCount = 0;

    int progress = 0;
    System.out.printf( "[Progress]: " );

    // the group shall be divided if the vision intervals do not overlap
    for( Map.Entry<Key, List<ViewRecord>> e : candidateGroupViews.entrySet() ) {
      // temporarily sort the candidate group views
      final List<ViewRecord> records = e.getValue();
      Collections.sort( records, new Comparator<ViewRecord>() {
        @Override
        public int compare( ViewRecord o1, ViewRecord o2 ) {
          if( o1 == null && o2 == null ) {
            return 0;
          } else if( o1 == null && o2 != null ) {
            return -1;
          } else if( o1 != null && o2 == null ) {
            return 1;
          } else {
            final int c = o1.getStartTime().compareTo( o2.getStartTime() );
            if( c == 0 ) {
              return o1.getEndTime().compareTo( o2.getEndTime() );
            } else {
              return c;
            }
          }
        }
      } );

      if( records != null && !records.isEmpty() ) {
        final ViewSequenceWritable vlist = new ViewSequenceWritable();

        final Iterator<ViewRecord> it = records.iterator();
        ViewRecord current = it.next();
        ViewRecord next = null;

        final List<Integer> indexes = new ArrayList<>();
        int index = 0;
        indexes.add( index );

        while( it.hasNext() ) {
          next = it.next();
          index++;

          if( next.getStartTime().getTime() -
              current.getEndTime().getTime() > delta ) {
            indexes.add( index );
          }
          current = next;
        }
        indexes.add( index + 1 );

        for( int i = 0; i < indexes.size() - 1; i++ ) {
          final int start = indexes.get( i );
          final int end = indexes.get( i + 1 );

          final ViewRecord main = records.get( start );

          final Group g = new Group();
          g.setGroupId( groupCount );
          g.setFamilyId( main.getFamilyId() );

          final GroupView gv = new GroupView();
          gv.setProgramId( main.getProgramId() );
          gv.setEpgChannelId( main.getEpgChannelId() );

          for( int j = start; j < end; j++ ) {
            final ViewRecord r = records.get( j );
            g.addUser( r.getUserId() );
            gv.setIntervalStart(
              gv.getIntervalStart() == null ? r.getStartTime() :
                gv.getIntervalStart().getTime() < r.getStartTime().getTime() ?
                  gv.getIntervalStart() : r.getStartTime() );
            gv.setIntervalEnd(
              gv.getIntervalEnd() == null ? r.getEndTime() :
                gv.getIntervalEnd().getTime() > r.getEndTime().getTime() ?
                  gv.getIntervalEnd() : r.getEndTime() );
          }

          determineGroupType( userAges, g );
          gv.setGroup( g );
          determineViewTimeSlot( gv );
          vlist.addView( gv );
          groupCount++;

          result.put( g.getGroupId(), gv );
        }
      }

      progress++;
      if( progress % ( candidateGroupViews.entrySet().size() / 100 ) == 0 ) {
        System.out.printf( "%d%%...", progress / ( candidateGroupViews.entrySet().size() / 100 ) );
      }//*/
    }
    System.out.printf( "Finished!%n" );
    return result;
  }


  /**
   * The method groups the view records by family, program and channel
   * identifier.
   *
   * @param records
   * @return
   */

  public static Map<Key, List<ViewRecord>> candidateGroupViews( List<ViewRecord> records ) {
    if( records == null ) {
      throw new NullPointerException();
    }

    final Map<Key, List<ViewRecord>> result = new HashMap<>();

    for( ViewRecord r : records ) {
      final Key k = new Key();
      k.familyId = r.getFamilyId();
      k.programId = r.getProgramId();
      //k.groupId = r.groupId;
      k.epgChannelId = r.getEpgChannelId();

      List<ViewRecord> v = result.get( k );
      if( v == null ) {
        v = new ArrayList<>();
      }

      v.add( r );
      result.put( k, v );
    }

    return result;
  }


  /**
   * The method discards records regarding views with a duration less than the
   * parameter <code>shortView</code>.
   *
   * @param lines
   * @return
   * @throws ParseException
   */

  public static List<ViewRecord> discardShortViews( List<String> lines )
    throws ParseException {

    if( lines == null ) {
      throw new NullPointerException();
    }

    final List<ViewRecord> result = new ArrayList<>( lines.size() );

    for( String l : lines ) {
      final StringTokenizer tk = new StringTokenizer( l, delimiter );
      final ViewRecord record = new ViewRecord();

      // id
      if( tk.hasMoreTokens() ) {
        tk.nextToken();
      }

      if( tk.hasMoreTokens() ) {
        record.setUserId( tk.nextToken() );
      }
      if( tk.hasMoreTokens() ) {
        record.setFamilyId( tk.nextToken() );
      }
      if( tk.hasMoreTokens() ) {
        record.setGroupId( tk.nextToken() );
      }
      if( tk.hasMoreTokens() ) {
        record.setProgramId( tk.nextToken() );
      }
      if( tk.hasMoreTokens() ) {
        record.setEpgChannelId( tk.nextToken() );
      }

      // 2003-01-18 22:10:00
      final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd hh:mm:ss" );
      if( tk.hasMoreTokens() ) {
        record.setStartTime( f.parse( tk.nextToken().replace( "\"", "" ) ) );
      }
      if( tk.hasMoreTokens() ) {
        record.setEndTime( f.parse( tk.nextToken().replace( "\"", "" ) ) );
      }

      // getTime() => milliseconds
      if( record.getEndTime().getTime() - record.getStartTime().getTime() >= shortView ) {
        result.add( record );
      }
    }
    return result;
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param contexts
   * @return
   */

  public static String printGroupTypeEvolution( Set<DynamicContext> contexts ) {
    if( contexts == null ) {
      throw new NullPointerException();
    }

    final StringBuilder b = new StringBuilder();
    for( DynamicContext dc : contexts ) {


      final Iterator<String> sit = dc.getStart().getAgeClassSet().iterator();
      while( sit.hasNext() ) {
        b.append( sit.next() );
        if( sit.hasNext() ) {
          b.append( "-" );
        }
      } // end age classes
      b.append( separator );
      b.append( dc.getStart().getTimeSlot() ); // time slot
      b.append( separator );

      final Iterator<String> tit = dc.getEnd().getAgeClassSet().iterator();
      while( tit.hasNext() ) {
        b.append( tit.next() );
        if( tit.hasNext() ) {
          b.append( "-" );
        }
      } // end age classes
      b.append( separator );
      b.append( dc.getEnd().getTimeSlot() ); // time slot
      b.append( separator );

      b.append( dc.getProbability() );
      b.append( format( "%n" ) );
    } // end context

    return b.toString();
  }

  /**
   * MISSING_COMMENT
   *
   * @param evolution
   * @return
   */

  public static String printGroupEvolution( Set<List<Group>> evolution ) {
    if( evolution == null ) {
      throw new NullPointerException();
    }

    final StringBuilder b = new StringBuilder();
    for( List<Group> e : evolution ) {
      for( int i = 0; i < e.size(); i++ ) {
        // group id
        b.append( e.get( i ).getGroupId() );
        b.append( separator );
        b.append( e.get( i ).getFamilyId() );
        b.append( separator );

        // group composition
        final Iterator<String> uit = e.get( i ).getUsers().iterator();
        while( uit.hasNext() ) {
          b.append( uit.next() );
          if( uit.hasNext() ) {
            b.append( "-" );
          }
        }
        b.append( separator );

        // group type set
        final Iterator<String> tit = e.get( i ).getTypeSet().iterator();
        while( tit.hasNext() ) {
          b.append( tit.next() );
          if( tit.hasNext() ) {
            b.append( "-" );
          }
        }
        b.append( separator );

        b.append( e.get( i ).getTimeSlot() );
        if( i < e.size() - 1 ) ;
        b.append( separator );
      }
      b.append( format( "%n" ) );
    }
    return b.toString();
  }

  /**
   * MISSING_COMMENT
   *
   * @param sequences
   * @return
   */

  public static String printGroupViewSequences
  ( Collection<ViewSequenceWritable> sequences ) {
    if( sequences == null ) {
      throw new NullPointerException();
    }

    final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd HH:mm" );

    final StringBuilder b = new StringBuilder();
    for( ViewSequenceWritable seq : sequences ) {
      if( seq != null && !seq.isEmpty() ) {
        final GroupView master = seq.getView( 0 );
        final List<String> users = new ArrayList<>( master.getGroup().getUsers() );
        for( int i = 0; i < users.size() - 1; i++ ) {
          b.append( users.get( i ) );
          b.append( "-" );
        }
        b.append( users.get( users.size() - 1 ) );
        b.append( separator );
        b.append( master.getGroup().getFamilyId() );
        b.append( separator );
        b.append( master.getGroup().getGroupId() );
        b.append( separator );
        final List<String> types = new ArrayList<>( master.getGroup().getTypeSet() );
        for( int i = 0; i < types.size() - 1; i++ ) {
          b.append( types.get( i ) );
          b.append( "-" );
        }
        b.append( types.get( types.size() - 1 ) );
        b.append( separator );

        for( GroupView g : seq.getSequence() ) {
          b.append( g.getEpgChannelId() );
          b.append( separator );
          b.append( g.getProgramId() );
          b.append( separator );
          b.append( f.format( master.getIntervalStart() ) );
          b.append( separator );
          b.append( f.format( master.getIntervalEnd() ) );
          b.append( separator );
          b.append( g.getTimeSlot() );
          b.append( separator );
        }
        b.append( String.format( "%n" ) );
      }
    }
    return b.toString();
  }


  /**
   * MISSING_COMMENT
   *
   * @param sequences
   * @return
   */

  public static String printCommands
  ( Collection<ViewSequenceWritable> sequences ) {
    if( sequences == null ) {
      throw new NullPointerException();
    }

    final String firstLine = "#!/bin/bash%n%n";
    final String exportLine = "export HADOOP_CLASSPATH=/home/hadoop/workspace/auditel/auditel.jar%n";

    final String commandLineTemplate =
      "hadoop it.univr.auditel.TrsaAuditel trsa_auditel input output "
      + "ageClasses=%s "
      + "timeSlot=%s "
      + "maxDuration=%s "
      + "durationOffset=15 "
      + "maxPerturbations=100 "
      + "initialTemperature=8%n";

    final String mkdirLineTemplate = "mkdir output/%s%n";

    final String copyLine1Template =
      "hadoop fs -copyToLocal trsa_auditel/output/*_epf output/%s/%n";

    final String copyLine2Template =
      "hadoop fs -copyToLocal trsa_auditel/output/*_trsa output/%s/%n";

    final String removeLine1Template =
      "hadoop fs -rm -r trsa_auditel/output/*_epf%n";
    final String removeLine2Template =
      "hadoop fs -rm -r trsa_auditel/output/*_trsa%n";


    final StringBuilder b = new StringBuilder();
    b.append( format( firstLine ) );
    b.append( format( exportLine ) );

    final Iterator<ViewSequenceWritable> it = sequences.iterator();
    int i = 0;
    while( it.hasNext() ) {
      final ViewSequenceWritable seq = it.next();
      final List<GroupView> views = seq.getSequence();
      if( views != null ) {
        final GroupView start = views.get( 0 );
        final Group group = start.getGroup();
        if( group != null ) {
          // ageClasses
          final Set<String> types = group.getTypeSet();
          final Iterator<String> tit = types.iterator();
          final StringBuilder ageClasses = new StringBuilder();
          while( tit.hasNext() ) {
            ageClasses.append( tit.next() );
            if( tit.hasNext() ) {
              ageClasses.append( "," );
            }
          }

          // timeSlot
          final String timeSlot = start.getTimeSlot();

          // maxDuration in minutes
          final GroupView end = views.get( views.size() - 1 );
          final int duration =
            (int) ceil( ( end.getIntervalEnd().getTime() -
                          start.getIntervalStart().getTime() ) /
                        ( 1000 * 60.0 ) );

          if( ageClasses.length() > 0 && timeSlot.length() > 0 && duration > 0 ) {
            b.append( String.format( mkdirLineTemplate, i ) );
            b.append( String.format( commandLineTemplate, ageClasses, timeSlot, duration ) );
            b.append( String.format( copyLine1Template, i ) );
            b.append( String.format( copyLine2Template, i ) );
            b.append( String.format( removeLine1Template, i ) );
            b.append( String.format( removeLine2Template, i ) );
          }
          i++;
        }
      }
    }

    return b.toString();
  }

  // ===========================================================================

  /**
   * MISSING_COMMENT
   *
   * @param file
   * @param header
   * @return
   */

  public static List<String> readLines( File file, boolean header ) {
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

  public static void writeFile( String dir, String filename, String content )
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

// ===========================================================================

  /**
   * MISSING_COMMENT
   */

  public static class Key {
    String familyId;
    String programId;
    String epgChannelId;
    String groupId;

    Key() {
      familyId = null;
      programId = null;
      epgChannelId = null;
      groupId = null;
    }

    @Override
    public boolean equals( Object o ) {
      if( this == o ) return true;
      if( !( o instanceof Key ) ) return false;

      Key key = (Key) o;

      if( epgChannelId != null ? !epgChannelId.equals( key.epgChannelId ) : key.epgChannelId != null )
        return false;
      if( familyId != null ? !familyId.equals( key.familyId ) : key.familyId != null )
        return false;
      if( groupId != null ? !groupId.equals( key.groupId ) : key.groupId != null )
        return false;
      if( programId != null ? !programId.equals( key.programId ) : key.programId != null )
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = familyId != null ? familyId.hashCode() : 0;
      result = 31 * result + ( programId != null ? programId.hashCode() : 0 );
      result = 31 * result + ( epgChannelId != null ? epgChannelId.hashCode() : 0 );
      result = 31 * result + ( groupId != null ? groupId.hashCode() : 0 );
      return result;
    }
  }
}
