package it.univr.auditel.shadoop.core;

import it.univr.auditel.entities.GContext;
import it.univr.auditel.entities.Group;
import it.univr.auditel.entities.GroupView;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import static java.lang.Integer.parseInt;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class ViewSequenceWritable implements Writable {

  private List<GroupView> sequence;

  public ViewSequenceWritable() {
    sequence = new ArrayList<>();
  }

  public List<GroupView> getSequence() {
    return sequence;
  }

  public void setSequence( List<GroupView> sequence ) {
    this.sequence = sequence;
  }

  public void addView( GroupView view ) {
    if( sequence == null ) {
      sequence = new ArrayList<>();
    }
    sequence.add( view );
  }

  public GroupView getView( int index ) {
    if( sequence == null ) {
      throw new IllegalArgumentException();
    } else {
      if( index < 0 || index >= sequence.size() ) {
        throw new IllegalArgumentException();
      } else {
        return sequence.get( index );
      }
    }
  }

  public int size() {
    if( sequence == null ) {
      return 0;
    } else {
      return sequence.size();
    }
  }

  public boolean isEmpty() {
    if( sequence == null ) {
      return true;
    } else {
      return sequence.isEmpty();
    }
  }

  @Override
  public void write( DataOutput dataOutput ) throws IOException {
    if( dataOutput == null ) {
      throw new NullPointerException();
    }

    if( sequence != null && !sequence.isEmpty() ) {
      final Group group = sequence.get( 0 ).getGroup();
      dataOutput.writeInt( group.getGroupId() );
      dataOutput.writeUTF( group.getFamilyId() );

      dataOutput.writeInt( group.getUsers().size() );
      final Iterator<String> utk = group.getUsers().iterator();
      while( utk.hasNext() ) {
        dataOutput.writeUTF( utk.next() );
      }

      dataOutput.writeInt( group.getTypeSet().size() );
      final Iterator<String> ttk = group.getTypeSet().iterator();
      while( ttk.hasNext() ) {
        dataOutput.writeUTF( ttk.next() );
      }

      dataOutput.writeUTF( group.getTimeSlot() );

      dataOutput.writeInt( sequence.size() );
      final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd hh:mm" );
      for( GroupView view : sequence ) {
        dataOutput.writeUTF( view.getProgramId() );
        dataOutput.writeUTF( view.getEpgChannelId() );
        dataOutput.writeUTF( f.format( view.getIntervalStart() ) );
        dataOutput.writeUTF( f.format( view.getIntervalEnd() ) );
        dataOutput.writeUTF( view.getTimeSlot() );
      }
    }

  }

  @Override
  public void readFields( DataInput dataInput ) throws IOException {
    if( dataInput == null ) {
      throw new NullPointerException();
    }

    final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd hh:mm" );

    final Group group = new Group();
    group.setGroupId( dataInput.readInt() );
    group.setFamilyId( dataInput.readUTF() );
    final int numUsers = dataInput.readInt();
    for( int i = 0; i < numUsers; i++ ) {
      group.addUser( dataInput.readUTF() );
    }
    final int numTypes = dataInput.readInt();
    for( int i = 0; i < numTypes; i++ ) {
      group.addType( dataInput.readUTF() );
    }

    group.setTimeSlot( dataInput.readUTF() );

    final int numViews = dataInput.readInt();
    for( int i = 0; i < numViews; i++ ) {
      final GroupView gv = new GroupView();
      gv.setGroup( new Group( group ) );
      gv.setProgramId( dataInput.readUTF() );
      gv.setEpgChannelId( dataInput.readUTF() );
      final String st = dataInput.readUTF();
      try {
        gv.setIntervalStart( f.parse( st ) );
      } catch( ParseException e ) {
        System.out.printf( "Unable to parse date \"%s\".%n", st );
      }
      final String et = dataInput.readUTF();
      try {
        gv.setIntervalEnd( f.parse( et ) );
      } catch( ParseException e ) {
        System.out.printf( "Unable to parse date \"%s\".%n", st );
      }
      gv.setTimeSlot( dataInput.readUTF() );

      this.addView( gv );
    }

  }

  /**
   * MISSING_COMMENT
   *
   * @param text
   */
  public void fromText( Text text ) {
    if( text == null ) {
      throw new NullPointerException();
    }

    final String line = text.toString();
    final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd HH:mm" );

    final Group g = new Group();
    GroupView v = new GroupView();

    final StringTokenizer tk = new StringTokenizer( line, "," );
    int i = 0, j = 0;
    while( tk.hasMoreTokens() ) {
      final String current = tk.nextToken();

      if( i == 0 ) { // list of users
        final StringTokenizer itk = new StringTokenizer( current, "-" );
        while( itk.hasMoreTokens() ) {
          g.addUser( itk.nextToken() );
        }
        i++;

      } else if( i == 1 ) { // family id
        g.setFamilyId( current );
        i++;

      } else if( i == 2 ) { // group id
        try {
          g.setGroupId( parseInt( current ) );
        } catch( NumberFormatException e ) {
          System.out.printf( "Unable to parse group id: \"%s\", in line \"%s\".%n", current, line );
        }
        i++;

      } else if( i == 3 ) { // type set
        final String typeSet = current;
        final StringTokenizer itk = new StringTokenizer( typeSet, "-" );
        while( itk.hasMoreTokens() ) {
          g.addType( itk.nextToken() );
        }
        i++;

      } else if( i >= 4 ) { // view sequence
        if( j == 0 ) { // channel id
          v.setEpgChannelId( current );
          j++;

        } else if( j == 1 ) {
          v.setProgramId( current );
          j++;

        } else if( j == 2 ) {
          try {
            v.setIntervalStart( f.parse( current ) );
          } catch( java.text.ParseException e ) {
            System.out.printf( "Unable to parse start date: \"%s\", in line \"%s\".%n", current, line );
          }
          j++;

        } else if( j == 3 ) {
          try {
            v.setIntervalEnd( f.parse( current ) );
          } catch( java.text.ParseException e ) {
            System.out.printf( "Unable to parse end date: \"%s\", in line \"%s\".%n", current, line );
          }
          j++;

        } else if( j == 4 ) {
          final String timeSlot = current;
          v.setTimeSlot( timeSlot );
          v.setGroup( new Group( g ) );
          v.getGroup().setTimeSlot( timeSlot );
          addView( v );
          // reset all
          j = 0;
          v = new GroupView();
        }
        //i++;
      }//*/
    }
  }

  /**
   * MISSING_COMMENT
   *
   * @return
   */

  public String toString() {
    final StringBuilder b = new StringBuilder();

    if( sequence != null && !sequence.isEmpty() ) {
      final Group group = sequence.get( 0 ).getGroup();
      b.append( group.getGroupId() );
      b.append( "\t" );
      b.append( group.getFamilyId() );
      b.append( "\t" );

      final Iterator<String> utk = group.getUsers().iterator();
      while( utk.hasNext() ) {
        b.append( utk.next() );
        if( utk.hasNext() ) {
          b.append( "," );
        }
      }
      b.append( "\t" );

      final Iterator<String> ttk = group.getTypeSet().iterator();
      while( ttk.hasNext() ) {
        b.append( ttk.next() );
        if( ttk.hasNext() ) {
          b.append( "," );
        }
      }
      b.append( "\t" );

      b.append( group.getTimeSlot() );
      b.append( "\t" );

      final DateFormat f = new SimpleDateFormat( "yyyy-MM-dd hh:mm" );
      final Iterator<GroupView> git = sequence.iterator();
      while( git.hasNext() ) {
        final GroupView view = git.next();
        b.append( view.getProgramId() );
        b.append( "-" );
        b.append( view.getEpgChannelId() );
        b.append( "-" );
        b.append( f.format( view.getIntervalStart() ) );
        b.append( "-" );
        b.append( f.format( view.getIntervalEnd() ) );
        b.append( "-" );
        b.append( view.getTimeSlot() );
        if( git.hasNext() ) {
          b.append( "," );
        }
      }
    }
    return b.toString();
  }


  /**
   * The method returns true if the sequence regards a group and a time slot
   * prescribed by the initial context contained in <code>initialContext</code>.
   *
   * @param initialContext
   * @param maxDuration
   * @param durationOffset
   * @return
   */

  public boolean checkSequence( GContext initialContext, int maxDuration, int durationOffset ) {
    if( !isEmpty() ) {
      final GroupView start = sequence.get( 0 );
      final GroupView end = sequence.get( sequence.size() - 1 );

      // in minutes
      // int duration = (int) ( ( end.getIntervalEnd().getTime() -
      //                         start.getIntervalStart().getTime() ) / ( 1000 * 60 ) );

      // TODO: day of week
      if( start.getGroup().getTypeSet().equals( initialContext.getAgeClassSet() ) &&
          start.getGroup().getTimeSlot().equals( initialContext.getTimeSlot() )
          //&&
          //duration <= maxDuration + durationOffset
        ) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @return
   */

  public String getKey() {
    final StringBuilder b = new StringBuilder();
    b.append( "(" );
    if( sequence != null && !sequence.isEmpty() ) {
      final GroupView start = sequence.get( 0 );
      final Iterator<String> it = start.getGroup().getTypeSet().iterator();
      while( it.hasNext() ) {
        b.append( it.next() );
        if( it.hasNext() ) {
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
   * The method returns the duration of the sequence in minutes.
   *
   * @return
   */

  public int getDuration() {
    if( sequence == null || sequence.isEmpty() ) {
      return 0;
    } else {
      final GroupView start = sequence.get( 0 );
      final GroupView end = sequence.get( sequence.size() - 1 );
      return (int)
        ( ( end.getIntervalEnd().getTime() -
            start.getIntervalStart().getTime() ) /
          ( 1000 * 60 ) );
    }
  }

}
