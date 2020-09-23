package it.univr.auditel.entities;

import java.io.Serializable;
import java.util.Date;

/**
 * Viewing performed by the same group of people.
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class GroupView implements Serializable {
  private Group group;
  private String programId;
  private String epgChannelId;
  private Date intervalStart;
  private Date intervalEnd;
  private String timeSlot;

  public GroupView() {
    this.group = null;
    this.programId = null;
    this.epgChannelId = null;
    this.intervalStart = null;
    this.intervalEnd = null;
    this.timeSlot = null;
  }

  public GroupView( GroupView view ){
    this.group = new Group( view.getGroup() );
    this.programId = view.getProgramId();
    this.epgChannelId = view.getEpgChannelId();
    this.intervalStart = new Date( view.getIntervalStart().getTime() );
    this.intervalEnd = new Date( view.getIntervalEnd().getTime() );
    this.timeSlot = view.getTimeSlot();
  }

  public Group getGroup() {
    return group;
  }

  public void setGroup( Group group ) {
    this.group = group;
  }

  public String getProgramId() {
    return programId;
  }

  public void setProgramId( String programId ) {
    this.programId = programId;
  }

  public String getEpgChannelId() {
    return epgChannelId;
  }

  public void setEpgChannelId( String epgChannelId ) {
    this.epgChannelId = epgChannelId;
  }

  public Date getIntervalStart() {
    return intervalStart;
  }

  public void setIntervalStart( Date intervalStart ) {
    this.intervalStart = intervalStart;
  }

  public Date getIntervalEnd() {
    return intervalEnd;
  }

  public void setIntervalEnd( Date intervalEnd ) {
    this.intervalEnd = intervalEnd;
  }

  public String getTimeSlot() {
    return timeSlot;
  }

  public void setTimeSlot( String timeSlot ) {
    this.timeSlot = timeSlot;
  }
}
