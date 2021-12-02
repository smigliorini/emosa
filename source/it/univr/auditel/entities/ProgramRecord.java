package it.univr.auditel.entities;

import java.io.Serializable;
import java.util.Date;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class ProgramRecord implements Serializable {

  private String programId;
  private String channelId;
  private Date startTime;
  private Date endTime;
  private Long duration;

  public ProgramRecord() {
    programId = null;
    channelId = null;
    startTime = null;
    endTime = null;
    duration = null;
  }

  public String getProgramId() {
    return programId;
  }

  public void setProgramId( String programId ) {
    this.programId = programId;
  }

  public String getChannelId() {
    return channelId;
  }

  public void setChannelId( String channelId ) {
    this.channelId = channelId;
  }

  public Date getStartTime() {
    return startTime;
  }

  public void setStartTime( Date startTime ) {
    this.startTime = startTime;
  }

  public Date getEndTime() {
    return endTime;
  }

  public void setEndTime( Date endTime ) {
    this.endTime = endTime;
  }

  public Long getDuration(){
    return duration;
  }

  public void setDuration( Long duration ){
    this.duration = duration;
  }
}
