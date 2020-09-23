package it.univr.auditel.entities;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */

import java.io.Serializable;
import java.util.Date;

/**
 * Record in the log file.
 */

public class ViewRecord implements Serializable {
  private String userId;
  private String familyId;
  private String groupId;
  private String programId;
  private String epgChannelId;
  private Date startTime;
  private Date endTime;

  public ViewRecord() {
    userId = null;
    familyId = null;
    groupId = null;
    programId = null;
    epgChannelId = null;
    startTime = null;
    endTime = null;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId( String userId ) {
    this.userId = userId;
  }

  public String getFamilyId() {
    return familyId;
  }

  public void setFamilyId( String familyId ) {
    this.familyId = familyId;
  }

  public String getGroupId() {
    return groupId;
  }

  public void setGroupId( String groupId ) {
    this.groupId = groupId;
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
}
