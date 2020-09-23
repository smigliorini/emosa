package it.univr.auditel.entities;

import java.io.Serializable;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class UserPreference implements Serializable {

  private String user;
  private String familyId;
  private String timeSlot;
  private String dayOfWeek;
  private String channelId;
  private Double preference;

  public UserPreference() {
    user = null;
    familyId = null;
    timeSlot = null;
    dayOfWeek = null;
    channelId = null;
    preference = null;
  }

  public String getUser() {
    return user;
  }

  public void setUser( String user ) {
    this.user = user;
  }

  public String getFamilyId() {
    return familyId;
  }

  public void setFamilyId( String familyId ) {
    this.familyId = familyId;
  }

  public String getTimeSlot() {
    return timeSlot;
  }

  public void setTimeSlot( String timeSlot ) {
    this.timeSlot = timeSlot;
  }

  public String getDayOfWeek() {
    return dayOfWeek;
  }

  public void setDayOfWeek( String dayOfWeek ) {
    this.dayOfWeek = dayOfWeek;
  }

  public String getChannelId() {
    return channelId;
  }

  public void setChannelId( String channelId ) {
    this.channelId = channelId;
  }

  public Double getPreference() {
    return preference;
  }

  public void setPreference( Double preference ) {
    this.preference = preference;
  }
}
