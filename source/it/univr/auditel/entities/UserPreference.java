package it.univr.auditel.entities;

import java.io.Serializable;
import java.util.*;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class UserPreference implements Serializable {

  private String user;
  private String familyId;
  private Set<String> groupTypeSet;
  private String timeSlot;
  private String dayOfWeek;
  private String channelId;
  private Double preference;

  public UserPreference(){
    user = null;
    familyId = null;
    groupTypeSet = new HashSet<>();
    timeSlot = null;
    dayOfWeek = null;
    channelId = null;
    preference = null;
  }

  public String getUser(){
    return user;
  }

  public void setUser( String user ){
    this.user = user;
  }

  public String getFamilyId(){
    return familyId;
  }

  public void setFamilyId( String familyId ){
    this.familyId = familyId;
  }

  public Set<String> getGroupTypeSet(){
    return groupTypeSet;
  }

  public void setGroupTypeSet( Set<String> groupTypeSet ){
    this.groupTypeSet = groupTypeSet;
  }

  public void setGroupTypeList( String groupTypeString ){
    final StringTokenizer tk = new StringTokenizer( groupTypeString, "-" );
    groupTypeSet = new HashSet<>();

    while( tk.hasMoreTokens() ){
      //final String current = ;
      groupTypeSet.add( tk.nextToken() );
    }
  }

  public String getTimeSlot(){
    return timeSlot;
  }

  public void setTimeSlot( String timeSlot ){
    this.timeSlot = timeSlot;
  }

  public String getDayOfWeek(){
    return dayOfWeek;
  }

  public void setDayOfWeek( String dayOfWeek ){
    this.dayOfWeek = dayOfWeek;
  }

  public String getChannelId(){
    return channelId;
  }

  public void setChannelId( String channelId ){
    this.channelId = channelId;
  }

  public Double getPreference(){
    return preference;
  }

  public void setPreference( Double preference ){
    this.preference = preference;
  }
}
