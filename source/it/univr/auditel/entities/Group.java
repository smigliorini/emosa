package it.univr.auditel.entities;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Group of users that have performed some viewings together.
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class Group implements Serializable {
  private Integer groupId;
  private String familyId;
  private Set<String> users;
  private Set<String> typeSet;
  private String timeSlot;

  public Group() {
    groupId = null;
    familyId = null;
    users = new HashSet<>();
    typeSet = new HashSet<>();
    timeSlot = null;
  }

  public Group( Group g ){
    this.groupId = g.groupId;
    this.familyId = g.familyId;
    this.users = new HashSet<>( g.users );
    this.typeSet = new HashSet<>( g.typeSet );
    this.timeSlot = g.timeSlot;
  }

  public Integer getGroupId() {
    return groupId;
  }

  public void setGroupId( Integer groupId ) {
    this.groupId = groupId;
  }

  public String getFamilyId() {
    return familyId;
  }

  public void setFamilyId( String familyId ) {
    this.familyId = familyId;
  }

  public Set<String> getUsers() {
    return users;
  }

  public void setUsers( Set<String> users ) {
    this.users = users;
  }

  public Set<String> getTypeSet() {
    return typeSet;
  }

  public void setTypeSet( Set<String> typeSet ) {
    this.typeSet = typeSet;
  }

  public String getTimeSlot() {
    return timeSlot;
  }

  public void setTimeSlot( String timeSlot ) {
    this.timeSlot = timeSlot;
  }

  public void addUser( String user ) {
    if( users == null ) {
      users = new HashSet<>();
    }
    users.add( user );
  }

  public void addType( String type ) {
    if( typeSet == null ) {
      typeSet = new HashSet<>();
    }
    typeSet.add( type );
  }
}