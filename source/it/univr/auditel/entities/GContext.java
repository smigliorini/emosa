package it.univr.auditel.entities;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class GContext implements Serializable {
  private Set<String> ageClassSet;
  private String timeSlot;

  public GContext() {
    ageClassSet = new HashSet<>();
    timeSlot = null;
  }

  public Set<String> getAgeClassSet() {
    return ageClassSet;
  }

  public void setAgeClassSet( Set<String> ageClassSet ) {
    this.ageClassSet = ageClassSet;
  }

  public String getTimeSlot() {
    return timeSlot;
  }

  public void setTimeSlot( String timeSlot ) {
    this.timeSlot = timeSlot;
  }

  public void addAgeClass( String ageClass ) {
    if( ageClassSet == null ) {
      ageClassSet = new HashSet<>();
    }
    ageClassSet.add( ageClass );
  }

  @Override
  public boolean equals( Object o ) {
    if( this == o ) return true;
    if( !( o instanceof GContext ) ) return false;

    GContext context = (GContext) o;

    if( ageClassSet != null ? !ageClassSet.equals( context.ageClassSet ) : context.ageClassSet != null )
      return false;
    if( timeSlot != null ? !timeSlot.equals( context.timeSlot ) : context.timeSlot != null )
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = ageClassSet != null ? ageClassSet.hashCode() : 0;
    result = 31 * result + ( timeSlot != null ? timeSlot.hashCode() : 0 );
    return result;
  }
}