package it.univr.auditel.entities;

import java.io.Serializable;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class DynamicContext implements Serializable {

  private GContext start;
  private GContext end;
  private double probability;

  public DynamicContext() {
    start = null;
    end = null;
    probability = 0;
  }

  public GContext getStart() {
    return start;
  }

  public void setStart( GContext start ) {
    this.start = start;
  }

  public GContext getEnd() {
    return end;
  }

  public void setEnd( GContext end ) {
    this.end = end;
  }

  public double getProbability() {
    return probability;
  }

  public void setProbability( double probability ) {
    this.probability = probability;
  }

  @Override
  public boolean equals( Object o ) {
    if( this == o ) return true;
    if( !( o instanceof DynamicContext ) ) return false;

    DynamicContext that = (DynamicContext) o;

    if( Double.compare( that.probability, probability ) != 0 ) return false;
    if( end != null ? !end.equals( that.end ) : that.end != null )
      return false;
    if( start != null ? !start.equals( that.start ) : that.start != null )
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = start != null ? start.hashCode() : 0;
    result = 31 * result + ( end != null ? end.hashCode() : 0 );
    temp = Double.doubleToLongBits( probability );
    result = 31 * result + (int) ( temp ^ ( temp >>> 32 ) );
    return result;
  }
}
