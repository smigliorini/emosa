package it.univr.veronacard.shadoop.core;

import com.vividsolutions.jts.geom.Geometry;
import java.io.Serializable;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class Step implements Serializable {

  // ===========================================================================

  public static enum Attributes {
    ORIGIN,
    DESTINATION,
    TRAVEL_MODE,
    PATH,
    DURATION,
    DISTANCE;
  }

  // === Properties ============================================================

  private String origin;
  private String destination;
  private String travelMode;
  private Geometry path;
  private Integer duration;
  private Integer distance;

  // === Methods ===============================================================

  public Step() {
    origin = null;
    destination = null;
    travelMode = null;
    path = null;
    duration = null;
    distance = null;
  }

  public Step
    ( String origin,
      String destination,
      String travelMode,
      Geometry path,
      Integer duration,
      Integer distance ) {

    this.origin = origin;
    this.destination = destination;
    this.travelMode = travelMode;
    this.path = path;
    this.duration = duration;
    this.distance = distance;
  }

  // ===========================================================================

  public String getOrigin() {
    return origin;
  }

  public void setOrigin( String origin ) {
    this.origin = origin;
  }

  public String getDestination() {
    return destination;
  }

  public void setDestination( String destination ) {
    this.destination = destination;
  }

  public String getTravelMode() {
    return travelMode;
  }

  public void setTravelMode( String travelMode ) {
    this.travelMode = travelMode;
  }

  public Geometry getPath() {
    return path;
  }

  public void setPath( Geometry path ) {
    this.path = path;
  }

  public Integer getDuration() {
    return duration;
  }

  public void setDuration( Integer duration ) {
    this.duration = duration;
  }

  public Integer getDistance() {
    return distance;
  }

  public void setDistance( Integer distance ) {
    this.distance = distance;
  }
}
