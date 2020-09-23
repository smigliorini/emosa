package it.univr.veronacard.entities;

import com.vividsolutions.jts.geom.LineString;
import java.io.Serializable;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class TravelStep implements Serializable {

  // === Properties ============================================================

  private double startLocationLat;
  private double startLocationLng;
  private double endLocationLat;
  private double endLocationLng;
  private String travelMode;
  private double duration; // in seconds
  private String durationText;
  private double distance; // in meters
  private String distanceText;
  private int index;
  private LineString polyline;

  // === Methods ===============================================================

  public TravelStep() {
  }

  public double getStartLocationLat() {
    return startLocationLat;
  }

  public void setStartLocationLat( double startLocationLat ) {
    this.startLocationLat = startLocationLat;
  }

  public double getStartLocationLng() {
    return startLocationLng;
  }

  public void setStartLocationLng( double startLocationLng ) {
    this.startLocationLng = startLocationLng;
  }

  public double getEndLocationLat() {
    return endLocationLat;
  }

  public void setEndLocationLat( double endLocationLat ) {
    this.endLocationLat = endLocationLat;
  }

  public double getEndLocationLng() {
    return endLocationLng;
  }

  public void setEndLocationLng( double endLocationLng ) {
    this.endLocationLng = endLocationLng;
  }

  public String getTravelMode() {
    return travelMode;
  }

  public void setTravelMode( String travelMode ) {
    this.travelMode = travelMode;
  }

  public double getDuration() {
    return duration;
  }

  public void setDuration( double duration ) {
    this.duration = duration;
  }

  public String getDurationText() {
    return durationText;
  }

  public void setDurationText( String durationText ) {
    this.durationText = durationText;
  }

  public double getDistance() {
    return distance;
  }

  public void setDistance( double distance ) {
    this.distance = distance;
  }

  public String getDistanceText() {
    return distanceText;
  }

  public void setDistanceText( String distanceText ) {
    this.distanceText = distanceText;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex( int index ) {
    this.index = index;
  }

  public LineString getPolyline() {
    return polyline;
  }

  public void setPolyline( LineString polyline ) {
    this.polyline = polyline;
  }
}
