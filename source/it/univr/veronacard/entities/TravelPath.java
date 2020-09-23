package it.univr.veronacard.entities;

import com.vividsolutions.jts.geom.LineString;
import java.util.List;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class TravelPath {

  private String originId;
  private String destinationId;
  private List<TravelStep> steps;
  private String travelMode;
  private LineString polyline;

  public TravelPath() {
  }

  public String getOriginId() {
    return originId;
  }

  public void setOriginId( String originId ) {
    this.originId = originId;
  }

  public String getDestinationId() {
    return destinationId;
  }

  public void setDestinationId( String destinationId ) {
    this.destinationId = destinationId;
  }

  public List<TravelStep> getSteps() {
    return steps;
  }

  public void setSteps( List<TravelStep> steps ) {
    this.steps = steps;
  }

  public String getTravelMode() {
    return travelMode;
  }

  public void setTravelMode( String travelMode ) {
    this.travelMode = travelMode;
  }

  public LineString getPolyline() {
    return polyline;
  }

  public void setPolyline( LineString polyline ) {
    this.polyline = polyline;
  }
}
