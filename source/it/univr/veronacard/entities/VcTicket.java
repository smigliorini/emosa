package it.univr.veronacard.entities;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class VcTicket {

  // === Properties ============================================================

  private Date date;
  private String siteName;
  private String device;

  // === Methods ===============================================================

  public VcTicket() {
    this.date = null;
    this.siteName = null;
    this.device = null;
  }

  // ===========================================================================

  public Date getDate() {
    return date;
  }

  public void setDate( Date date ) {
    this.date = date;
  }

  public void setDate( String day, String time ) {
    if( day != null && !day.isEmpty() ) {
      final SimpleDateFormat f;
      final String stringDate;

      if( time != null && !time.isEmpty() ) {
        f = new SimpleDateFormat( "dd-MM-yy HH:mm:ss" );
        stringDate = String.format( "%s %s", day, time );
      } else {
        f = new SimpleDateFormat( "dd-MM-yy" );
        stringDate = day;
      }

      try {
        this.date = f.parse( stringDate );
      } catch( ParseException e ) {
        this.date = null;
      }
    } else {
      this.date = null;
    }
  }

  public String getSiteName() {
    return siteName;
  }

  public void setSiteName( String siteName ) {
    this.siteName = siteName;
  }

  public String getDevice() {
    return device;
  }

  public void setDevice( String device ) {
    this.device = device;
  }
}
