/*
 * Copyright (c) 2015-2017 by Mauro Gambini, Sara Migliorini. Dept. of Computer
 * Science, University of Verona, Ca' Vignal 2, Floor 1, Room 80, Strada le
 * Grazie 15, 37134 Verona (Italy). All Rights Reserved.
 * No part of this software or any of its contents may be reproduced, copied,
 * modified, adapted or transmitted in any form or by any means, without the
 * prior written consent of the copyright holders.
 * Unless required by applicable law or agreed to in writing, the software is
 * provided "as is" without warranties of any kind, either express or implied.
 * The authors and copyright holders disclaim all warranties and conditions
 * with regard to the software, including but not limited to the warranties of
 * non-infringement, merchantability, or fitness for a particular purpose. In
 * no event shall the authors and copyright holders be liable for any claim,
 * damages or other liability, whether in an action of contract, tort or
 * otherwise, arising from, out of or in connection with the software or the
 * use or other dealings in the software.
 */

package it.univr.veronacard.entities;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class VCard implements Serializable {

  private String serial;
  private Date activationDate;
  private String active;
  private String activationType;
  private String profile;
  private List<VcTicket> tickets;

  // === Methods ===============================================================

  public VCard() {
    this.serial = null;
    this.activationDate = null;
    this.active = null;
    this.activationType = null;
    this.profile = null;
    this.tickets = null;
  }

  // ===========================================================================

  public String getSerial() {
    return serial;
  }

  public void setSerial( String serial ) {
    this.serial = serial;
  }

  public Date getActivationDate() {
    return activationDate;
  }

  public void setActivationDate( Date activationDate ) {
    this.activationDate = activationDate;
  }

  public void setActivationDate( String activation ) {
    if( activation != null ) {
      final SimpleDateFormat f = new SimpleDateFormat( "dd-MM-yy" );
      try {
        this.activationDate = f.parse( activation );
      } catch( ParseException e ) {
        this.activationDate = null;
      }
    }
  }

  public String getActive() {
    return active;
  }

  public void setActive( String active ) {
    this.active = active;
  }

  public String getActivationType() {
    return activationType;
  }

  public void setActivationType( String activationType ) {
    this.activationType = activationType;
  }

  public String getProfile() {
    return profile;
  }

  public void setProfile( String profile ) {
    this.profile = profile;
  }

  public List<VcTicket> getTickets() {
    return tickets;
  }

  public void setTickets( List<VcTicket> tickets ) {
    this.tickets = tickets;
  }

  public void addTicket( VcTicket ticket ) {
    if( this.tickets == null ) {
      this.tickets = new ArrayList<>();
    }
    this.tickets.add( ticket );
  }
}
