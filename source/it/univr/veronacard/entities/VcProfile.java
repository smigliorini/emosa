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
import java.util.Date;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class VcProfile implements Serializable {

  private String id;
  private String name;
  private Boolean enabled;
  private Integer days;
  private Date date;
  private String description;

  public VcProfile() {
    this.id = null;
    this.name = null;
    this.enabled = null;
    this.days = null;
    this.date = null;
    this.description = null;
  }

  public String getId() {
    return id;
  }

  public void setId( String id ) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled( Boolean enabled ) {
    this.enabled = enabled;
  }

  public Integer getDays() {
    return days;
  }

  public void setDays( Integer days ) {
    this.days = days;
  }

  public Date getDate() {
    return date;
  }

  public void setDate( Date date ) {
    this.date = date;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription( String description ) {
    this.description = description;
  }
}
