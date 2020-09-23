package it.univr.veronacard.entities;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public enum VcProfileEnum {

  VC24,
  VC48,
  VC72;

  public static VcProfileEnum fromText( String p ){
    if( p == null ){
      return null;
    }

    if( p.equals( VC24.name() )){
      return VC24;
    } else if( p.equals( VC48.name() )){
      return VC48;
    } else if( p.equals( VC72.name() )){
      return VC72;
    } else {
      return null;
    }
  }

  public static VcProfileEnum getProfile( String p ){
    if( p == null ){
      return null;
    }

    if( p.equals( "vrcard2-24" ) || p.equals( "24 Ore" )){
      return VC24;
    } else if( p.equals( "vrcard2-48" )){
      return VC48;
    } else if( p.equals( "72 Ore" )){
      return VC72;
    }
    else return null;
  }

  /**
   * Returns the duration in seconds.
   *
   * @param p
   * @return
   */

  /*public static int getMaxDuration( String p ){
    final VcProfileEnum prof = getProfile( p );
    return getMaxDuration( prof );
  }//*/

  /**
   * Returns the maximum duration in seconds.
   *
   * @param prof
   * @return
   */

  /*public static int getMaxDuration( VcProfileEnum prof ){
    if( prof == null ){
      return 0;
    } else if( prof == VC24 ){
      //return 24;
      return 8 * 60 * 60;
    } else if( prof == VC48 ){
      //return 48;
      return 8 * 2 * 60 * 60;
    } else if( prof == VC72 ){
      //return 72;
      return 8 * 3 * 60 * 60;
    } else {
      return 0;
    }
  }

  public static int getMinDuration( VcProfileEnum prof ){
    if( prof == null ){
      return 0;
    } else if( prof == VC24 ){
      //return 24;
      return 6 * 60 * 60;
    } else if( prof == VC48 ){
      //return 48;
      return 6 * 2 * 60 * 60;
    } else if( prof == VC72 ){
      //return 72;
      return 6 * 3 * 60 * 60;
    } else {
      return 0;
    }
  }//*/
}
