package it.univr.veronacard.entities;

import static java.lang.String.format;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class VcSiteInfo {

  public static int defaultVisitingTime = 30; // minutes

  /**
   * MISSING_COMMENT
   *
   * @param sitename
   * @return
   */

  public static String getSiteCode( String sitename ){
    if( sitename == null ) {
      throw new NullPointerException();
    }

    switch( sitename.toLowerCase().trim() ){
      case "arena":
        return "1";
      case "san zeno":
        return "2";
      case "casa giulietta":
        return "3";
      case "centro fotografia":
        return "4";
      case "san fermo":
        return "5";
      case "santa anastasia":
        return "6";
      case "duomo":
        return "7";
      case "museo miniscalchi":
        return "8";
      case "museo africano":
        return "9";
      case "museo radio":
        return "10";
      case "castelvecchio":
        return "11";
      case "museo storia":
        return "12";
      case "museo lapidario":
        return "13";
      case "teatro romano":
        return "14";
      case "tomba giulietta":
        return "15";
      case "torre lamberti":
        return "16";
      case "amo":
        return "17";
      case "giardino giusti":
        return "18";
      case "sighseeing":
        return "23";
      case "verona tour":
        return "26";
      case "palazzo della ragione":
        return "27";
      case "museo conte":
        return "28";
      default:
        throw new IllegalArgumentException
          ( format( "Unrecognized site: \"%s\"", sitename ) );
    }
  }


  /**
   * MISSING_COMMENT
   *
   * @param sitecode
   * @return
   */

  public static String getSiteName( String sitecode ){
    if( sitecode == null ) {
      throw new NullPointerException();
    }

    switch( sitecode.toLowerCase().trim() ){
      case "1":
        return "arena";
      case "2":
        return "san zeno";
      case "3":
        return "casa giulietta";
      case "4":
        return "centro fotografia";
      case "5":
        return "san fermo";
      case "6":
        return "santa anastasia";
      case "7":
        return "duomo";
      case "8":
        return "museo miniscalchi";
      case "9":
        return "museo africano";
      case "10":
        return "museo radio";
      case "11":
        return "castelvecchio";
      case "12":
        return "museo storia";
      case "13":
        return "museo lapidario";
      case "14":
        return "teatro romano";
      case "15":
        return "tomba giulietta";
      case "16":
        return "torre lamberti";
      case "17":
        return "amo";
      case "18":
        return "giardino giusti";
      case "23":
        return "sighseeing";
      case "26":
        return "verona tour";
      case "27":
        return "palazzo della ragione";
      case "28":
        return "museo conte";
      default:
        throw new IllegalArgumentException
          ( format( "Unrecognized site: \"%s\"", sitecode ) );
    }
  }

  // index = siteId - 1
  // stay time in seconds!
  public static int stayTime[] = new int[]{
    2   * 60 * 60, // Arena
    1,5 * 60 * 60, // Basilica di San Zeno
    1   * 60 * 60, // Casa di Giulietta
    1   * 60 * 60, // Centro Intern. di Fotografia Scavi Scaligeri
    1,5 * 60 * 60, // Chiesa di San Fermo
    1   * 60 * 60, // Chiesa di Santa Anastasia
    2   * 60 * 60, // Complesso del Duomo
    1,5 * 60 * 60, // Fondazione Museo Miniscalchi Erizzo
    1   * 60 * 60, // Museo Africano
    45 * 60, // Museo della Radio d'Epoca
    1,5 * 60 * 60, // Museo di Castelvecchio
    2   * 60 * 60, // Museo di Storia Naturale
    2   * 60 * 60, // Museo Lapidario Maffeiano
    2,5 * 60 * 60, // Teatro Romano e Museo Archeologico
    1,5 * 60 * 60, // Tomba di Giulieeta Museo degli Affreschi
    45 * 60, // Torre dei Lamberti
    1   * 60 * 60, // Arena Museo Opera
    1   * 60 * 60, // Giardino Giusti
    0,
    0,
    0,
    0,
    4 * 60 * 60, // City Sightseeing
    0,
    0,
    2 * 60 * 60, // Verona Tour
    1 * 60 * 60, // Palazzo della Ragione
    1 * 60 * 60  // Museo Conte
  };

  // waiting time in seconds!
  public static int waitingTime[][] = new int[][]{
    {
      10  * 60,
      15  * 60,
      20  * 60,
      0,5 * 60,
      1,5 * 60,
      4   * 60,
      1,5 * 60,
      4   * 60,
      3   * 60,
      40  * 60,
      5   * 60,
      10  * 60,
      1   * 60,
      0   * 60,
      1   * 60,
      3   * 60,
      1   * 60,
      4   * 60,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      4 * 60,
      4 * 60
    },
    {
      1  * 60 * 60,
      13  * 60,
      10  * 60,
      0   * 60,
      1   * 60,
      14  * 60,
      10  * 60,
      2   * 60 * 60,
      7   * 60,
      8   * 60,
      4   * 60,
      12  * 60,
      20  * 60,
      4   * 60,
      3   * 60,
      10  * 60,
      3   * 60,
      50  * 60,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      14 * 60,
      24 * 60
    },

  };

}
