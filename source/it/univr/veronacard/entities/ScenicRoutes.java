package it.univr.veronacard.entities;

import java.util.ArrayList;
import java.util.List;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class ScenicRoutes {



  /*private static WKTReader reader = new WKTReader( );

  private static List<Step> scenicRoutes = new ArrayList<Step>(  ){
    {
      try {
        add( new Step
               ( "PB",
                 "PE",
                 "DRIVING",
                 reader.read
                   ( "LINESTRING (10.9935595 45.4383765, 10.9944302 45.4382012, "
                     + "10.9942066 45.4374563, 10.9953029 45.43709550000001, "
                     + "10.9969684 45.437737, 10.9987864 45.4387246, "
                     + "10.9982448 45.4393191, 10.9964985 45.4410304, "
                     + "10.9972077 45.4413908, 10.9963716 45.44245129999999)" ),
                 329,1107) );
      } catch( ParseException e ) {
        // nothing here
      }

      try {
        add( new Step
               ( "PB",
                 "PTB",
                 "DRIVING",
                 reader.read
                   ( "LINESTRING (10.9935595 45.4383765, 10.9944302 45.4382012, "
                     + "10.9942066 45.4374563, 10.9953029 45.43709550000001, "
                     + "10.9969684 45.437737, 10.9987864 45.4387246, "
                     + "10.9982448 45.4393191, 10.9968714 45.4406976, "
                     + "10.9961735 45.4413428, 10.994843 45.442638, "
                     + "10.9941592 45.443302, 10.9932763 45.4429268, "
                     + "10.9939144 45.4422118, 10.9936846 45.44209439999999)" ),
                 450,1411) );
      } catch( ParseException e ) {
        // nothing here
      }

      try {
        add( new Step
               ( "PE",
                 "PB",
                 "DRIVING",
                 reader.read
                   ( "LINESTRING (10.9971342 45.4428648, 10.9955071 45.4420138, "
                     + "10.994843 45.442638, 10.9941592 45.443302, "
                     + "10.9932763 45.4429268, 10.9939144 45.4422118, "
                     + "10.9952557 45.4408983, 10.9958605 45.4402222, "
                     + "10.9948881 45.4394746, 10.9951649 45.4384667, "
                     + "10.9944302 45.4382012, 10.9935595 45.4383765)" ),
                 400,1154 ) );
      } catch( ParseException e ) {
        // nothing here
      }


      try {
        add( new Step
               ( "PE",
                 "PTB",
                 "DRIVING",
                 reader.read
                   ( "LINESTRING (10.9971342 45.4428648, 10.9955071 45.4420138, "
                     + "10.994843 45.442638, 10.9941592 45.443302, "
                     + "10.9932763 45.4429268, 10.9939144 45.4422118, "
                     + "10.9936846 45.44209439999999)" ),
                 188,533 ) );
      } catch( ParseException e ) {
        // nothing here
      }

      try {
        add( new Step
               ( "PTB",
                 "PB",
                 "DRIVING",
                 reader.read
                   ( "LINESTRING (10.9936846 45.44209439999999, "
                     + "10.9941874 45.4419109, 10.9952557 45.4408983, "
                     + "10.9958605 45.4402222, 10.9948881 45.4394746, "
                     + "10.9951649 45.4384667, 10.9944302 45.4382012, "
                     + "10.9935595 45.4383765)" ),
                 226, 661 ) );
      } catch( ParseException e ) {
        // nothing here
      }

      try {
        add( new Step
               ( "PTB",
                 "PE",
                 "DRIVING",
                 reader.read
                   ( "LINESTRING (10.9936846 45.44209439999999, "
                     + "10.9941874 45.4419109, 10.9956191 45.4405517, "
                     + "10.9965492 45.4409797, 10.9964985 45.4410304, "
                     + "10.9972077 45.4413908, 10.9963716 45.44245129999999)" ),
                 156, 544 ) );
      } catch( ParseException e ) {
        // nothing here
      }

      try {
        add( new Step
               ( "PB",
                 "PE",
                 "WALKING",
                 reader.read
                   ( "LINESTRING (10.9932779 45.43882379999999, "
                     + "10.9928064 45.4391503, 10.9934941 45.4394851, "
                     + "10.993738 45.4395727, 10.9969698 45.4417687, "
                     + "10.9963716 45.44245129999999, 10.9971342 45.4428648, "
                     + "10.9973492 45.4429825)" ),
                 523, 686 ) );
      } catch( ParseException e ) {
        // nothing here
      }

      try {
        add( new Step
               ( "PB",
                 "PTB",
                 "WALKING",
                 reader.read
                   ( "LINESTRING (10.9932779 45.43882379999999, "
                     + "10.9928064 45.4391503, 10.9934941 45.4394851, "
                     + "10.9933959 45.440044, 10.9929885 45.4417384, "
                     + "10.9936846 45.44209439999999)" ),
                 344, 447 ) );
      } catch( ParseException e ) {
        // nothing here
      }

      try {
        add( new Step
               ( "PE",
                 "PB",
                 "WALKING",
                 reader.read
                   ( "LINESTRING (10.9973492 45.4429825, 10.9971342 45.4428648, "
                     + "10.9963716 45.44245129999999, 10.9969698 45.4417687, "
                     + "10.993738 45.4395727, 10.9934941 45.4394851, "
                     + "10.9928064 45.4391503, 10.9932779 45.43882379999999)" ),
                 496, 686 ) );
      } catch( ParseException e ) {
        // nothing here
      }

      try {
        add( new Step
               ( "PE",
                 "PTB",
                 "WALKING",
                 reader.read
                   ( "LINESTRING (10.9973492 45.4429825, 10.9971342 45.4428648, "
                     + "10.9966524 45.4435216, 10.9936846 45.44209439999999)" ),
                 281,384 ) );
      } catch( ParseException e ) {
        // nothing here
      }

      try {
        add( new Step
               ( "PTB",
                 "PB",
                 "WALKING",
                 reader.read
                   ( "LINESTRING (10.9936846 45.44209439999999, "
                     + "10.993213 45.4418933, 10.9929885 45.4417384, "
                     + "10.9933959 45.440044, 10.9934941 45.4394851, "
                     + "10.9928064 45.4391503, 10.9932779 45.43882379999999)" ),
                 318, 447 ) );
      } catch( ParseException e ) {
        // nothing here
      }

      try {
        add( new Step
               ( "PTB",
                 "PE",
                 "WALKING",
                 reader.read
                   ( "LINESTRING (10.9936846 45.44209439999999, "
                     + "10.9966524 45.4435216, 10.9971342 45.4428648, "
                     + "10.9973492 45.4429825)" ),
                 283, 384 ) );
      } catch( ParseException e ) {
        // nothing here
      }
    }
  };*/

  private static List<String> scenicSites = new ArrayList<String>(  ){
    {
      add( "PB" );
      add( "PE" );
      add( "PTB" );
    }
  } ;

  // ===========================================================================


  /*public static List<Step> getScenicRoutes() {
    return scenicRoutes;
  }

  public static void setScenicRoutes( List<Step> scenicRoutes ) {
    ScenicRoutes.scenicRoutes = scenicRoutes;
  }*/

  public static List<String> getScenicSites() {
    return scenicSites;
  }

  /*public static void setScenicSites( List<String> scenicSites ) {
    ScenicRoutes.scenicSites = scenicSites;
  }*/

  public static boolean isScenicSite( String siteId ){
    return scenicSites.contains( siteId.toUpperCase() );
  }
}
