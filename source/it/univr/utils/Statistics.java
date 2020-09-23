package it.univr.utils;

import java.util.Collections;
import java.util.List;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class Statistics {

  private Statistics() {
    // nothing here
  }

  /**
   * The method computes the mean of the values in the list {@code data}.
   *
   * @param data
   * @return
   */

  public static double computeMean( List<Double> data ) {
    double sum = 0.0;
    for( double a : data ) {
      sum += a;
    }
    return sum / data.size();
  }


  /**
   * The method computes the variance of the values in the list {@code data}.
   *
   * @param data
   * @return
   */

  public static double computeVariance( List<Double> data ) {
    final double mean = computeMean( data );
    double temp = 0;
    for( double a : data ) {
      temp += ( a - mean ) * ( a - mean );
    }
    return temp / ( data.size() - 1 );
  }


  /**
   * The method computes the standard deviation of the values in the list {@code
   * data}.
   *
   * @param data
   * @return
   */

  public static double computeStandardDeviation( List<Double> data ) {
    return Math.sqrt( computeVariance( data ) );
  }


  /**
   * The method computes the median of the values in the list {@code data}.
   *
   * @param data
   * @return
   */

  public double median( List<Double> data ) {
    Collections.sort( data );

    final int index = ( data.size() / 2 );

    if( data.size() % 2 == 0 ) {
      return ( data.get( index - 1 ) + data.get( index ) ) / 2.0;
    }
    return data.get( index );
  }
}
