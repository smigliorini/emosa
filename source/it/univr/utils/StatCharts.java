package it.univr.utils;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import javax.imageio.ImageIO;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;


/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class StatCharts {


  /**
   * MISSING_COMMENT
   *
   * @param chart
   * @param width
   * @param height
   * @return
   */

  public static byte[] chartToImage( JFreeChart chart, int width, int height ) {
    if( chart == null ) {
      throw new NullPointerException();
    }

    final BufferedImage image = chart.createBufferedImage( width, height );
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      ImageIO.write( image, "png", stream );
    } catch( IOException e ) {
      e.printStackTrace();
    }

    return stream.toByteArray();
  }


  /**
   * MISSING_COMMENT
   *
   * @param image
   * @param path
   */

  public static void writeImageToFile( byte[] image, String path ) {
    if( image == null ) {
      throw new NullPointerException();
    }
    if( path == null ) {
      throw new NullPointerException();
    }

    try {
      final InputStream in = new ByteArrayInputStream( image );
      final BufferedImage bi = ImageIO.read( in );
      final File file = new File( path );
      ImageIO.write( bi, "png", file );

    } catch( IOException e ) {
      e.printStackTrace();
    }
  }
}
