package it.univr.veronacard.shadoop.core;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;
import com.vividsolutions.jts.io.WKTReader;
import edu.umn.cs.spatialHadoop.core.OGCJTSShape;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.copyOfRange;

/**
 * MISSING_COMMENT
 *
 * @author Mauro Gambini, Sara Migliorini
 * @version 0.0.0
 */
public class FeatureWritable extends OGCJTSShape {

  // === Properties ============================================================

  private List<Text> attributes;

  private final WKTReader textReader = new WKTReader();
  private final WKBReader byteReader = new WKBReader();
  private final WKBWriter byteWriter = new WKBWriter();

  // === Methods ===============================================================

  public FeatureWritable() {
    this.geom = null;
    this.attributes = Collections.emptyList();
  }

  public FeatureWritable( Geometry geometry ) {
    this.geom = geometry;
    this.attributes = Collections.emptyList();
  }

  public void addAttribute( Text attribute ) {
    if( attributes == null ) {
      attributes = new ArrayList<>();
    }
    attributes.add( attribute );
  }

  public void resetAttributes() {
    this.attributes = new ArrayList<>();
  }

  public Text getAttribute( int index ) {
    if( attributes == null || index > attributes.size() - 1 ) {
      return null;
    } else {
      return attributes.get( index );
    }
  }

  // ===========================================================================

  @Override
  public void write( DataOutput dataOutput ) throws IOException {
    if( dataOutput == null ) {
      throw new NullPointerException();
    }

    // write geometry
    final byte[] wkb = byteWriter.write( geom );
    dataOutput.writeInt( wkb.length );
    dataOutput.write( wkb );

    // write attributes
    if( attributes != null ) {
      dataOutput.writeInt( attributes.size() );
      for( Text a : attributes ) {
        a.write( dataOutput );
      }
    } else {
      dataOutput.writeInt( 0 );
    }
  }

  @Override
  public void readFields( DataInput dataInput ) throws IOException {
    if( dataInput == null ) {
      throw new NullPointerException();
    }

    final byte[] wkb = new byte[dataInput.readInt()];
    dataInput.readFully( wkb );
    try {
      this.geom = byteReader.read( wkb );
    } catch( ParseException e ) {
      this.geom = null;
    }

    final int numAttributes = dataInput.readInt();
    attributes = new ArrayList<>();
    for( int i = 0; i < numAttributes; i++ ) {
      final Text a = new Text();
      a.readFields( dataInput );
      attributes.add( a );
    }
  }

  @Override
  public void fromText( Text text ) {
    this.geom = TextSerializerHelper.consumeGeometryJTS( text, '\t' );
    final byte[] bytes = text.getBytes();
    final int length = text.getLength();

    attributes = new ArrayList<>();
    int start = 0;
    for( int i = 0; i < length; i++ ) {
      while( bytes[i] != '\t' ) {
        i++;
      }
      final String s = new String( copyOfRange( bytes, start, i ), UTF_8 );
      attributes.add( new Text( s ) );
      i++; // skip '\t' character
      start = i;
    }
  }

  @Override
  public Text toText( Text text ) {
    final String wkt = geom == null ? "" : geom.toText();
    final byte[] wkb = wkt.getBytes();
    text.append( wkb, 0, wkb.length );

    if( attributes != null ) {
      text.append( new byte[]{(byte) '\t'}, 0, 1 );
      for( Text a : attributes ) {
        text.append( a.getBytes(), 0, a.getLength() );
        text.append( new byte[]{(byte) '\t'}, 0, 1 );
      }
    }
    return text;
  }

  // ===========================================================================


  @Override
  public boolean equals( Object obj ) {
    if( obj instanceof FeatureWritable ) {
      final FeatureWritable other = (FeatureWritable) obj;
      return this.geom.equals( other.geom ) &&
             this.attributes.equals( other.attributes );
    } else {
      return false;
    }
  }
}
