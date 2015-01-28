/**
 * Copyright 2015
 */
package uk.co.pinpointlabs.function;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import uk.co.pinpointlabs.model.Data;

/**
 * Function to map data suitable for bulk loading into cassandra
 * 
 * @author Robert Bruce <rob@pinpointlabs.co.uk>
 */
public class MapDataToCQLBytesPair implements PairFunction<Data, ByteBuffer, List<ByteBuffer>> {

  /**
   * Generated serial version uid
   */
  private static final long serialVersionUID = -4807077651199070907L;

  /* (non-Javadoc)
   * @see org.apache.spark.api.java.function.PairFunction#call(java.lang.Object)
   */
  public Tuple2<ByteBuffer, List<ByteBuffer>> call(Data data) throws Exception {
    ByteBuffer key = ByteBufferUtil.bytes(data.getFirst());
    
    List<ByteBuffer> values = new ArrayList<ByteBuffer>();
    
    values.add(ByteBufferUtil.bytes(data.getFirst()));
    values.add(ByteBufferUtil.bytes(data.getSecond()));
    values.add(ByteBufferUtil.bytes(new Date().getTime()));
    
    return new Tuple2<ByteBuffer, List<ByteBuffer>>(key, values);
  }
}
