/**
 * Copyright 2015
 */
package uk.co.pinpointlabs.function;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
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

  /**
   * Call the method to map the data into a list of <tt>ByteBuffer</tt> matching the
   * insert statement declared as part of the job
   * 
   * @param data the input data
   * @return the key/value tuple
   */
  public Tuple2<ByteBuffer, List<ByteBuffer>> call(Data data) throws Exception {
    ByteBuffer key = Int32Type.instance.decompose(data.getFirst());
    
    List<ByteBuffer> values = new ArrayList<ByteBuffer>();
    
    // add the data to the values
    values.add(Int32Type.instance.decompose(data.getFirst()));
    values.add(Int32Type.instance.decompose(data.getSecond()));
    
    // add the timestamp and ttl
    values.add(LongType.instance.decompose(new Date().getTime()));
    values.add(Int32Type.instance.decompose(0));
    
    return new Tuple2<ByteBuffer, List<ByteBuffer>>(key, values);
  }
}
