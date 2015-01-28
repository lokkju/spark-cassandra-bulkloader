/**
 * Copyright 2015
 */
package uk.co.pinpointlabs.function;

import java.util.Arrays;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.FlatMapFunction;

import uk.co.pinpointlabs.model.Data;

/**
 * Function to map data into models
 * 
 * @author Robert Bruce <rob@pinpointlabs.co.uk>
 */
public class MapTextToData implements FlatMapFunction<String, Data> {

  /**
   * Generated serial version uid
   */
  private static final long serialVersionUID = 2328895679698637446L;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
   */
  @SuppressWarnings("unchecked")
  public Iterable<Data> call(String line) throws Exception {
    String[] data = line.trim().split("\\s+");

    if (data.length < 2) {
      return (Iterable<Data>) IteratorUtils.emptyIterator();
    }
    
    // we just use the last 2 values
    
    Integer first = Integer.valueOf(data[data.length - 2]);
    Integer second = Integer.valueOf(data[data.length - 1]);

    return Arrays.asList(new Data[] {new Data(first, second)});
  }

}
