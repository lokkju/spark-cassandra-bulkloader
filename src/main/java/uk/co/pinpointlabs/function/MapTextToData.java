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

  /**
   * Call the method to map the text data into the model
   * 
   * @param line the line of text to parse
   * @return the data models
   */
  @SuppressWarnings("unchecked")
  public Iterable<Data> call(String line) throws Exception {
    String[] data = line.trim().split("\\s+");

    if (data.length < 2) {
      return (Iterable<Data>) IteratorUtils.emptyIterator();
    }
    
    Integer first, second;
    
    // we just use the last 2 values
    try {
      first = Integer.valueOf(data[data.length - 2]);
      second = Integer.valueOf(data[data.length - 1]);
    } catch (NumberFormatException ex) {
      return (Iterable<Data>) IteratorUtils.emptyIterator();
    }

    return Arrays.asList(new Data[] {new Data(first, second)});
  }

}
