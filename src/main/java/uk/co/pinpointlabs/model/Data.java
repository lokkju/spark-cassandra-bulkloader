/**
 * Copyright 2015
 */
package uk.co.pinpointlabs.model;

import java.io.Serializable;

/**
 * Test model for inserts
 * 
 * @author Robert Bruce <rob@pinpointlabs.co.uk>
 */
public class Data implements Serializable {
  protected Integer first;
  protected Integer second;
  
  /**
   * Creates an empty data model
   */
  public Data() {
    
  }
  
  /**
   * Creates a new Data model with data
   * 
   * @param first the first to set
   * @param second the second to set
   */
  public Data(Integer first, Integer second) {
    this.setFirst(first);
    this.setSecond(second);
  }
  
  /**
   * Gets the first
   * @return the first
   */
  public Integer getFirst() {
    return first;
  }
  /**
   * Sets the first
   * @param first the first to set
   */
  public void setFirst(Integer first) {
    this.first = first;
  }
  /**
   * Gets the second
   * @return the second
   */
  public Integer getSecond() {
    return second;
  }
  /**
   * Sets the second
   * @param second the second to set
   */
  public void setSecond(Integer second) {
    this.second = second;
  }
}
