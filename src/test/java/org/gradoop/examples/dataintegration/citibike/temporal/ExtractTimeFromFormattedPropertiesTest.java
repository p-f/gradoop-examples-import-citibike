/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.examples.dataintegration.citibike.temporal;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link ExtractTimeFromFormattedProperties} function.
 */
public class ExtractTimeFromFormattedPropertiesTest extends GradoopFlinkTestBase {

  /**
   * The function to test.
   */
  private ExtractTimeFromFormattedProperties<Vertex> function;

  /**
   * A test element to extract the data from.
   */
  private Vertex element;

  /**
   * Set up this test. This specific format is used in the data.
   */
  @Before
  public void setUp() {
    function = new ExtractTimeFromFormattedProperties<>("from", "to",
      "yyyy-MM-dd HH:mm:ss[.SSS][.SS]");
    element = getConfig().getLogicalGraphFactory().getVertexFactory().createVertex();
  }

  /**
   * Test if the constructor throws an exception when no format is given.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testFailWithNoFormats() {
    new ExtractTimeFromFormattedProperties<>("a", "b");
  }

  /**
   * Test the parser with no milliseconds field.
   */
  @Test
  public void testWithNoOptionalField() {
    element.setProperty("from", "2001-01-01 12:33:44");
    element.setProperty("to", "2020-02-29 13:45:00");
    final Tuple2<Long, Long> result = function.map(element);
    assertEquals(978352424000L, (long) result.f0);
    assertEquals(1582983900000L, (long) result.f1);
  }

  /**
   * Test the parser with a short and a long millisecond field.
   */
  @Test
  public void testWithOptionalField() {
    element.setProperty("from", "2020-03-01 11:01:01.12");
    element.setProperty("to", "2020-03-01 11:01:01.123");
    final Tuple2<Long, Long> result = function.map(element);
    assertEquals(1583060461120L, (long) result.f0);
    assertEquals(1583060461123L, (long) result.f1);
  }
}