/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.examples.dataintegration.citibike.transformations;

import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class RenameAndMovePropertiesToMapTest {
  @Test
  public void testTransformation() {
    Vertex v = new EPGMVertex();
    PropertyValue value_a = PropertyValue.create("value_a");
    PropertyValue value_b = PropertyValue.create("value_b");
    PropertyValue value_c = PropertyValue.create("value_c");
    v.setProperty("__a", value_a);
    v.setProperty("__b", value_b);
    v.setProperty("__c", value_c);
    TransformationFunction<Vertex> function = new RenameAndMovePropertiesToMap<>(
      Arrays.asList("__a", "__b"), "map", s -> s.substring(2));
    Vertex result = function.apply(v, v);
    assertNotNull(result.getProperties());
    assertEquals(2, result.getProperties().size());
    assertTrue(v.hasProperty("__c"));
    assertEquals(value_c, v.getPropertyValue("__c"));
    assertTrue(v.hasProperty("map"));
    Map<PropertyValue, PropertyValue> expected = new HashMap<>();
    expected.put(PropertyValue.create("a"), value_a);
    expected.put(PropertyValue.create("b"), value_b);
    assertEquals(PropertyValue.create(expected), v.getPropertyValue("map"));
  }
}