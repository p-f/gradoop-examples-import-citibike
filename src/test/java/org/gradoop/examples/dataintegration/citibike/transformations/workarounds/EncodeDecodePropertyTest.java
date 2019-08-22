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
package org.gradoop.examples.dataintegration.citibike.transformations.workarounds;

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class EncodeDecodePropertyTest {

  /**
   * The object used to test this codec.
   */
  @Parameterized.Parameter
  public Object propertyValue;

  /**
   * Test this codec by encoding and decoding an object.
   */
  @Test
  public void testCodec() {
    PropertyValue startValue = PropertyValue.create(propertyValue);
    final String key = "testKey";
    Element e = new EPGMVertex();
    e.setProperty(key, startValue);
    // Encode
    Element encoded = new EncodeProperty<>(key).apply(e, e);
    // Decode
    Element decoded = new DecodeProperty<>(key).apply(e, e);
    // Check property
    PropertyValue recodedValue = decoded.getPropertyValue(key);
    assertEquals(startValue, recodedValue);
  }

  /**
   * Parameters for this test.
   * These will be all supported property value types.
   *
   * @return An iterable of objects that may be used as property values.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object> parameters() {
    return GradoopTestUtils.SUPPORTED_PROPERTIES.values();
  }
}