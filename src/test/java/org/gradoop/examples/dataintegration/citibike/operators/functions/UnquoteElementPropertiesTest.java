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
package org.gradoop.examples.dataintegration.citibike.operators.functions;

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import static org.gradoop.common.model.impl.properties.PropertyValue.create;
import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link UnquoteElementProperties} map function.
 */
public class UnquoteElementPropertiesTest extends GradoopFlinkTestBase {

  /**
   * Test the map functionality.
   */
  @Test
  public void testMap() {
    UnquoteElementProperties<Vertex> function = new UnquoteElementProperties<>();
    Vertex vertex = getConfig().getLogicalGraphFactory().getVertexFactory().createVertex();
    vertex.setProperty(GradoopTestUtils.KEY_0, "\"a\"");
    vertex.setProperty(GradoopTestUtils.KEY_1, 1L);
    vertex.setProperty(GradoopTestUtils.KEY_2, "\"\"\"");
    vertex.setProperty(GradoopTestUtils.KEY_3, "\"\"b\"\"");
    vertex.setProperty(GradoopTestUtils.KEY_4, "\"c");
    Vertex result = function.map(vertex);
    assertEquals(create("a"), result.getPropertyValue(GradoopTestUtils.KEY_0));
    assertEquals(create(1L), result.getPropertyValue(GradoopTestUtils.KEY_1));
    assertEquals(create("\""), result.getPropertyValue(GradoopTestUtils.KEY_2));
    assertEquals(create("b"), result.getPropertyValue(GradoopTestUtils.KEY_3));
    assertEquals(create("\"c"), result.getPropertyValue(GradoopTestUtils.KEY_4));
  }
}
