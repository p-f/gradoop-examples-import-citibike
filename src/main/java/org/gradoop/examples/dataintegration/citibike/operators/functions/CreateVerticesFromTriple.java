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
package org.gradoop.examples.dataintegration.citibike.operators.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

import java.util.List;
import java.util.Objects;

/**
 * Create the source- and the target vertex from the triple representation created by
 * {@link ObjectToTripleWithNewIds}.
 *
 * @param <V> The vertex type.
 */
public class CreateVerticesFromTriple<V extends Vertex>
  implements FlatMapFunction<Tuple3<V, GradoopId, GradoopId>, V> {

  /**
   * A vertex to be reused for output.
   */
  private final V reuseVertex;

  /**
   * The label of the new source vertex.
   */
  private final String labelSource;

  /**
   * The label of the new target vertex.
   */
  private final String labelTarget;

  /**
   * Keys of properties to be moved to the new source vertex.
   */
  private final List<String> propertiesSource;

  /**
   * Keys of properties to be moved to the new target vertex.
   */
  private final List<String> propertiesTarget;

  /**
   * Create a new instance of this flatmap function.
   *
   * @param labelSource      The label of the new source vertex.
   * @param labelTarget      The label of the new target vertex.
   * @param propertiesSource A list of keys for properties to be moved to the new source.
   * @param propertiesTarget A list of keys for properties to be moved to the new target.
   * @param vertexFactory    A factory used to create new vertices.
   */
  public CreateVerticesFromTriple(String labelSource, String labelTarget, List<String> propertiesSource,
    List<String> propertiesTarget, VertexFactory<V> vertexFactory) {
    this.labelSource = Objects.requireNonNull(labelSource);
    this.labelTarget = Objects.requireNonNull(labelTarget);
    this.propertiesSource = Objects.requireNonNull(propertiesSource);
    this.propertiesTarget = Objects.requireNonNull(propertiesTarget);
    this.reuseVertex = Objects.requireNonNull(vertexFactory).createVertex();
  }

  @Override
  public void flatMap(Tuple3<V, GradoopId, GradoopId> triple, Collector<V> out) {
    final V inputVertex = triple.f0;
    out.collect(createVertex(inputVertex, triple.f1, labelSource, propertiesSource));
    out.collect(createVertex(inputVertex, triple.f2, labelTarget, propertiesTarget));
  }

  /**
   * Create a new vertex from the original vertex.
   *
   * @param source The vertex to act as the source for the data.
   * @param id     The id of the new vertex.
   * @param label  The label of the new vertex.
   * @param keys   Keys of properties to copy to the new vertex.
   * @return The updated (reused) vertex.
   */
  private V createVertex(V source, GradoopId id, String label, List<String> keys) {
    reuseVertex.setId(id);
    reuseVertex.setLabel(label);
    Properties newProperties = Properties.createWithCapacity(keys.size());
    for (String key : keys) {
      if (source.hasProperty(key)) {
        newProperties.set(key, source.getPropertyValue(key));
      }
    }
    reuseVertex.setProperties(newProperties);
    return reuseVertex;
  }
}
