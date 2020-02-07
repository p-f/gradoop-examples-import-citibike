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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Create an edge between the newly created vertices from the original vertex.
 *
 * @param <V> The input vertex type.
 * @param <E> The output edge type.
 */
public class CreateEdgeFromTriple<V extends Vertex, E extends Edge>
  implements MapFunction<Tuple3<V, GradoopId, GradoopId>, E>, ResultTypeQueryable<E> {

  /**
   * A list of property keys to remove.
   */
  private final List<String> keysToRemove;

  /**
   * An edge instance to be reused.
   */
  private final E reuseEdge;

  /**
   * The type of the newly created edge.
   */
  private final Class<E> edgeType;

  public CreateEdgeFromTriple(EdgeFactory<E> edgeFactory, List<String> keysToRemove) {
    this.keysToRemove = Objects.requireNonNull(keysToRemove);
    this.reuseEdge = Objects.requireNonNull(edgeFactory)
      .createEdge(GradoopId.NULL_VALUE, GradoopId.NULL_VALUE);
    this.edgeType = edgeFactory.getType();
    keysToRemove.sort(Comparator.naturalOrder());
  }

  @Override
  public E map(Tuple3<V, GradoopId, GradoopId> triple) {
    final V vertex = triple.f0;
    reuseEdge.setLabel(vertex.getLabel());
    reuseEdge.setId(GradoopId.get());
    Properties newProperties = Properties.create();
    for (String vertexProperty : vertex.getPropertyKeys()) {
      if (Collections.binarySearch(keysToRemove, vertexProperty) < 0) {
        newProperties.set(vertexProperty, vertex.getPropertyValue(vertexProperty));
      }
    }
    reuseEdge.setProperties(newProperties);
    return reuseEdge;
  }

  @Override
  public TypeInformation<E> getProducedType() {
    return TypeInformation.of(edgeType);
  }
}
