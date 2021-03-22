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
package org.gradoop.examples.dataintegration.citibike.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.examples.dataintegration.citibike.operators.functions.CreateEdgeFromTriple;
import org.gradoop.examples.dataintegration.citibike.operators.functions.CreateVerticesFromTriple;
import org.gradoop.examples.dataintegration.citibike.operators.functions.ObjectToTripleWithNewIds;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Splits vertices into two vertices and one edge connecting the two vertices. The initial vertex will be
 * converted to an edge in the process.
 *
 * @param <G> The graph head type.
 * @param <V> The vertex type.
 * @param <E> The edge type.
 * @param <LG> The graph type.
 * @param <GC> The graph collection type.
 */
public class SplitVertex<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> implements UnaryBaseGraphToBaseGraphOperator<LG> {

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
   * Create a new instance of this operator.
   *
   * @param labelSource The label of the new source vertex.
   * @param labelTarget The label of the new target vertex.
   * @param propertiesSource A list of keys for properties to be moved to the new source.
   * @param propertiesTarget A list of keys for properties to be moved to the new target.
   */
  public SplitVertex(String labelSource, String labelTarget, List<String> propertiesSource,
    List<String> propertiesTarget) {
    this.labelSource = Objects.requireNonNull(labelSource);
    this.labelTarget = Objects.requireNonNull(labelTarget);
    this.propertiesSource = Objects.requireNonNull(propertiesSource);
    this.propertiesTarget = Objects.requireNonNull(propertiesTarget);
  }

  @Override
  public LG execute(LG graph) {
    final DataSet<Tuple3<V, GradoopId, GradoopId>> triples =
      graph.getVertices().map(new ObjectToTripleWithNewIds<>());
    final DataSet<V> newVertices = triples.flatMap(new CreateVerticesFromTriple<>(labelSource, labelTarget,
      propertiesSource, propertiesTarget, graph.getFactory().getVertexFactory()));
    List<String> keys = new ArrayList<>(propertiesSource);
    keys.addAll(propertiesTarget);
    final DataSet<E> newEdges = triples.map(new CreateEdgeFromTriple<>(graph.getFactory().getEdgeFactory(),
      keys));
    return graph.getFactory().fromDataSets(newVertices, newEdges);
  }
}
