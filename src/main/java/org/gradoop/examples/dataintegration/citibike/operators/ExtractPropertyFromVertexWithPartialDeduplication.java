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
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.dataintegration.transformation.impl.ExtractPropertyFromVertex;
import org.gradoop.dataintegration.transformation.impl.config.EdgeDirection;
import org.gradoop.dataintegration.transformation.impl.functions.ExtractPropertyWithOriginId;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.function.Function;

/**
 * A version of the {@link ExtractPropertyFromVertex} operator that caches IDs of newly created vertices
 * and reuses them accordingly.<p>
 */
public class ExtractPropertyFromVertexWithPartialDeduplication implements UnaryGraphToGraphOperator {

  /**
   * The label of vertices to extract from.
   */
  private final String forVerticesOfLabel;

  /**
   * The property to extract.
   */
  private final String originalPropertyName;

  /**
   * The label for the newly created vertices.
   */
  private final String newVertexLabel;

  /**
   * The new property key.
   */
  private final String newPropertyName;

  /**
   * The edge label.
   */
  private final String edgeLabel;

  /**
   * The function used decide which elements should be reused.
   */
  private Function<PropertyValue, Object> deduplicationKey = null;

  /**
   * Create a new instance of this operator.
   *
   * @param forVerticesOfLabel   The label of vertices to extract the properties from.
   * @param originalPropertyName The name of the property to extract.
   * @param newVertexLabel       The label of the newly created vertices.
   * @param newPropertyName      The name of the new property.
   * @param edgeLabel            The label of the newly created edges.
   */
  public ExtractPropertyFromVertexWithPartialDeduplication(String forVerticesOfLabel,
    String originalPropertyName, String newVertexLabel, String newPropertyName, EdgeDirection edgeDirection,
    String edgeLabel) {
    this.forVerticesOfLabel = forVerticesOfLabel;
    this.originalPropertyName = originalPropertyName;
    this.newVertexLabel = newVertexLabel;
    this.newPropertyName = newPropertyName;
    this.edgeLabel = edgeLabel;
  }

  @Override
  public LogicalGraph execute(LogicalGraph logicalGraph) {
    final DataSet<EPGMVertex> sourceVertices = logicalGraph.getVerticesByLabel(forVerticesOfLabel);
    final DataSet<Tuple2<PropertyValue, GradoopId>> idToProp =
      sourceVertices.flatMap(new ExtractPropertyWithOriginId(originalPropertyName));
    return null;
  }
}
