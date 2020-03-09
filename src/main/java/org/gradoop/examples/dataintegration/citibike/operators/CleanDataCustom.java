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
package org.gradoop.examples.dataintegration.citibike.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.examples.dataintegration.citibike.CitibikeDataImporter;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A dataset-specific custom function cleaning and preprocessing the data.
 */
public class CleanDataCustom implements UnaryGraphToGraphOperator {

  /**
   * A placeholder used for unset properties.
   */
  private static final PropertyValue UNSET = PropertyValue.create("\\N");

  /**
   * Logger for this class.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(CleanDataCustom.class);

  @Override
  public LogicalGraph execute(LogicalGraph logicalGraph) {
    DataSet<EPGMVertex> vertices = logicalGraph.getVertices().flatMap(new CleanVertices());
    return logicalGraph.getFactory().fromDataSets(
      logicalGraph.getGraphHead(), vertices, logicalGraph.getEdges());
  }

  /**
   * A function cleaning trip vertices.
   */
  private static class CleanVertices implements FlatMapFunction<EPGMVertex, EPGMVertex> {

    @Override
    public void flatMap(EPGMVertex in, Collector<EPGMVertex> out) throws Exception {
      in.setId(GradoopId.get());
      if (in.getProperties() == null) return;
      // Clean unset properties.
      for (Property property : in.getProperties()) {
        if (property.getValue().equals(UNSET)) {
          in.removeProperty(property.getKey());
        }
        if (property.getKey().endsWith("time") && property.getValue().getString().contains("time")) {
          // This looks like a header row.
          LOGGER.debug("This looks like a header row: {}", in);
          return;
        }
      }
      in.setLabel(CitibikeDataImporter.LABEL_TRIP);
      out.collect(in);
    }
  }
}
