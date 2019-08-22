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
package org.gradoop.examples.dataintegration.citibike;

import org.gradoop.examples.dataintegration.citibike.temporal.ExtractTimeFromFormattedProperties;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.IOException;

/**
 * A data importer for CSV files provided by {@code https://www.citibikenyc.com/system-data}.
 * <p>
 * <b>Note: </b>Citi Bike, Citi Bike and Arc Design and the Blue Wave are registered service marks of
 * Citigroup Inc.
 * <p>
 * This importer also sets temporal attributes on {@code trip} type vertices.
 */
public class TemporalCitibikeDataImporter extends CitibikeDataImporter implements TemporalDataSource {

  /**
   * Initialize this data importer.
   *
   * @param inputPath The path of the input files.
   * @param config    The Gradoop config.
   */
  public TemporalCitibikeDataImporter(String inputPath, TemporalGradoopConfig config) {
    super(inputPath, config);
  }

  @Override
  public TemporalGraph getTemporalGraph() throws IOException {
    LogicalGraph graph = getLogicalGraph();
    return ((TemporalGradoopConfig) config).getTemporalGraphFactory().fromNonTemporalDataSets(
      graph.getGraphHead(), null,
      graph.getVertices(), new ExtractTimeFromFormattedProperties<>("starttime", "stoptime",
        "\"yyyy-MM-dd HH:mm:ss\""),
      graph.getEdges(), null);
  }

  @Override
  public TemporalGraphCollection getTemporalGraphCollection() throws IOException {
    return ((TemporalGradoopConfig) config).getTemporalGraphCollectionFactory().fromGraph(getTemporalGraph());
  }
}
