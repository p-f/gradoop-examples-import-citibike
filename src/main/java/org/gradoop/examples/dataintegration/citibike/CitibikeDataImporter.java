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

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.dataintegration.importer.impl.csv.MinimalCSVImporter;
import org.gradoop.dataintegration.transformation.VertexDeduplication;
import org.gradoop.dataintegration.transformation.impl.ExtractPropertyFromVertex;
import org.gradoop.dataintegration.transformation.impl.config.EdgeDirection;
import org.gradoop.examples.dataintegration.citibike.transformations.MovePropertiesFromMap;
import org.gradoop.examples.dataintegration.citibike.transformations.RenameAndMovePropertiesToMap;
import org.gradoop.examples.dataintegration.citibike.transformations.workarounds.DecodeProperty;
import org.gradoop.examples.dataintegration.citibike.transformations.workarounds.EncodeProperty;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A data importer for CSV files provided by {@code https://www.citibikenyc.com/system-data}.
 * <p>
 * <b>Note: </b>Citi Bike, Citi Bike and Arc Design and the Blue Wave are registered service marks of
 * Citigroup Inc.
 */
public class CitibikeDataImporter implements DataSource {

  /**
   * The column names of the input CSV files.
   */
  private static final List<String> COLUMNS = Arrays.asList(
    "tripduration",
    "starttime",
    "stoptime",
    "start_station_id",
    "start_station_name",
    "start_station_lat",
    "start_station_long",
    "end_station_id",
    "end_station_name",
    "end_station_lat",
    "end_station_long",
    "bike_id",
    "user_type",
    "year_birth",
    "gender"
  );

  /**
   * Attributes of start stations of a trip.
   */
  private static final List<String> STATION_START_ATTRIBUTES = Arrays.asList(
    "start_station_id",
    "start_station_name",
    "start_station_lat",
    "start_station_long"
  );

  /**
   * Attributes of end stations of a trip.
   */
  private static final List<String> STATION_END_ATTRIBUTES = Arrays.asList(
    "end_station_id",
    "end_station_name",
    "end_station_lat",
    "end_station_long"
  );

  /**
   * A placeholder used for unset properties.
   */
  private static final PropertyValue UNSET = PropertyValue.create("\\N");

  /**
   * The label for trip-type vertices.
   */
  public static final String LABEL_TRIP = "trip";

  /**
   * The label for Station-type vertices.
   */
  public static final String LABEL_STATION = "Station";

  /**
   * The property used to temporarily store infos about the start station.
   */
  public final String PROP_START_STATION = "start_station";

  /**
   * The property used to temporarily store infos about the end station.
   */
  public final String PROP_END_STATION = "end_station";

  /**
   * The path of the input CSV file/files.
   */
  public final String inputPath;

  /**
   * The Gradoop config.
   */
  protected final GradoopFlinkConfig config;

  /**
   * Initialize this data importer.
   *
   * @param inputPath The path of the input files.
   * @param config    The Gradoop config.
   */
  public CitibikeDataImporter(String inputPath, GradoopFlinkConfig config) {
    this.inputPath = Objects.requireNonNull(inputPath);
    this.config = Objects.requireNonNull(config);
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    DataSource source =  new MinimalCSVImporter(inputPath, ",", config, COLUMNS, false);
    LogicalGraph inputGraph = source.getLogicalGraph().transformVertices((v, c) -> {
      v.setId(GradoopId.get());
      if (v.getProperties() == null) return v;
      // Clean unset properties.
      for (Property property : v.getProperties()) {
        if (property.getValue().equals(UNSET)) {
          v.removeProperty(property.getKey());
        }
      }
      v.setLabel("trip");
      return v;
    });
    ExtractPropertyFromVertex extractTripStart = new ExtractPropertyFromVertex(
      LABEL_TRIP, PROP_START_STATION, LABEL_STATION, "s", EdgeDirection.NEWVERTEX_TO_ORIGIN, "trip_start");
    extractTripStart.setCondensation(false);
    ExtractPropertyFromVertex extractTripEnd = new ExtractPropertyFromVertex(
      LABEL_TRIP, PROP_END_STATION, LABEL_STATION, "s", EdgeDirection.ORIGIN_TO_NEWVERTEX, "trip_end");
    extractTripEnd.setCondensation(false);
    // Extract stations.
    LogicalGraph transformed = inputGraph
      .transformVertices(new RenameAndMovePropertiesToMap<>(STATION_START_ATTRIBUTES, PROP_START_STATION,
        (Function<String, String> & Serializable) (k -> k.substring(14))))
      .transformVertices(new RenameAndMovePropertiesToMap<>(STATION_END_ATTRIBUTES, PROP_END_STATION,
        (Function<String, String> & Serializable) (k -> k.substring(12))))
      .transformVertices(new EncodeProperty<>(PROP_START_STATION))
      .transformVertices(new EncodeProperty<>(PROP_END_STATION))
      .callForGraph(extractTripStart)
      .callForGraph(extractTripEnd)
      .transformVertices(new DecodeProperty<>("s"))
      .transformVertices(new MovePropertiesFromMap<>("s"))
      .transformVertices((current, trans) -> {
        if (current.getLabel().equals(LABEL_TRIP)) {
          current.removeProperty("start_station");
          current.removeProperty("end_station");
        }
        return current;
      })
      .callForGraph(new ExtractPropertyFromVertex(LABEL_TRIP, "bike_id", "Bike", "id",
        EdgeDirection.ORIGIN_TO_NEWVERTEX, "useBike"))
      .callForGraph(new VertexDeduplication<>(LABEL_STATION, Collections.singletonList("id")));
    // TODO: Remove this, then the bug in ExtractPropertyFromVertex is fixed.
    return transformed.getFactory().fromDataSets(transformed.getVertices(), transformed.getEdges());
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    return config.getGraphCollectionFactory().fromGraph(getLogicalGraph());
  }
}
