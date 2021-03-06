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
package org.gradoop.examples.dataintegration.citibike;

import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.dataintegration.importer.impl.csv.MinimalCSVImporter;
import org.gradoop.dataintegration.transformation.VertexDeduplication;
import org.gradoop.dataintegration.transformation.impl.ExtractPropertyFromVertex;
import org.gradoop.dataintegration.transformation.impl.config.EdgeDirection;
import org.gradoop.examples.dataintegration.citibike.metadata.MetaDataUtil;
import org.gradoop.examples.dataintegration.citibike.metadata.station.StationMetadata;
import org.gradoop.examples.dataintegration.citibike.operators.CleanDataCustom;
import org.gradoop.examples.dataintegration.citibike.operators.SplitVertex;
import org.gradoop.examples.dataintegration.citibike.operators.UnquoteAllProperties;
import org.gradoop.examples.dataintegration.citibike.temporal.ExtractTimeFromFormattedProperties;
import org.gradoop.examples.dataintegration.citibike.temporal.RemoveElementsWithInvalidTimes;
import org.gradoop.examples.dataintegration.citibike.transformations.AttachStationMetaData;
import org.gradoop.examples.dataintegration.citibike.transformations.MovePropertiesFromMap;
import org.gradoop.examples.dataintegration.citibike.transformations.RenameAndMovePropertiesToMap;
import org.gradoop.examples.dataintegration.citibike.transformations.workarounds.DecodeProperty;
import org.gradoop.examples.dataintegration.citibike.transformations.workarounds.EncodeProperty;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.model.api.functions.TimeIntervalExtractor;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.util.TemporalGradoopConfig;

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
public class CitibikeDataImporter implements DataSource, TemporalDataSource {

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
   * The label for trip-type vertices.
   */
  public static final String LABEL_TRIP = "Trip";

  /**
   * The label for Station-type vertices.
   */
  public static final String LABEL_STATION = "Station";

  /**
   * The date time format of the post 2015 datasets.
   */
  public static final String DATETIME_FORMAT_1 = "yyyy-MM-dd HH:mm:ss[.SSSS][.SSS][.SS]";

  /**
   * The date time format of the pre 2015 datasets.
   */
  public static final String DATETIME_FORMAT_2 = "M[M]/d[d]/yyyy H[H]:mm[:ss]";

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
  private final String inputPath;

  /**
   * The station metadata (optional).
   */
  private final StationMetadata metadata;

  /**
   * Selects which kind of graph should be created.
   */
  private final TargetGraphSchema outputSchema;

  /**
   * The Gradoop config.
   */
  protected final GradoopFlinkConfig config;

  /**
   * Initialize this data importer.
   *  @param inputPath   The path of the input files.
   * @param metadata     The station metadata (can be {@code null}).
   * @param outputSchema The output schema.
   * @param config       The Gradoop config.
   */
  public CitibikeDataImporter(String inputPath, StationMetadata metadata, TargetGraphSchema outputSchema,
    GradoopFlinkConfig config) {
    this.inputPath = Objects.requireNonNull(inputPath);
    this.metadata = metadata;
    this.outputSchema = outputSchema;
    this.config = Objects.requireNonNull(config);
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    DataSource source =  new MinimalCSVImporter(inputPath, ",", config, COLUMNS);
    LogicalGraph inputGraph = source.getLogicalGraph()
      .callForGraph(new CleanDataCustom())
      .callForGraph(new UnquoteAllProperties<>());
    LogicalGraph preparedTrips = inputGraph
            .transformVertices(new RenameAndMovePropertiesToMap<>(STATION_START_ATTRIBUTES, PROP_START_STATION,
                    (Function<String, String> & Serializable) (k -> k.substring(14))))
            .transformVertices(new RenameAndMovePropertiesToMap<>(STATION_END_ATTRIBUTES, PROP_END_STATION,
                    (Function<String, String> & Serializable) (k -> k.substring(12))))
            .transformVertices(new EncodeProperty<>(PROP_START_STATION))
            .transformVertices(new EncodeProperty<>(PROP_END_STATION));
    final String start_station = PROP_START_STATION;
    final String end_station = PROP_END_STATION;
    switch (outputSchema) {
      case TRIPS_AS_VERTICES:
        ExtractPropertyFromVertex extractTripStart = new ExtractPropertyFromVertex(
                LABEL_TRIP, PROP_START_STATION, LABEL_STATION, "s", EdgeDirection.NEWVERTEX_TO_ORIGIN, "trip_start");
        ExtractPropertyFromVertex extractTripEnd = new ExtractPropertyFromVertex(
                LABEL_TRIP, PROP_END_STATION, LABEL_STATION, "s", EdgeDirection.ORIGIN_TO_NEWVERTEX, "trip_end");
        preparedTrips = preparedTrips
                .callForGraph(extractTripStart)
                .callForGraph(extractTripEnd);
        break;
      case TRIPS_AS_EDGES:
        // Create Trip edges.
        preparedTrips = preparedTrips
          .callForGraph(new SplitVertex<>(LABEL_STATION, LABEL_STATION,
            Collections.singletonList(start_station), Collections.singletonList(end_station)))
          .transformVertices((current, t) -> {
                  if (!current.getLabel().equals(LABEL_STATION)) {
                    return current;
                  }
                  PropertyValue stationProperty;
                  if (current.hasProperty(start_station)) {
                    stationProperty = current.getPropertyValue(start_station);
                  } else if (current.hasProperty(end_station)) {
                    stationProperty = current.getPropertyValue(end_station);
                  } else {
                    return current;
                  }
                  current.removeProperty(start_station);
                  current.removeProperty(end_station);
                  current.setProperty("s", stationProperty);

                  return current;
                });
        break;
      default:
        throw new UnsupportedOperationException("Schema not supported: " + outputSchema);
    }
    // Extract stations.
    LogicalGraph transformed = preparedTrips
      .transformVertices(new DecodeProperty<>("s"))
      .transformVertices(new MovePropertiesFromMap<>("s"))
      .transformVertices((current, trans) -> {
        if (current.getLabel().equals(LABEL_TRIP)) {
          current.removeProperty(start_station);
          current.removeProperty(end_station);
        }
        return current;
      })
      .callForGraph(new VertexDeduplication<>(LABEL_STATION, Collections.singletonList("id")));
    if (outputSchema == TargetGraphSchema.TRIPS_AS_VERTICES) {
      transformed = transformed.callForGraph(new ExtractPropertyFromVertex(LABEL_TRIP,
              "bike_id", "Bike", "id", EdgeDirection.ORIGIN_TO_NEWVERTEX, "useBike"));
    }
    if (metadata != null) {
      transformed = transformed.callForGraph(
        new AttachStationMetaData<>(MetaDataUtil.getStationsById(metadata)));
    }
    return transformed;
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {
    return config.getGraphCollectionFactory().fromGraph(getLogicalGraph());
  }

  @Override
  public TemporalGraph getTemporalGraph() throws IOException {
    LogicalGraph graph = getLogicalGraph();
    final TimeIntervalExtractor<EPGMVertex> vertexIntervalExtractor =
            new ExtractTimeFromFormattedProperties<>("starttime", "stoptime",
                    DATETIME_FORMAT_1, DATETIME_FORMAT_2);
    final TimeIntervalExtractor<EPGMEdge> edgeIntervalExtractor =
            new ExtractTimeFromFormattedProperties<>("starttime", "stoptime",
                    DATETIME_FORMAT_1, DATETIME_FORMAT_2);
    return ((TemporalGradoopConfig) config).getTemporalGraphFactory().fromNonTemporalDataSets(
      graph.getGraphHead(), null,
      graph.getVertices().filter(new RemoveElementsWithInvalidTimes<>(vertexIntervalExtractor)),
            vertexIntervalExtractor,
      graph.getEdges().filter(new RemoveElementsWithInvalidTimes<>(edgeIntervalExtractor)),
            edgeIntervalExtractor);
  }

  @Override
  public TemporalGraphCollection getTemporalGraphCollection() throws IOException {
    return ((TemporalGradoopConfig) config).getTemporalGraphCollectionFactory().fromGraph(getTemporalGraph());
  }
}
