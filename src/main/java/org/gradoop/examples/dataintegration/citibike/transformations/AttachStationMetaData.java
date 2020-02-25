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
package org.gradoop.examples.dataintegration.citibike.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.examples.dataintegration.citibike.CitibikeDataImporter;
import org.gradoop.examples.dataintegration.citibike.metadata.station.Station;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;

/**
 * Attaches station metadata from a {@link Station} object to a station vertex.
 *
 * @param <G> The graph head type.
 * @param <V> The vertex type.
 * @param <E> The edge type.
 * @param <LG> The graph type.
 * @param <GC> The graph collection type.
 */
public class AttachStationMetaData<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> implements UnaryBaseGraphToBaseGraphOperator<LG>,
  MapFunction<V, V> {

  /**
   * Logger for this class.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(AttachStationMetaData.class);

  /**
   * The stations, ordered by ID.
   */
  private final Station[] stations;

  /**
   * A dummy station used as to search the stations array.
   */
  private final Station dummy = new Station();

  /**
   * Create an instance of this map function.
   *
   * @param stations The stations (ordered by ID).
   */
  public AttachStationMetaData(Station[] stations) {
    this.stations = Objects.requireNonNull(stations);
  }

  @Override
  public V map(V vertex){
    if (vertex.getLabel().equals(CitibikeDataImporter.LABEL_STATION)) {
      final PropertyValue idValue = vertex.getPropertyValue("id");
      if (idValue == null) {
        LOGGER.warn("Station was not id property: {}", vertex);
        return vertex;
      }
      int stationId = -1;
      try {
        stationId = Integer.parseInt(idValue.getString());
      } catch (NumberFormatException nfe) {
        LOGGER.info("Station has no valid station ID: {} ({})", vertex, idValue.toString());
        return vertex;
      }
      dummy.setStationId(stationId);
      final int index = Arrays.binarySearch(stations, dummy);
      if (index >= 0) {
        final Station theStation = stations[index];
        vertex.setProperty("capacity", theStation.getCapacity());
        vertex.setProperty("regionId", theStation.getRegionId());
        vertex.setProperty("rentalUrl", Objects.toString(theStation.getRentalUrl()));
      } else {
        LOGGER.warn("No station infos for ID {} found.", stationId);
      }
    }
    return vertex;
  }

  @Override
  public LG execute(LG graph) {
    return graph.getFactory().fromDataSets(
      graph.getGraphHead(),
      graph.getVertices().map(this),
      graph.getEdges());
  }
}
