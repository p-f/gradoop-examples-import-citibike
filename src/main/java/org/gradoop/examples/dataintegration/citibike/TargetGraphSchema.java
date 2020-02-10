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

/**
 * Determines which of the pre-defined schemas to use for the newly created
 * graph.
 */
public enum TargetGraphSchema {
    /**
     * Create trips as vertices.
     */
    TRIPS_AS_VERTICES,
    /**
     * Create trips as edges.
     */
    TRIPS_AS_EDGES
}
