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
package org.gradoop.examples.dataintegration.citibike.temporal;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.temporal.model.api.functions.TimeIntervalExtractor;

import java.util.Objects;

/**
 * A filter function removing all elements where some time interval is not
 * valid, i.e. its start time is after its end time. This uses an extractor
 * function to determine the time interval.
 *
 * @param <E> The element type.
 */
public class RemoveElementsWithInvalidTimes<E extends Element> implements FilterFunction<E> {

    /**
     * The function extracting the time interval from items.
     */
    private final TimeIntervalExtractor<E> intervalExtractor;

    /**
     * Create a new instance of this filter function.
     *
     * @param intervalExtractor The function used to extract the time interval from elements.
     */
    public RemoveElementsWithInvalidTimes(TimeIntervalExtractor<E> intervalExtractor) {
        this.intervalExtractor = Objects.requireNonNull(intervalExtractor);
    }

    @Override
    public boolean filter(E element) throws Exception {
        final Tuple2<Long, Long> time = intervalExtractor.map(element);
        return time.f0 <= time.f1;
    }
}
