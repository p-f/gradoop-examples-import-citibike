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

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

import static java.util.Collections.singletonList;

/**
 * Test for the {@link SplitVertex} operator.
 */
public class SplitVertexTest extends GradoopFlinkTestBase {

    /**
     * Test the operator execution on a graph.
     *
     * @throws Exception when the execution in Flink fails.
     */
    @Test
    public void testWithGraph() throws Exception {
        final String labelSource = "s";
        final String labelTarget = "t";
        final List<String> propertiesSource = singletonList("a");
        final List<String> propertiesTarget = singletonList("b");
        final FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
                "(:a{a: 1L, b: 2L, c: 0L})(:b{a: 3L, d: 5L})(:c{b:4L, e: 6L})" +
                "] expected [" +
                "(:s {a: 1L})-[:a {c: 0L}]->(:t {b: 2L})" +
                "(:s {a: 3L})-[:b {d: 5L}]->(:t)" +
                "(:s)-[:c {e: 6L}]->(:t {b: 4L})" +
                "]");
        LogicalGraph result = loader.getLogicalGraphByVariable("input")
                .callForGraph(new SplitVertex<>(labelSource, labelTarget,
                        propertiesSource, propertiesTarget));
        LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
        collectAndAssertTrue(result.equalsByData(expected));
    }
}
