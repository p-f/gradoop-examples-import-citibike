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
package org.gradoop.examples.dataintegration.citibike.operators.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Create a {@link Tuple3 triple} for each input object, containing the input object unchanged as well as
 * two new {@link GradoopId ids}.
 *
 * @param <O> The object type.
 */
public class ObjectToTripleWithNewIds<O> implements MapFunction<O, Tuple3<O, GradoopId, GradoopId>> {

  /**
   * A tuple to be reused for output.
   */
  private final Tuple3<O, GradoopId, GradoopId> reuseTuple = new Tuple3<>();

  @Override
  public Tuple3<O, GradoopId, GradoopId> map(O input) {
    reuseTuple.f0 = input;
    reuseTuple.f1 = GradoopId.get();
    reuseTuple.f2 = GradoopId.get();
    return reuseTuple;
  }
}
