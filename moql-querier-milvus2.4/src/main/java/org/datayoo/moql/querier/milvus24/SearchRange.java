/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datayoo.moql.querier.milvus24;

import io.milvus.param.MetricType;
import org.datayoo.moql.EntityMap;
import org.datayoo.moql.Operand;
import org.datayoo.moql.operand.function.AbstractFunction;

import java.util.List;

/**
 * @author Tang Tadin
 */
public class SearchRange extends AbstractFunction {

  public static final String FUNCTION_NAME = "searchRange";

  protected MetricType metricType;

  protected float radius;

  protected float rangeFilter;

  public SearchRange(List<Operand> parameters) {
    super(FUNCTION_NAME, 3, parameters);
    metricType = MetricType.valueOf(
        (String) parameters.get(0).operate((EntityMap) null));
    radius = ((Double) parameters.get(1)
        .operate((EntityMap) null)).floatValue();
    rangeFilter = ((Double) parameters.get(2)
        .operate((EntityMap) null)).floatValue();
  }

  /* (non-Javadoc)
   * @see org.moql.operand.function.AbstractFunction#innerOperate(org.moql.data.EntityMap)
   */
  @Override
  protected Object innerOperate(EntityMap entityMap) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Object innerOperate(Object[] entityArray) {
    throw new UnsupportedOperationException();
  }

  public MetricType getMetricType() {
    return metricType;
  }

  public float getRadius() {
    return radius;
  }

  public float getRangeFilter() {
    return rangeFilter;
  }
}