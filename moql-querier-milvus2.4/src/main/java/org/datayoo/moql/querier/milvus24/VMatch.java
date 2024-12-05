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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.milvus.param.MetricType;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.data.SparseFloatVec;
import org.datayoo.moql.EntityMap;
import org.datayoo.moql.Operand;
import org.datayoo.moql.operand.function.AbstractFunction;

import java.util.*;

/**
 * @author Tang Tadin
 */
public class VMatch extends AbstractFunction {

  public static final String FUNCTION_NAME = "vMatch";

  protected String vectorName;

  protected MetricType metricType;

  protected List<BaseVector> vectors = new LinkedList<>();

  public VMatch(List<Operand> parameters) {
    super(FUNCTION_NAME, 3, parameters);
    vectorName = getParameters().get(0).getName();
    String v = (String) getParameters().get(1).operate((EntityMap) null);
    metricType = MetricType.valueOf(v.toUpperCase());
    v = (String) getParameters().get(2).operate((EntityMap) null);
    Gson gson = new GsonBuilder().create();
    List list = gson.fromJson(v, List.class);
    vectors = toVectors(list);
  }

  protected List<BaseVector> toVectors(List list) {
    List vectors = new LinkedList();
    for (Object o : list) {
      if (o instanceof List) {
        vectors.add(toVector((List) o));
      } else if (o instanceof Map) {
        vectors.add(toVector((Map) o));
      }
    }
    return vectors;
  }

  protected FloatVec toVector(List list) {
    List<Float> vectors = new LinkedList<>();
    for (Object o1 : list) {
      Number n = (Number) o1;
      vectors.add(n.floatValue());
    }
    return new FloatVec(vectors);
  }

  protected SparseFloatVec toVector(Map map) {
    SortedMap<Long, Float> vectorMap = new TreeMap<>();
    for (Object obj : map.entrySet()) {
      Map.Entry entry = (Map.Entry) obj;
      vectorMap.put(Long.valueOf(entry.getKey().toString()),
          Float.valueOf(entry.getValue().toString()));
    }
    return new SparseFloatVec(vectorMap);
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

  public String getVectorName() {
    return vectorName;
  }

  public MetricType getMetricType() {
    return metricType;
  }

  public void setMetricType(MetricType metricType) {
    this.metricType = metricType;
  }

  public List<BaseVector> getVectors() {
    return vectors;
  }

}
