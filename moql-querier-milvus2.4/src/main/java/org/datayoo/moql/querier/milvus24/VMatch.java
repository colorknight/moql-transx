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
import org.datayoo.moql.EntityMap;
import org.datayoo.moql.Operand;
import org.datayoo.moql.operand.function.AbstractFunction;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Tang Tadin
 */
public class VMatch extends AbstractFunction {

  public static final String FUNCTION_NAME = "vMatch";

  protected String vectorName;

  protected List<List> vectorArray = new LinkedList<>();

  public VMatch(List<Operand> parameters) {
    super(FUNCTION_NAME, 2, parameters);
    vectorName = getParameters().get(0).getName();
    String v = (String) getParameters().get(1).operate((EntityMap) null);
    Gson gson = new GsonBuilder().create();
    vectorArray = toFloat(gson.fromJson(v, List.class));
  }

  protected List toFloat(List vectorArray) {
    List nVectorArray = new LinkedList();
    for (Object o : vectorArray) {
      List l = (List) o;
      List nl = new LinkedList();
      for (Object o1 : l) {
        Number n = (Number) o1;
        nl.add(n.floatValue());
      }
      nVectorArray.add(nl);
    }
    return nVectorArray;
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

  public List<List> getVectorArray() {
    return vectorArray;
  }

}
