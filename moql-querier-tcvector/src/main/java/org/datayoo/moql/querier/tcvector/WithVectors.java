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
package org.datayoo.moql.querier.tcvector;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.datayoo.moql.EntityMap;
import org.datayoo.moql.Operand;
import org.datayoo.moql.operand.function.AbstractFunction;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Tang Tadin
 */
public class WithVectors extends AbstractFunction {

  public static final String FUNCTION_NAME = "withVectors";

  protected List<List<Double>> vectorArray = new LinkedList<>();


  public WithVectors(List<Operand> parameters) {
    super(FUNCTION_NAME, 1, parameters);
    String v = (String) getParameters().get(0).operate((EntityMap) null);
    Gson gson = new GsonBuilder().create();
    vectorArray = toDouble(gson.fromJson(v, List.class));
  }

  protected List toDouble(List vectorArray) {
    List nVectorArray = new LinkedList();
    for (Object o : vectorArray) {
      List l = (List) o;
      List nl = new LinkedList();
      for (Object o1 : l) {
        Number n = (Number) o1;
        nl.add(n.doubleValue());
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

  public List<List<Double>> getVectorArray() {
    return vectorArray;
  }
}
