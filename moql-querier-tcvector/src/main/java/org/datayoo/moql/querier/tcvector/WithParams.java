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

import com.tencent.tcvectordb.model.param.dml.GeneralParams;
import com.tencent.tcvectordb.model.param.dml.HNSWSearchParams;
import com.tencent.tcvectordb.model.param.dml.Params;
import org.apache.commons.lang3.StringUtils;
import org.datayoo.moql.EntityMap;
import org.datayoo.moql.MoqlException;
import org.datayoo.moql.Operand;
import org.datayoo.moql.operand.function.AbstractFunction;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Tang Tadin
 */
public class WithParams extends AbstractFunction {

  public static final String FUNCTION_NAME = "withParams";

  protected Params params;

  public WithParams(List<Operand> parameters) {
    super(FUNCTION_NAME, 0, parameters);
    String v = (String) getParameters().get(0).operate((EntityMap) null);
    if (v.startsWith("HNSWSearchParams")) {
      Pattern pattern = Pattern.compile("\\((.*?)\\)");
      Matcher matcher = pattern.matcher(v);

      int ef = Integer.parseInt(matcher.group(1));
      params = new HNSWSearchParams(ef);
    }
    if (v.startsWith("GeneralParams")) {
      Pattern pattern = Pattern.compile("\\((.*?)\\)");
      Matcher matcher = pattern.matcher(v);
      String[] buildParams = StringUtils.split(matcher.group(1), ",");
      int ef = Integer.parseInt(buildParams[0]);
      int NProbe = Integer.parseInt(buildParams[1]);
      double Radius = Double.parseDouble(buildParams[2]);
      GeneralParams.Builder builder = GeneralParams.newBuilder();
      builder.withEf(ef);
      builder.withNProbe(NProbe);
      builder.withRadius(Radius);
      params = builder.build();
    }
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

  public Params getParams() throws MoqlException {
    if (params == null) {
      throw new MoqlException("params is wrong");
    }
    return params;
  }
}
