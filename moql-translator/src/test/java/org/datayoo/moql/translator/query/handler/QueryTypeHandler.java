package org.datayoo.moql.translator.query.handler;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.datayoo.moql.translator.query.MongoQueryConstant;

import java.util.Map;

/**
 * @author BLADE
 * @version 1.0
 * @className QueryTypeHandler
 * @description TODO
 * @date 7/3/2021 3:08 AM
 **/
public class QueryTypeHandler extends AbstractHandler<JsonObject, JsonElement> {

  @Override
  protected JsonElement separateInnerPart(JsonObject dslPart,
      Map<String, Object> context) {
    return null;
  }

  @Override
  protected String doHandle(JsonObject jsonObject,
      Map<String, Object> context) {
    JsonElement queryTypeE = jsonObject.get(MongoQueryConstant.QUERY_TYPE);
    // find(%s) / aggregate(%s)
    return queryTypeE.getAsString();
  }

  @Override
  protected String assembleInnerData(JsonElement jsonElement,
      Map<String, Object> context) {
    return null;
  }
}
