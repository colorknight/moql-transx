package org.datayoo.moql.translator.query.handler;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.datayoo.moql.translator.query.MongoQueryConstant;

import java.util.Map;

/**
 * @author BLADE
 * @version 1.0
 * @className QueryCollection
 * @description TODO
 * @date 7/3/2021 3:39 AM
 **/
public class QueryCollectionHandler extends AbstractHandler<JsonObject, JsonElement> {

  @Override
  protected JsonElement separateInnerPart(JsonObject dslPart,
      Map<String, Object> context) {
    return null;
  }

  @Override
  protected String doHandle(JsonObject jsonObject,
      Map<String, Object> context) {
    return jsonObject.get(MongoQueryConstant.QUERY_COLLECTION).getAsString();
  }

  @Override
  protected String assembleInnerData(JsonElement jsonElement,
      Map<String, Object> context) {
    return null;
  }
}
