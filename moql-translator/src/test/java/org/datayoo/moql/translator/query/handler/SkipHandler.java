package org.datayoo.moql.translator.query.handler;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.datayoo.moql.translator.query.MongoQueryConstant;

import java.util.Map;

/**
 * @author BLADE
 * @version 1.0
 * @className SkipHandler
 * @description TODO
 * @date 7/15/2021 5:52 PM
 **/
public class SkipHandler extends AbstractHandler<JsonObject, JsonElement> {
  @Override
  protected JsonElement separateInnerPart(JsonObject dslPart,
      Map<String, Object> context) {
    return null;
  }

  @Override
  protected String doHandle(JsonObject dslPart, Map<String, Object> context) {
    String queryType = (String) context.get(MongoQueryConstant.QUERY_TYPE);
    if (MongoQueryConstant.QUERY_TYPE_FIND.equals(queryType)) {
      String format = "skip(%d)";
      return String
          .format(format, dslPart.get(MongoQueryConstant.SKIP).getAsInt());
    } else {
      return dslPart.toString();
    }
  }

  @Override
  protected String assembleInnerData(JsonElement jsonElement,
      Map<String, Object> context) {
    return null;
  }
}
