package org.datayoo.moql.translator.query.handler;

import com.google.gson.JsonObject;
import org.datayoo.moql.translator.query.MongoQueryConstant;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author BLADE
 * @version 1.0
 * @className GroupHandler
 * @description TODO
 * @date 7/15/2021 2:19 PM
 **/
public class GroupHandler extends AbstractHandler<JsonObject, JsonObject> {

  @Override
  protected JsonObject separateInnerPart(JsonObject dslPart,
      Map<String, Object> context) {
    return dslPart.get(MongoQueryConstant.GROUP).getAsJsonObject();
  }

  @Override
  protected String doHandle(JsonObject dslPart, Map<String, Object> context) {
    return "{$group:{%s}}";
  }

  @Override
  protected String assembleInnerData(JsonObject jsonObject,
      Map<String, Object> context) {
    return jsonObject.entrySet().stream()
        .map(e -> e.getKey() + ":" + e.getValue())
        .collect(Collectors.joining(","));
  }
}
