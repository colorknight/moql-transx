package org.datayoo.moql.translator.query.handler;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.datayoo.moql.translator.query.MongoQueryConstant;

import java.util.Map;

/**
 * @author BLADE
 * @version 1.0
 * @className OrHandler
 * @description TODO
 * @date 7/14/2021 6:31 PM
 **/
public class OrHandler extends AbstractHandler<JsonElement, JsonArray> {

  @Override
  protected JsonArray separateInnerPart(JsonElement dslPart,
      Map<String, Object> context) {
    JsonObject asJsonObject = dslPart.getAsJsonObject();
    return asJsonObject.get(MongoQueryConstant.OR).getAsJsonArray();
  }

  @Override
  protected String doHandle(JsonElement dslPart, Map<String, Object> context) {
    return "{$or:[%s]}";
  }

  @Override
  protected String assembleInnerData(JsonArray jsonArray,
      Map<String, Object> context) {
    HandlerFactory handlerFactory = HandlerFactory.getInstance();
    String sql = "";
    for (int i = 0; i < jsonArray.size(); i++) {
      JsonElement jsonElement = jsonArray.get(i);
      Map.Entry<String, JsonElement> firstEntry = jsonElement.getAsJsonObject()
          .entrySet().stream().findFirst().get();
      try {
        sql += handlerFactory.getHandlerByType(firstEntry.getKey())
            .handle(jsonElement, context);
        sql += ",";
      } catch (Exception e) {
        e.printStackTrace();///??????????????????????????????????????
        if (logger.isErrorEnabled()) {
          logger.error("assembleInnerData", e);
        }
      }
    }
    sql = sql.substring(0, sql.length() - 1);
    return sql;
  }
}
