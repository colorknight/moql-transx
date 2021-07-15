package org.datayoo.moql.translator.query.handler;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.datayoo.moql.translator.query.MongoQueryConstant;

import java.util.Map;

/**
 * @author BLADE
 * @version 1.0
 * @className AndHandler
 * @description TODO
 * @date 7/13/2021 5:06 PM
 **/
public class AndHandler extends AbstractHandler<JsonObject, JsonArray> {
  @Override
  protected JsonArray separateInnerPart(JsonObject dslPart,
      Map<String, Object> context) {
    return dslPart.get(MongoQueryConstant.AND).getAsJsonArray();
  }

  @Override
  protected String doHandle(JsonObject dslPart, Map<String, Object> context) {
    return "{$and:[%s]}";
  }

  @Override
  protected String assembleInnerData(JsonArray jsonArray,
      Map<String, Object> context) {
    String sql = "";
    HandlerFactory handlerFactory = HandlerFactory.getInstance();
    for (int i = 0; i < jsonArray.size(); i++) {
      JsonObject jsonObject = jsonArray.get(i).getAsJsonObject();
      Map.Entry<String, JsonElement> jsonElementEntry = jsonObject.entrySet()
          .stream().findFirst().get();
      try {
        sql += handlerFactory.getHandlerByType(jsonElementEntry.getKey())
            .handle(jsonObject, context);
        sql += ",";
      } catch (Exception e) {
        if (logger.isErrorEnabled()) {
          e.printStackTrace();
          logger.error("assembleInnerData error!", e);
        }
      }
    }
    sql = sql.substring(0, sql.length() - 1);
    return sql;
  }
}
