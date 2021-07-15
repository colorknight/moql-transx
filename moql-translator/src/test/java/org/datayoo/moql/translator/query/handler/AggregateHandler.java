package org.datayoo.moql.translator.query.handler;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.datayoo.moql.translator.query.MongoQueryConstant;

import java.util.Map;

/**
 * @author BLADE
 * @version 1.0
 * @className AggregateHandler
 * @description TODO
 * @date 7/15/2021 2:10 PM
 **/
public class AggregateHandler extends AbstractHandler<JsonObject, JsonArray> {
  @Override
  protected JsonArray separateInnerPart(JsonObject dslPart,
      Map<String, Object> context) {
    return dslPart.get(MongoQueryConstant.AGGREGATE).getAsJsonArray();
  }

  @Override
  protected String doHandle(JsonObject dslPart, Map<String, Object> context) {
    return "";
  }

  @Override
  protected String assembleInnerData(JsonArray jsonArray,
      Map<String, Object> context) {
    HandlerFactory handlerFactory = HandlerFactory.getInstance();
    String sql = "";
    for (int i = 0; i < jsonArray.size(); i++) {
      JsonObject jsonObject = jsonArray.get(i).getAsJsonObject();
      String key = jsonObject.entrySet().stream().findFirst().get().getKey();
      try {
        sql += handlerFactory.getHandlerByType(key).handle(jsonObject, context);
        sql += ",";
      } catch (Exception e) {
        e.printStackTrace();
        if (logger.isErrorEnabled()) {
          logger.error("assembleInnerData error!", e);
        }
      }
    }
    sql = sql.substring(0, sql.length() - 1);
    return sql;
  }
}
