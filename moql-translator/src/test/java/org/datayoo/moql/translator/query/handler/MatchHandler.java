package org.datayoo.moql.translator.query.handler;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.datayoo.moql.translator.query.MongoQueryConstant;

import java.util.Map;

/**
 * @author BLADE
 * @version 1.0
 * @className MatchHandler
 * @description TODO
 * @date 7/9/2021 2:13 PM
 **/
public class MatchHandler extends AbstractHandler<JsonObject, JsonElement> {

  @Override
  protected JsonElement separateInnerPart(JsonObject dslPart,
      Map<String, Object> context) {
    return dslPart.get(MongoQueryConstant.MATCH);
  }

  @Override
  protected String doHandle(JsonObject jsonObject,
      Map<String, Object> context) {
    String queryType = (String) context.get(MongoQueryConstant.QUERY_TYPE);
    if (MongoQueryConstant.QUERY_TYPE_FIND.equals(queryType)) {
      JsonElement mathE = jsonObject.get(MongoQueryConstant.MATCH);
      if (mathE.isJsonObject()) {
        return "";
      } else {
        if (StringUtils.isEmpty(mathE.getAsString())) {
          return "{}";
        }
      }
    } else {
      return "{$match:%s}";
    }
    return "";
  }

  @Override
  protected String assembleInnerData(JsonElement jsonElement,
      Map<String, Object> context) {

    if (jsonElement.isJsonObject()) {
      HandlerFactory handlerFactory = HandlerFactory.getInstance();
      JsonObject jsonObject = jsonElement.getAsJsonObject();
      String key = jsonObject.entrySet().stream().findFirst().get().getKey();
      try {
        return handlerFactory.getHandlerByType(key).handle(jsonObject, context);
      } catch (Exception exception) {
        if (logger.isErrorEnabled()) {
          logger.error("assembleInnerData error!", exception);
        }
        exception.printStackTrace();
        return "";
      }
    } else if (jsonElement.isJsonPrimitive()) {
      return null;
    }
    return "";
  }
}
