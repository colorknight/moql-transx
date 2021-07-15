package org.datayoo.moql.translator.query.handler;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.datayoo.moql.translator.query.MongoQueryConstant;

import java.util.HashMap;
import java.util.Map;

/**
 * @author BLADE
 * @version 1.0
 * @className RootHandler
 * @description TODO
 * @date 7/3/2021 2:38 AM
 **/
public class RootHandler extends AbstractHandler<JsonElement, JsonElement> {

  @Override
  protected JsonElement separateInnerPart(JsonElement dslPart,
      Map<String, Object> context) {
    return dslPart;
  }

  @Override
  protected String doHandle(JsonElement jsonElement,
      Map<String, Object> context) {
    return null;
  }

  @Override
  protected String assembleInnerData(JsonElement jsonElement,
      Map<String, Object> context) {
    JsonArray jsonArray = jsonElement.getAsJsonArray();
    HandlerFactory instance = HandlerFactory.getInstance();

    Map<String, String> localContext = new HashMap<>(6);

    preHandleInner(jsonArray);

    for (int i = 0; i < jsonArray.size(); i++) {
      innerPart(jsonArray.get(i).getAsJsonObject(), instance, context,
          localContext);
    }

    String sql = assembleSql(localContext);
    return sql;
  }

  /**
   * 所有类型排序
   *
   * @param jsonArray
   */
  private void preHandleInner(JsonArray jsonArray) {
    boolean hasMatch = false;
    for (int i = 0; i < jsonArray.size(); i++) {
      JsonElement subElement = jsonArray.get(i);
      JsonElement queryTypeE = subElement.getAsJsonObject()
          .get(MongoQueryConstant.QUERY_TYPE);
      if (null != queryTypeE) {
        if (i != 0) {
          jsonArray.set(i, jsonArray.get(0));
          jsonArray.set(0, subElement);
        }
      }
      JsonElement matchE = subElement.getAsJsonObject()
          .get(MongoQueryConstant.MATCH);
      if (null != matchE) {
        hasMatch = true;
      }
    }
    if (!hasMatch) {
      appendEmptyMatch(jsonArray);
    }
  }

  private void appendEmptyMatch(JsonArray jsonArray) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty(MongoQueryConstant.MATCH, "");
    jsonArray.add(jsonObject);
  }

  private void innerPart(JsonObject jsonObject, HandlerFactory instance,
      Map<String, Object> context, Map<String, String> localContext) {

    // handler清楚自己的结构，这里只取第一个
    Map.Entry<String, JsonElement> jsonEntry = jsonObject.entrySet().stream()
        .findFirst().get();
    try {
      String key = jsonEntry.getKey();
      String handleResult = instance.getHandlerByType(key)
          .handle(jsonObject, context);
      localContext.put(key, handleResult);
      // 传递queryType
      if (MongoQueryConstant.QUERY_TYPE.equals(key)) {
        context.put(key, handleResult);
      }
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("assembleInnerData error", e);
    }
  }

  private String assembleSql(Map<String, String> localContext) {
    String queryCollection = getResult(localContext,
        MongoQueryConstant.QUERY_COLLECTION);
    String queryType = getResult(localContext, MongoQueryConstant.QUERY_TYPE);
    String match = getResult(localContext, MongoQueryConstant.MATCH);
    String aggregate = getResult(localContext, MongoQueryConstant.AGGREGATE);
    String project = getResult(localContext, MongoQueryConstant.PROJECT);
    String limit = getResult(localContext, MongoQueryConstant.LIMIT);
    String skip = getResult(localContext, MongoQueryConstant.SKIP);
    String count = getResult(localContext, MongoQueryConstant.COUNT);

    String sqlFormat = "db.%s.%s(%s%s%s)%s%s%s";
    return String
        .format(sqlFormat, queryCollection, queryType, match, aggregate,
            appendPrefix(",", project), appendPrefix(".", limit),
            appendPrefix(".", skip), appendPrefix(".", count));
  }

  private String getResult(Map<String, String> localContext, String type) {
    String result = localContext.get(type);
    return null == result ? "" : result;
  }

  private String appendPrefix(String prefix, String content) {
    if (!"".equals(content)) {
      return prefix + content;
    }
    return content;
  }
}
