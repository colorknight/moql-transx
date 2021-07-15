package org.datayoo.moql.translator.query.handler;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.datayoo.moql.translator.query.MongoQueryConstant;

import java.util.Map;

/**
 * @author BLADE
 * @version 1.0
 * @className ProjectQueryHandler
 * @description TODO
 * @date 7/3/2021 4:01 AM
 **/
public class ProjectQueryHandler extends AbstractHandler<JsonObject, JsonElement> {

  @Override
  protected JsonElement separateInnerPart(JsonObject dslPart,
      Map<String, Object> context) {
    return null;
  }

  @Override
  protected String doHandle(JsonObject jsonObject,
      Map<String, Object> context) {
    String queryType = (String) context.get(MongoQueryConstant.QUERY_TYPE);
    if(MongoQueryConstant.QUERY_TYPE_FIND.equals(queryType)){
      JsonElement element = jsonObject.get(MongoQueryConstant.PROJECT);
      if (null != element) {
        return element.toString();
      }
      return "";
    }else {
      return jsonObject.toString();
    }
  }

  @Override
  protected String assembleInnerData(JsonElement jsonElement,
      Map<String, Object> context) {
    return null;
  }
}
