package org.datayoo.moql.translator.query.handler;

import com.google.gson.JsonObject;

import java.util.Map;

/**
 * @author BLADE
 * @version 1.0
 * @className CountHandler
 * @description TODO
 * @date 7/16/2021 12:20 AM
 **/
public class CountHandler extends AbstractHandler<JsonObject, JsonObject>{
  @Override
  protected JsonObject separateInnerPart(JsonObject dslPart,
      Map<String, Object> context) {
    return null;
  }

  @Override
  protected String doHandle(JsonObject dslPart, Map<String, Object> context) {
    return "count()";
  }

  @Override
  protected String assembleInnerData(JsonObject jsonElement,
      Map<String, Object> context) {
    return null;
  }
}
