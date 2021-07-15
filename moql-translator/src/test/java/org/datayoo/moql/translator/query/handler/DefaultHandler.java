package org.datayoo.moql.translator.query.handler;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author BLADE
 * @version 1.0
 * @className DefaultHandler
 * @description TODO
 * @date 7/12/2021 7:37 PM
 **/
public class DefaultHandler extends AbstractHandler<JsonElement, JsonElement> {

  @Override
  protected JsonElement separateInnerPart(JsonElement dslPart,
      Map<String, Object> context) {
    return null;
  }

  @Override
  protected String doHandle(JsonElement dslPart, Map<String, Object> context) {
    if (dslPart.isJsonArray()) {

    } else if (dslPart.isJsonObject()) {
      JsonObject jsonObject = dslPart.getAsJsonObject();
      try {
        Map.Entry<String, JsonElement> entry = jsonObject.entrySet().stream()
            .findFirst().get();
        String key = entry.getKey();
        JsonElement value = entry.getValue();
        String newResult = value.getAsJsonObject().entrySet().stream()
            .map(e -> {
              //
              if (e.getValue().getAsString().contains("ISODate")) {
                return e.getKey() + ":" + e.getValue().getAsString();
              } else {
                return e.getKey() + ":" + e.getValue();
              }
            }).collect(Collectors.joining(",", "{", "}"));
        String format = "{%s:%s}";
        return String.format(format, key, newResult);
      } catch (Exception e) {
        if (logger.isWarnEnabled()) {
          logger.warn("format sql error!", e);
        }
      }
      return jsonObject.toString();
    }
    return "";
  }

  @Override
  protected String assembleInnerData(JsonElement jsonElement,
      Map<String, Object> context) {
    return null;
  }
}
