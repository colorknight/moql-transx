package org.datayoo.moql.translator.query;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.commons.lang.Validate;
import org.datayoo.moql.translator.query.handler.RootHandler;

import java.util.HashMap;

/**
 * @author BLADE
 * @version 1.0
 * @className Process
 * @description dsl解析中心
 * @date 7/3/2021 1:57 AM
 **/
public class Process {
  private String dsl;

  public Process(String dsl) {
    Validate.notEmpty(dsl, "dsl must not empty!");
    this.dsl = dsl;
  }

  public String process() {
    JsonElement jsonElement = preProcess();

    HashMap<String, Object> context = new HashMap<>();
    return new RootHandler().handle(jsonElement, context);
  }

  private JsonElement preProcess() {
    return parserDsl();
  }

  private JsonElement parserDsl() {
    JsonParser jsonParser = new JsonParser();
    return jsonParser.parse(dsl);
  }

}
