package org.datayoo.moql.translator.query.handler;

import com.google.gson.JsonElement;

import java.util.Map;

/**
 * @author BLADE
 * @version 1.0
 * @interfaceName Handler
 * @description TODO
 * @date 7/3/2021 2:07 AM
 **/
public interface Handler<T extends JsonElement> {
  String handle(T dslPart, Map<String, Object> context);
}
