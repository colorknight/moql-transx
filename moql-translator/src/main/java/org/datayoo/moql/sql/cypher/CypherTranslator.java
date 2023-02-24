package org.datayoo.moql.sql.cypher;

import org.datayoo.moql.Filter;
import org.datayoo.moql.Selector;
import org.datayoo.moql.sql.FunctionTranslator;
import org.datayoo.moql.sql.SqlTranslator;

import java.util.List;
import java.util.Map;

/**
 * @author tangtadin
 * @version 1.0
 * @description: TODO
 * @date 2022/6/27 12:39 PM
 */
public class CypherTranslator implements SqlTranslator {

  public static final String NODE_PREFIX = "n";
  public static final String RELATION_PREFIX = "r";

  @Override
  public String translate2Sql(Selector selector) {
    return null;
  }

  @Override
  public String translate2Sql(Selector selector,
      Map<String, Object> translationContext) {
    return null;
  }

  @Override
  public String translate2Condition(Filter filter) {
    return null;
  }

  @Override
  public String translate2Condition(Filter filter,
      Map<String, Object> translationContext) {
    return null;
  }

  @Override
  public void addFunctionTranslator(FunctionTranslator functionTranslator) {

  }

  @Override
  public void addAllFunctionTranslator(
      List<FunctionTranslator> functiionTranslators) {

  }

  @Override
  public FunctionTranslator removeFunctionTranslator(String functionName) {
    return null;
  }

  @Override
  public List<FunctionTranslator> getFunctionTranslators() {
    return null;
  }
}
