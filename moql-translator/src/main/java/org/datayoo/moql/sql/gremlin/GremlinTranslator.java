package org.datayoo.moql.sql.gremlin;

import org.apache.commons.lang3.Validate;
import org.datayoo.moql.Filter;
import org.datayoo.moql.Selector;
import org.datayoo.moql.core.SelectorImpl;
import org.datayoo.moql.core.SetlectorImpl;
import org.datayoo.moql.core.Table;
import org.datayoo.moql.core.Tables;
import org.datayoo.moql.core.table.SelectorTable;
import org.datayoo.moql.metadata.TableMetadata;
import org.datayoo.moql.operand.function.Regex;
import org.datayoo.moql.sql.FunctionTranslator;
import org.datayoo.moql.sql.SqlTranslator;
import org.datayoo.moql.sql.es.*;
import org.datayoo.moql.sql.mongodb.MongoFunctionTranslator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author tangtadin
 * @version 1.0
 * @description: TODO
 * @date 2023/2/9 17:12
 */
public class GremlinTranslator implements SqlTranslator {


  protected Map<String, FunctionTranslator> functionTranslators = new HashMap<String, FunctionTranslator>();

  {
  }

  @Override
  public String translate2Sql(Selector selector) {
    return translate2Sql(selector, new HashMap<String, Object>());
  }

  @Override
  public String translate2Sql(Selector selector,
      Map<String, Object> translationContext) {
    Validate.notNull(selector, "selector is null!");
    if (selector instanceof SelectorImpl) {
      return translate2Sql((SelectorImpl) selector);
    } else {
      return translate2Sql((SetlectorImpl) selector, translationContext);
    }
  }

  protected String translate2Sql(SelectorImpl selector) {
    StringBuilder sbud = new StringBuilder();

    return null;
  }

  protected String translate2FromClause(Tables tables,
      Map<String, Object> translationContext) {
    StringBuilder sbud = new StringBuilder();
    boolean multiTables = false;
    if (tables.getQueryable() instanceof Table) {
      return translateTable((Table) tables.getQueryable(), translationContext);
    } else {
      throw new UnsupportedOperationException("Unsupported grammar!");
    }
  }

  protected String translateTable(Table table,
      Map<String, Object> translationContext) {
    StringBuffer sbuf = new StringBuffer();
    TableMetadata tableMetadata = table.getTableMetadata();
    if (table instanceof SelectorTable) {
      SelectorTable selectorTable = (SelectorTable) table;
      sbuf.append("(");
      sbuf.append(translate2Sql(selectorTable.getSelector()));
      sbuf.append(") ");
    } else {
      sbuf.append(tableMetadata.getValue());
      sbuf.append(" ");
    }
    sbuf.append(tableMetadata.getName());
    sbuf.append(" ");
    return sbuf.toString();
  }

  protected void validateTable(String tableName) {

  }

  protected String translate2Sql(SetlectorImpl setlector,
      Map<String, Object> translationContext) {
    throw new UnsupportedOperationException("pending...");
  }

  @Override
  public String translate2Condition(Filter filter) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public String translate2Condition(Filter filter,
      Map<String, Object> translationContext) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public synchronized void addFunctionTranslator(
      FunctionTranslator functionTranslator) {
    functionTranslators.put(functionTranslator.getFunctionName(),
        functionTranslator);
  }

  @Override
  public synchronized void addAllFunctionTranslator(
      List<FunctionTranslator> functiionTranslators) {
    for (FunctionTranslator functionTranslator : functiionTranslators) {
      functionTranslators.put(functionTranslator.getFunctionName(),
          functionTranslator);
    }
  }

  @Override
  public synchronized FunctionTranslator removeFunctionTranslator(
      String functionName) {
    return functionTranslators.remove(functionName);
  }

  @Override
  public synchronized List<FunctionTranslator> getFunctionTranslators() {
    return new LinkedList<>(functionTranslators.values());
  }
}
