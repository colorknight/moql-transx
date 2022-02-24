package org.datayoo.moql.sql;

import org.datayoo.moql.Operand;
import org.datayoo.moql.core.*;
import org.datayoo.moql.core.group.GroupRecordSetOperator;
import org.datayoo.moql.core.table.SelectorTable;
import org.datayoo.moql.metadata.TableMetadata;
import org.datayoo.moql.operand.expression.AbstractOperationExpression;
import org.datayoo.moql.operand.expression.ExpressionType;
import org.datayoo.moql.operand.expression.ParenExpression;
import org.datayoo.moql.operand.expression.relation.InExpression;
import org.datayoo.moql.operand.function.AbstractFunction;
import org.datayoo.moql.operand.selector.ColumnSelectorOperand;
import org.datayoo.moql.operand.selector.ValueSelectorOperand;
import org.datayoo.moql.util.StringFormater;

import java.util.Map;

/**
 * @author tangtadin
 * @version 1.0
 * @description: TODO
 */
public class DynamodbTranslator extends MoqlGrammarTranslator {

  protected String translate2SelectClause(RecordSetOperator recordSetOperator,
      Map<String, Object> translationContext) {
    StringBuffer sbuf = new StringBuffer();
    sbuf.append("select ");
    sbuf.append(
        translateColumns(recordSetOperator.getColumns(), translationContext));
    return sbuf.toString();
  }

  protected String translateColumns(Columns columns,
      Map<String, Object> translationContext) {
    StringBuffer sbuf = new StringBuffer();
    int i = 0;
    for (Column column : columns.getColumns()) {
      if (column.isJustUsed4Order())
        continue;
      if (i != 0) {
        sbuf.append(", ");
      }
      String t = translateOperand(column.getOperand(), translationContext)
          .trim();
      int index = t.indexOf(".");
      if (index != -1) {
        t = t.substring(index + 1);
      }
      sbuf.append(t);
      i++;
    }
    sbuf.append(" ");
    return sbuf.toString();
  }

  protected String translateTable(Table table,
      Map<String, Object> translationContext) {
    TableMetadata tableMetadata = table.getTableMetadata();
    if (table instanceof SelectorTable) {
      throw new UnsupportedOperationException("Unsupport inner select clause!");
    }
    return tableMetadata.getValue();
  }

  protected String translateOperand(Operand operand,
      Map<String, Object> translationContext) {
    StringBuffer sbuf = new StringBuffer();
    if (operand instanceof AbstractOperationExpression) {
      AbstractOperationExpression expression = (AbstractOperationExpression) operand;
      if (expression.getExpressionType() == ExpressionType.LOGIC) {
        sbuf.append(translateLogicExpression(expression, translationContext));
      } else if (expression.getExpressionType() == ExpressionType.RELATION) {
        sbuf.append(
            translateRelationExpression(expression, translationContext));
      } else if (expression.getExpressionType() == ExpressionType.ARITHMETIC) {
        sbuf.append(
            translateArithmeticExpression(expression, translationContext));
      } else if (expression.getExpressionType() == ExpressionType.BITWISE) {
        sbuf.append(translateBitwiseExpression(expression, translationContext));
      } else {
        throw new IllegalArgumentException(StringFormater
            .format("Doesn't support operand with type '{}'!",
                expression.getExpressionType()));
      }
    } else if (operand instanceof ParenExpression) {
      ParenExpression parenExpression = (ParenExpression) operand;
      sbuf.append(
          translateParenExpression(parenExpression, translationContext));
    } else if (operand instanceof ColumnSelectorOperand) {
      ColumnSelectorOperand selectorOperand = (ColumnSelectorOperand) operand;
      sbuf.append(translate2Sql(selectorOperand.getColumnSelector()));
    } else if (operand instanceof ValueSelectorOperand) {
      ValueSelectorOperand selectorOperand = (ValueSelectorOperand) operand;
      sbuf.append("(");
      sbuf.append(translate2Sql(selectorOperand.getValueSelector()).trim());
      sbuf.append(") ");
    } else if (operand instanceof AbstractFunction) {
      AbstractFunction function = (AbstractFunction) operand;
      sbuf.append(translateFunction(function, translationContext));
    } else {
      String t = operand.getName();
      int index = t.indexOf('.');
      if (index != -1) {
        t = t.substring(index + 1);
      }
      sbuf.append(t);
      sbuf.append(" ");
    }
    return sbuf.toString();
  }

  @Override
  protected String translateInExpression(InExpression expression,
      Map<String, Object> translationContext) {
    StringBuffer sbuf = new StringBuffer();
    sbuf.append(
        translateOperand(expression.getLeftOperand(), translationContext));
    sbuf.append("in [");
    int i = 0;
    for (Operand rOperand : expression.getrOperands()) {
      if (i != 0) {
        sbuf.append(", ");
      }
      sbuf.append(translateOperand(rOperand, translationContext).trim());
      i++;
    }
    sbuf.append("] ");
    return sbuf.toString();
  }

  @Override
  protected String translate2Sql(SetlectorImpl setlector,
      Map<String, Object> translationContext) {
    throw new UnsupportedOperationException("Unsupport set operator!");
  }

  @Override
  protected String translate2LimitClause(Limit limit,
      Map<String, Object> translationContext) {
    throw new UnsupportedOperationException("Unsupport limit clause!");
  }

  @Override
  protected String translateJoin(Join join, boolean multiTables,
      Map<String, Object> translationContext) {
    throw new UnsupportedOperationException("Unsupport join clause!");
  }

  @Override
  protected String translate2GroupbyClause(
      GroupRecordSetOperator groupRecordSetOperator,
      Map<String, Object> translationContext) {
    throw new UnsupportedOperationException("Unsupport groupBy clause!");
  }

  @Override
  protected String translate2HavingClause(HavingImpl having,
      Map<String, Object> translationContext) {
    throw new UnsupportedOperationException("Unsupport having clause!");
  }
}
