package org.datayoo.moql.querier.util;

import org.datayoo.moql.ColumnDefinition;
import org.datayoo.moql.SelectorDefinition;
import org.datayoo.moql.core.RecordSetImpl;
import org.datayoo.moql.core.RecordSetMetadata;
import org.datayoo.moql.metadata.ColumnMetadata;
import org.datayoo.moql.metadata.GroupMetadata;
import org.datayoo.moql.metadata.SelectorMetadata;
import org.datayoo.moql.util.StringFormater;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * @author tangtadin
 * @version 1.0
 * @description: TODO
 * @date 2022/2/24 11:12 AM
 */
public abstract class SelectorDefinitionUtils {

  public static RecordSetImpl createRecordSet(
      SelectorDefinition selectorDefinition) {
    SelectorMetadata selectorMetadata = (SelectorMetadata) selectorDefinition;
    List<ColumnDefinition> columns = new LinkedList<ColumnDefinition>();
    for (ColumnMetadata columnMetadata : selectorMetadata.getColumns()
        .getColumns()) {
      columns.add(columnMetadata);
    }
    List<ColumnDefinition> groupColumns = new LinkedList<ColumnDefinition>();
    if (selectorMetadata.getGroupBy() != null) {
      for (GroupMetadata groupMetadata : selectorMetadata.getGroupBy()) {
        ColumnDefinition columnDefinition = getColumnDefinition(
            groupMetadata.getColumn(), columns);
        groupColumns.add(columnDefinition);
      }
    }
    RecordSetMetadata recordSetMetadata = new RecordSetMetadata(columns,
        groupColumns);
    return new RecordSetImpl(recordSetMetadata, new Date(), new Date(),
        new LinkedList<Object[]>());
  }

  public static RecordSetImpl createRecordSetWithoutTablePrefix(
      SelectorDefinition selectorDefinition) {
    SelectorMetadata selectorMetadata = (SelectorMetadata) selectorDefinition;
    List<ColumnDefinition> columns = new LinkedList<ColumnDefinition>();
    for (ColumnMetadata columnMetadata : selectorMetadata.getColumns()
        .getColumns()) {
      int index = columnMetadata.getName().indexOf('.');
      if (index != -1) {
        columnMetadata = new ColumnMetadata(
            columnMetadata.getName().substring(index + 1),
            columnMetadata.getValue());
      }
      columns.add(columnMetadata);
    }
    List<ColumnDefinition> groupColumns = new LinkedList<ColumnDefinition>();
    if (selectorMetadata.getGroupBy() != null) {
      for (GroupMetadata groupMetadata : selectorMetadata.getGroupBy()) {
        int index = groupMetadata.getColumn().indexOf('.');
        if (index != -1) {
          groupMetadata = new GroupMetadata(
              groupMetadata.getColumn().substring(index + 1));
        }
        ColumnDefinition columnDefinition = getColumnDefinition(
            groupMetadata.getColumn(), columns);
        groupColumns.add(columnDefinition);
      }
    }
    RecordSetMetadata recordSetMetadata = new RecordSetMetadata(columns,
        groupColumns);
    return new RecordSetImpl(recordSetMetadata, new Date(), new Date(),
        new LinkedList<Object[]>());
  }

  protected static ColumnDefinition getColumnDefinition(String name,
      List<ColumnDefinition> columns) {
    for (ColumnDefinition columnDefinition : columns) {
      if (name.equals(columnDefinition.getName()))
        return columnDefinition;
    }
    throw new IllegalArgumentException(
        StringFormater.format("Invalid group column name '{}'!", name));
  }

}
