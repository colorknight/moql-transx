package org.datayoo.moql.querier.tcvector;

import com.tencent.tcvectordb.client.VectorDBClient;
import com.tencent.tcvectordb.model.Collection;
import com.tencent.tcvectordb.model.DocField;
import com.tencent.tcvectordb.model.Document;
import com.tencent.tcvectordb.model.param.collection.IndexField;
import com.tencent.tcvectordb.model.param.database.ConnectParam;
import com.tencent.tcvectordb.model.param.dml.QueryParam;
import com.tencent.tcvectordb.model.param.dml.SearchByVectorParam;
import com.tencent.tcvectordb.model.param.enums.ReadConsistencyEnum;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.datayoo.moql.*;
import org.datayoo.moql.core.*;
import org.datayoo.moql.core.factory.MoqlFactoryImpl;
import org.datayoo.moql.metadata.*;
import org.datayoo.moql.operand.OperandFactory;
import org.datayoo.moql.operand.expression.AbstractOperationExpression;
import org.datayoo.moql.operand.expression.ParenExpression;
import org.datayoo.moql.operand.expression.logic.AndExpression;
import org.datayoo.moql.operand.expression.logic.NotExpression;
import org.datayoo.moql.operand.expression.logic.OrExpression;
import org.datayoo.moql.operand.expression.relation.EqualExpression;
import org.datayoo.moql.operand.expression.relation.InExpression;
import org.datayoo.moql.operand.expression.relation.OperandExpression;
import org.datayoo.moql.operand.factory.OperandFactoryImpl;
import org.datayoo.moql.operand.function.Function;
import org.datayoo.moql.querier.DataQuerier;
import org.datayoo.moql.querier.SupplementReader;

import java.io.IOException;
import java.util.*;

import static org.datayoo.moql.parser.MoqlParser.parseMoql;

public class TcVectorQuerier implements DataQuerier {

  public static final String RESERVED_FUNC_WITH_VECTORS = "withVectors";

  public static final String RESERVED_FUNC_WITH_PARAMS = "withParams";
  protected VectorDBClient vectorDBClient;

  protected static MoqlFactoryImpl moqlFactory = new MoqlFactoryImpl();

  protected String documentIdColumn;

  protected String vectorsColumn;


  static {
    ClassLoader classLoader = WithVectors.class.getClassLoader();
    OperandFactory operandFactory = new OperandFactoryImpl();
    operandFactory.registFunction(WithVectors.FUNCTION_NAME, WithVectors.class.getName(),
        classLoader);
    operandFactory.registFunction(WithParams.FUNCTION_NAME,
        WithParams.class.getName(), classLoader);

    moqlFactory.setOperandFactory(operandFactory);
  }

  public TcVectorQuerier() {
  }

  public TcVectorQuerier(VectorDBClient vectorDBClient) {
    this.vectorDBClient = vectorDBClient;
  }

  @Override
  public void connect(String[] strings, Properties properties) {
    if (vectorDBClient != null) {
      return;
    }
    String url = (String) properties.get("url");
    String username = (String) properties.get("username");
    String key = (String) properties.get("key");
    ConnectParam connectParam = ConnectParam.newBuilder().withUrl(url)
        .withUsername(username).withKey(key).withTimeout(30).build();
    vectorDBClient = new VectorDBClient(connectParam,
        ReadConsistencyEnum.EVENTUAL_CONSISTENCY);
  }

  @Override
  public void disconnect() {
    vectorDBClient = null;
  }

  @Override
  public RecordSet query(String sql) throws IOException {
    return query(sql, null, null);
  }

  @Override
  public RecordSet query(String sql, Properties properties) throws IOException {
    return query(sql, properties, null);
  }

  @Override
  public RecordSet query(String sql, SupplementReader supplementReader)
      throws IOException {
    return query(sql, null, supplementReader);
  }

  @Override
  public RecordSet query(String sql, Properties properties,
      SupplementReader supplementReader) throws IOException {
    Validate.notEmpty(sql, "sql is empty!");
    sql = sql.replaceAll("`", StringUtils.EMPTY);
    try {
      if (vectorDBClient == null) {
        throw new MoqlException("TcVectorQuerier is null or has benn close");
      }
      SelectorDefinition selectorDefinition = parseMoql(sql);
      decorateSelectorDefinition(selectorDefinition);
      SelectorMetadata selectorMetadata = (SelectorMetadata) selectorDefinition;
      BuilderProxy builderProxy = createBuilder(selectorDefinition);
      String table = getCollectionName(selectorMetadata.getTables());
      String[] names = StringUtils.split(table, ".");
      if (names.length != 2) {
        throw new MoqlException("Database or Collection is Empty");
      }
      Collection collection = vectorDBClient.database(names[0])
          .describeCollection(names[1]);
      for (IndexField indexField : collection.getIndexes()) {
        if (indexField.isPrimaryKey()) {
          this.documentIdColumn = indexField.getFieldName();
        }
        if (indexField.isVectorField()) {
          this.vectorsColumn = indexField.getFieldName();
        }
      }
      if (builderProxy.isSearchMode()) {
        SearchByVectorParam searchParam = builderProxy.buildSearch();
        List<List<Document>> documents = vectorDBClient.database(names[0])
            .collection(names[1]).search(searchParam);
        return toSearchRecordSet((SelectorMetadata) selectorDefinition,
            documents);
      } else {
        QueryParam queryParam = builderProxy.buildQuery();
        List<Document> documents = vectorDBClient.database(names[0])
            .collection(names[1]).query(queryParam);
        return toQueryRecordSet((SelectorMetadata) selectorDefinition,
            documents);
      }
    } catch (MoqlException e) {
      throw new IOException("Parse failed!", e);
    }
  }

  protected void decorateSelectorDefinition(
      SelectorDefinition selectorDefinition) throws MoqlException {
    if (selectorDefinition instanceof SelectorMetadata) {
      SelectorMetadata selectorMetadata = (SelectorMetadata) selectorDefinition;
      List<ColumnMetadata> columnMetadatas = selectorMetadata.getColumns()
          .getColumns();
      if (columnMetadatas.size() == 1) {
        ColumnMetadata columnMetadata = columnMetadatas.get(0);
        if (columnMetadata.getValue().equals("*")) {
          String table = getCollectionName(selectorMetadata.getTables());
          String[] names = StringUtils.split(table, ".");
          List<IndexField> fields = vectorDBClient.database(names[0])
              .describeCollection(names[1]).getIndexes();
          selectorMetadata.getColumns().setColumns(getCollectionFields(fields));
          if (selectorMetadata.getWhere() == null) {
            throw new MoqlException("");
          }
        }
      }
    } else {
      throw new UnsupportedOperationException(
          "Tencent Cloud VectorDB must have query criteria");
    }
  }

  protected List<ColumnMetadata> getCollectionFields(List<IndexField> fields) {
    List<ColumnMetadata> columnMetadatas = new LinkedList<>();
    for (IndexField field : fields) {
      ColumnMetadata columnMetadata = new ColumnMetadata(field.getFieldName(),
          field.getFieldName());
      columnMetadata.setDataType(field.getFieldType());
      columnMetadatas.add(columnMetadata);
    }
    return columnMetadatas;
  }

  protected String getCollectionName(TablesMetadata tablesMetadata) {
    QueryableMetadata queryableMetadata = tablesMetadata.getTables().get(0);
    if (!(queryableMetadata instanceof TableMetadata)) {
      throw new UnsupportedOperationException(
          "Unsupport multi tables operation!");
    }
    return ((TableMetadata) queryableMetadata).getValue();
  }

  protected RecordSet toSearchRecordSet(SelectorMetadata selectorMetadata,
      List<List<Document>> documents) {
    List<ColumnMetadata> columnMetadatas = selectorMetadata.getColumns()
        .getColumns();
    RecordSetMetadata recordSetMetadata = new RecordSetMetadata(
        (List) columnMetadatas, null);

    List<Object[]> records = new ArrayList<>(documents.size());
    if (!documents.isEmpty()) {
      for (Document document : documents.get(0)) {
        List<DocField> docFields = document.getDocFields();
        List<ColumnDefinition> list = recordSetMetadata.getColumns();
        Object[] record = new Object[columnMetadatas.size()];
        int i = 0;
        for (ColumnDefinition column : list) {
          if (column.getName().equals(this.documentIdColumn)) {
            record[i] = document.getId();
          } else if (column.getName().equals(this.vectorsColumn)) {
            record[i] = document.getVector();
          } else {
            for (DocField docField : docFields) {
              if (column.getName().equals(docField.getName())) {
                record[i] = docField.getValue();
              }
            }
          }
          i++;
        }
        records.add(record);
      }
    }
    return new RecordSetImpl(recordSetMetadata, new Date(), new Date(),
        records);
  }

  protected RecordSet toQueryRecordSet(SelectorMetadata selectorMetadata,
      List<Document> documents) {
    List<ColumnMetadata> columnMetadatas = selectorMetadata.getColumns()
        .getColumns();
    RecordSetMetadata recordSetMetadata = new RecordSetMetadata(
        (List) columnMetadatas, null);

    List<Object[]> records = new ArrayList<>(documents.size());
    for (Document document : documents) {
      List<DocField> docFields = document.getDocFields();
      List<ColumnDefinition> list = recordSetMetadata.getColumns();
      Object[] record = new Object[columnMetadatas.size()];
      int i = 0;
      for (ColumnDefinition column : list) {
        if (column.getName().equals(this.documentIdColumn)) {
          record[i] = document.getId();
        } else if (column.getName().equals(this.vectorsColumn)) {
          record[i] = document.getVector();
        } else {
          for (DocField docField : docFields) {
            if (column.getName().equals(docField.getName())) {
              record[i] = docField.getValue();
            }
          }
        }
        i++;
      }
      records.add(record);
    }
    return new RecordSetImpl(recordSetMetadata, new Date(), new Date(),
        records);
  }

  protected BuilderProxy createBuilder(SelectorDefinition selectorDefinition)
      throws MoqlException {
    Selector selector = moqlFactory.createSelector(selectorDefinition);
    if (selector instanceof SetlectorImpl)
      throw new UnsupportedOperationException("Unsupport 'set' operation!");
    SelectorImpl selectorImpl = (SelectorImpl) selector;
    if (selectorImpl.getRecordSetOperator() instanceof Group)
      throw new UnsupportedOperationException(
          "Unsupport groupBy clause operation!");
    if (selectorImpl.getHaving() != null)
      throw new UnsupportedOperationException(
          "Unsupport having clause operation!");
    if (selectorImpl.getOrder() != null)
      throw new UnsupportedOperationException(
          "Unsupport order clause operation!");

    BuilderProxy builder = new BuilderProxy();
    buildSelectClause(builder, selectorImpl.getRecordSetOperator());
    Map<String, Object> paramMap = new HashMap<>();
    if (selectorImpl.getWhere() != null) {
      buildWhereClause(builder, selectorImpl.getWhere());
    }
    if (selectorImpl.getLimit() != null) {
      buildLimitClause(builder, selectorImpl.getLimit());
    }
    return builder;
  }

  protected void buildSelectClause(BuilderProxy builder,
      RecordSetOperator recordSetOperator) {
    List<String> outputFields = new LinkedList<>();
    for (Column column : recordSetOperator.getColumns().getColumns()) {
      ColumnMetadata columnMetadata = column.getColumnMetadata();
      if (columnMetadata.getNestedSelector() != null) {
        throw new UnsupportedOperationException(
            "Unsupport nested select clause!");
      } else if (columnMetadata.getCaseMetadata() != null) {
        throw new UnsupportedOperationException("Unsupport case clause!");
      }
      outputFields.add(columnMetadata.getValue());
    }
    builder.withOutFields(outputFields);
  }

  protected void buildWhereClause(BuilderProxy builder, Condition condition)
      throws MoqlException {
    StringBuilder stringBuilder = new StringBuilder();
    buildOperand(builder, condition.getOperand(), stringBuilder);
    if (stringBuilder.length() > 0) {
      builder.withFilter(stringBuilder.toString());
    }
  }

  protected void buildOperand(BuilderProxy builder, Operand operand,
      StringBuilder stringBuilder) throws MoqlException {

    if (operand instanceof ParenExpression) {
      ParenExpression parenExpression = (ParenExpression) operand;
      buildOperand(builder, parenExpression.getOperand(), stringBuilder);
    } else if (operand instanceof OperandExpression) {
      OperandExpression operandExpression = (OperandExpression) operand;
      buildOperand(builder, operandExpression.getRightOperand(), stringBuilder);
    } else if (operand instanceof NotExpression
        || operand instanceof OrExpression
        || operand instanceof AndExpression) {
      AbstractOperationExpression logicExpression = (AbstractOperationExpression) operand;
      StringBuilder temp1 = new StringBuilder();
      buildOperand(builder, logicExpression.getLeftOperand(), temp1);
      if (temp1.length() > 0) {
        stringBuilder.append(temp1);
      }
      StringBuilder temp2 = new StringBuilder();
      buildOperand(builder, logicExpression.getRightOperand(), temp2);
      if (temp2.length() > 0) {
        if (temp1.length() > 0) {
          stringBuilder.append(' ');
          stringBuilder.append(logicExpression.getOperator());
          stringBuilder.append(' ');
        }
        stringBuilder.append(temp2);
      }
    } else if (operand instanceof InExpression) {
      InExpression inExpression = (InExpression) operand;
      String v = inExpression.getRightOperand().toString();
      if (inExpression.getLeftOperand().toString()
          .equals(this.documentIdColumn)) {
        List<String> documents = Arrays.asList(StringUtils.split(v, ","));
        builder.withQueryId(documents);
      } else {
        stringBuilder.append(inExpression.getLeftOperand().toString());
        stringBuilder.append(" in ");
        StringBuilder temp = new StringBuilder();
        temp.append('[');
        temp.append(v.substring(1, v.length() - 1));
        temp.append(']');
        stringBuilder.append(temp);
      }
    } else if (operand instanceof Function) {
      buildFunction(builder, (Function) operand);
    } else {
      if (operand instanceof EqualExpression) {
        EqualExpression expression = (EqualExpression) operand;
        if (expression.getLeftOperand().toString()
            .equals(this.documentIdColumn)) {
          String v = expression.getRightOperand().toString();
          List<String> documentsId = Collections.singletonList(v);
          builder.withQueryId(documentsId);
        }
      } else {
        stringBuilder.append(operand);
      }
    }
  }

  protected void buildFunction(BuilderProxy builder, Function function)
      throws MoqlException {
    if (function.getName().equals(RESERVED_FUNC_WITH_VECTORS)) {
      WithVectors withVectors = (WithVectors) function;
      builder.withSearchVectors(withVectors.getVectorArray());
    } else if (function.getName().equals(RESERVED_FUNC_WITH_PARAMS)) {
      WithParams withParams = (WithParams) function;
      builder.withParams(withParams.getParams());
    }
  }

  protected void buildLimitClause(BuilderProxy builder, Limit limit) {
    LimitMetadata limitMetadata = limit.getLimitMetadata();
    builder.withLimit(limitMetadata.getValue());
    if (limitMetadata.getOffset() != 0) {
      builder.withOffset(limitMetadata.getOffset());
    }
  }

}
