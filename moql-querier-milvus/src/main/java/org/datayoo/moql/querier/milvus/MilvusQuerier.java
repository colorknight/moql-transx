package org.datayoo.moql.querier.milvus;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.*;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.response.FieldDataWrapper;
import org.apache.commons.lang3.Validate;
import org.datayoo.moql.*;
import org.datayoo.moql.core.*;
import org.datayoo.moql.core.factory.MoqlFactoryImpl;
import org.datayoo.moql.core.table.CommonTable;
import org.datayoo.moql.metadata.*;
import org.datayoo.moql.operand.OperandFactory;
import org.datayoo.moql.operand.expression.AbstractOperationExpression;
import org.datayoo.moql.operand.expression.ParenExpression;
import org.datayoo.moql.operand.expression.logic.AndExpression;
import org.datayoo.moql.operand.expression.logic.NotExpression;
import org.datayoo.moql.operand.expression.logic.OrExpression;
import org.datayoo.moql.operand.expression.relation.InExpression;
import org.datayoo.moql.operand.expression.relation.OperandExpression;
import org.datayoo.moql.operand.factory.OperandFactoryImpl;
import org.datayoo.moql.operand.function.Function;
import org.datayoo.moql.querier.DataQuerier;
import org.datayoo.moql.querier.SupplementReader;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.*;

import static org.datayoo.moql.parser.MoqlParser.parseMoql;

/**
 * @author tangtadin
 * @version 1.0
 * @description: TODO
 * @date 2023/2/23 12:09
 */
public class MilvusQuerier implements DataQuerier {

  public static final String RESERVED_FUNC_VMATCH = "vMatch";
  public static final String RESERVED_FUNC_PARTITIONBY = "partitionBy";

  public static final String RESERVED_FUNC_CONSISTENCYLEVEL = "consistencyLevel";
  public static final String RESERVED_FUNC_GRACEFUL_TIME = "gracefulTime";

  public static final String RESERVED_FUNC_GUARANTEE_TIMESTAMP = "guaranteeTimestamp";

  public static final String RESERVED_FUNC_ROUND_DECIMAL = "roundDecimal";

  public static final String RESERVED_FUNC_TRAVEL_TIMESTAMP = "travelTimestamp";
  public static final String RESERVED_FUNC_NPROBE = "nProbe";
  public static final String RESERVED_FUNC_EF = "ef";
  public static final String RESERVED_FUNC_SEARCHK = "searchK";

  protected static MoqlFactoryImpl moqlFactory = new MoqlFactoryImpl();

  static {
    ClassLoader classLoader = MilvusQuerier.class.getClassLoader();
    OperandFactory operandFactory = new OperandFactoryImpl();
    operandFactory.registFunction(VMatch.FUNCTION_NAME, VMatch.class.getName(),
        classLoader);
    operandFactory.registFunction(PartitionBy.FUNCTION_NAME,
        PartitionBy.class.getName(), classLoader);
    operandFactory.registFunction(ConsistencyLevel.FUNCTION_NAME,
        ConsistencyLevel.class.getName(), classLoader);
    operandFactory.registFunction(GracefulTime.FUNCTION_NAME,
        GracefulTime.class.getName(), classLoader);
    operandFactory.registFunction(GuaranteeTimestamp.FUNCTION_NAME,
        GuaranteeTimestamp.class.getName(), classLoader);
    operandFactory.registFunction(TravelTimestamp.FUNCTION_NAME,
        TravelTimestamp.class.getName(), classLoader);
    operandFactory.registFunction(RoundDecimal.FUNCTION_NAME,
        RoundDecimal.class.getName(), classLoader);
    operandFactory.registFunction(NProbe.FUNCTION_NAME, NProbe.class.getName(),
        classLoader);
    operandFactory.registFunction(Ef.FUNCTION_NAME, Ef.class.getName(),
        classLoader);
    operandFactory.registFunction(SearchK.FUNCTION_NAME,
        SearchK.class.getName(), classLoader);
    moqlFactory.setOperandFactory(operandFactory);
  }

  protected MilvusClient milvusClient;

  public MilvusQuerier() {
  }

  public MilvusQuerier(MilvusClient milvusClient) {
    Validate.notNull(milvusClient, "milvusClient is null!");
    this.milvusClient = milvusClient;
  }

  @Override
  public void connect(String[] serverIps, Properties properties)
      throws IOException {
    Validate.notEmpty(serverIps, "serverIps is null!");
    ConnectParam.Builder builder = ConnectionBuilderHelper.createConnectionBuilder(
        serverIps[0], properties);
    milvusClient = new MilvusServiceClient(builder.build());
  }

  @Override
  public void disconnect() throws IOException {
    if (milvusClient != null) {
      try {
        milvusClient.close();
      } finally {
        milvusClient = null;
      }
    }
  }

  @Override
  public RecordSet query(String sql) throws IOException {
    return query(sql, null, null);
  }

  @Override
  public RecordSet query(String sql, Properties queryProps) throws IOException {
    return query(sql, queryProps, null);
  }

  @Override
  public RecordSet query(String sql, SupplementReader supplementReader)
      throws IOException {
    return query(sql, null, supplementReader);
  }

  @Override
  public RecordSet query(String sql, Properties queryProps,
      SupplementReader supplementReader) throws IOException {
    Validate.notEmpty(sql, "sql is empty!");
    try {
      SelectorDefinition selectorDefinition = parseMoql(sql);
      decorateSelectorDefinition(selectorDefinition);
      BuilderProxy builderProxy = createBuilder(selectorDefinition);
      if (builderProxy.isSearchMode()) {
        SearchParam searchParam = (SearchParam) builderProxy.build();
        R<SearchResults> result = milvusClient.search(searchParam);
        if (result.getStatus() == ErrorCode.Success_VALUE)
          return toSearchRecordSet((SelectorMetadata) selectorDefinition,
              result);
        else {
          throw new IOException(
              String.format("Search failed! Reason: %s !", result.toString()));
        }
      } else {
        QueryParam queryParam = (QueryParam) builderProxy.build();
        R<QueryResults> result = milvusClient.query(queryParam);
        if (result.getStatus() == ErrorCode.Success_VALUE)
          return toQueryRecordSet((SelectorMetadata) selectorDefinition,
              result);
        else {
          throw new IOException(
              String.format("Query failed! Reason: %s !", result.toString()));
        }
      }
    } catch (MoqlException e) {
      throw new IOException("Parse failed!", e);
    }
  }

  protected void decorateSelectorDefinition(
      SelectorDefinition selectorDefinition) {
    if (selectorDefinition instanceof SelectorMetadata) {
      SelectorMetadata selectorMetadata = (SelectorMetadata) selectorDefinition;
      List<ColumnMetadata> columnMetadatas = selectorMetadata.getColumns()
          .getColumns();
      if (columnMetadatas.size() == 1) {
        ColumnMetadata columnMetadata = columnMetadatas.get(0);
        if (columnMetadata.getValue().equals("*")) {
          String collectionName = getCollectionName(
              selectorMetadata.getTables());
          List<FieldSchema> fieldSchemas = getFieldSchemas(collectionName);
          selectorMetadata.getColumns()
              .setColumns(getCollectionFields(fieldSchemas));
          if (selectorMetadata.getWhere() == null) {
            selectorMetadata.setWhere(
                buildDefaultConditionMetadata(fieldSchemas));
          }
        }
      }
    } else {
      throw new UnsupportedOperationException("");
    }
  }

  protected String getCollectionName(TablesMetadata tablesMetadata) {
    QueryableMetadata queryableMetadata = tablesMetadata.getTables().get(0);
    if (!(queryableMetadata instanceof TableMetadata)) {
      throw new UnsupportedOperationException(
          "Unsupport multi tables operation!");
    }
    return ((TableMetadata) queryableMetadata).getName();
  }

  protected List<FieldSchema> getFieldSchemas(String collection) {
    R<DescribeCollectionResponse> respDescribeCollection = milvusClient.describeCollection(
        DescribeCollectionParam.newBuilder().withCollectionName(collection)
            .build());
    return respDescribeCollection.getData().getSchema().getFieldsList();
  }

  protected List<ColumnMetadata> getCollectionFields(
      List<FieldSchema> fieldSchemas) {
    List<ColumnMetadata> columnMetadatas = new LinkedList<>();
    for (FieldSchema fieldSchema : fieldSchemas) {
      ColumnMetadata columnMetadata = new ColumnMetadata(fieldSchema.getName(),
          fieldSchema.getName());
      columnMetadata.setDataType(fieldSchema.getDataType());
      columnMetadatas.add(columnMetadata);
    }
    return columnMetadatas;
  }

  protected ConditionMetadata buildDefaultConditionMetadata(
      List<FieldSchema> fieldSchemas) {
    for (FieldSchema fieldSchema : fieldSchemas) {
      DataType dataType = fieldSchema.getDataType();
      if (dataType == DataType.Float || dataType == DataType.Double
          || dataType == DataType.Int8 || dataType == DataType.Int16
          || dataType == DataType.Int32 || dataType == DataType.Int64) {
        OperationMetadata operationMetadata = new RelationOperationMetadata(">",
            fieldSchema.getName(), "0");
        return new ConditionMetadata(operationMetadata);
      } else if (dataType == DataType.Bool) {
        OperationMetadata operationMetadata = new RelationOperationMetadata(
            "==", fieldSchema.getName(), "true");
        return new ConditionMetadata(operationMetadata);
      } else if (dataType == DataType.String) {
        OperationMetadata operationMetadata = new RelationOperationMetadata(
            "!=", fieldSchema.getName(), " ");
        return new ConditionMetadata(operationMetadata);
      }
    }
    return null;
  }

  public SearchParam buildSearchParam(String sql) throws IOException {
    try {
      SelectorDefinition selectorDefinition = parseMoql(sql);
      decorateSelectorDefinition(selectorDefinition);
      BuilderProxy builderProxy = createBuilder(selectorDefinition);
      return (SearchParam) builderProxy.build();
    } catch (MoqlException e) {
      throw new IOException("Parse failed!", e);
    }
  }

  public QueryParam buildQueryParam(String sql) throws IOException {
    try {
      SelectorDefinition selectorDefinition = parseMoql(sql);
      decorateSelectorDefinition(selectorDefinition);
      BuilderProxy builderProxy = createBuilder(selectorDefinition);
      return (QueryParam) builderProxy.build();
    } catch (MoqlException e) {
      throw new IOException("Parse failed!", e);
    }
  }

  protected RecordSet toSearchRecordSet(SelectorMetadata selectorMetadata,
      R<SearchResults> result) {
    if (result.getData() == null)
      throw new MoqlRuntimeException(result.getException());
    SearchResultData searchResultData = result.getData().getResults();
    List<ColumnMetadata> columnMetadatas = selectorMetadata.getColumns()
        .getColumns();
    //    setOutputDataType(columnMetadatas, searchResultData.getFieldsDataList());
    int[] posMappings = posMappings(columnMetadatas,
        searchResultData.getFieldsDataList());
    RecordSetMetadata recordSetMetadata = new RecordSetMetadata(
        getOutputColumns(columnMetadatas, getIdType(searchResultData)), null);
    return new RecordSetImpl(recordSetMetadata, new Date(), new Date(),
        toRecords(searchResultData, posMappings));
  }

  //  protected void setOutputDataType(List<ColumnMetadata> columnMetadatas,
  //      List<FieldData> fieldDatas) {
  //    Iterator<ColumnMetadata> colIt = columnMetadatas.iterator();
  //    Iterator<FieldData> fdIt = fieldDatas.iterator();
  //    while (colIt.hasNext()) {
  //      ColumnMetadata columnMetadata = colIt.next();
  //      FieldData fieldData = fdIt.next();
  //      columnMetadata.setDataType(fieldData.getType());
  //    }
  //  }

  protected DataType getIdType(SearchResultData searchResultData) {
    IDs iDs = searchResultData.getIds();
    if (iDs.hasIntId())
      return DataType.Int64;
    else
      return DataType.String;
  }

  protected RecordSet toQueryRecordSet(SelectorMetadata selectorMetadata,
      R<QueryResults> result) {
    if (result.getData() == null)
      throw new MoqlRuntimeException(result.getException());
    QueryResults queryResults = result.getData();
    List<ColumnMetadata> columnMetadatas = selectorMetadata.getColumns()
        .getColumns();
    //    setOutputDataType(columnMetadatas, queryResults.getFieldsDataList());
    int[] posMappings = posMappings(columnMetadatas,
        result.getData().getFieldsDataList());
    RecordSetMetadata recordSetMetadata = new RecordSetMetadata(
        (List) columnMetadatas, null);
    return new RecordSetImpl(recordSetMetadata, new Date(), new Date(),
        toRecords(queryResults, posMappings));
  }

  protected int[] posMappings(List<ColumnMetadata> columnMetadatas,
      List<FieldData> fieldDatas) {
    int[] posMappings = new int[columnMetadatas.size()];
    int i = 0;
    for (FieldData fieldData : fieldDatas) {
      posMappings[i++] = findPos(columnMetadatas, fieldData.getFieldName());
    }
    return posMappings;
  }

  protected int findPos(List<ColumnMetadata> columnMetadatas, String name) {
    int i = 0;
    for (ColumnMetadata columnMetadata : columnMetadatas) {
      if (columnMetadata.getName().equals(name))
        return i;
      i++;
    }
    throw new IllegalArgumentException(
        String.format("There is no column named '%s'!", name));
  }

  protected List getOutputColumns(List<ColumnMetadata> columnMetadatas,
      DataType idType) {
    ColumnMetadata columnMetadata = new ColumnMetadata("id", "id");
    columnMetadatas.add(0, columnMetadata);
    columnMetadata.setDataType(idType);
    columnMetadata = new ColumnMetadata("idScore", "idScore");
    columnMetadata.setDataType(DataType.Float);
    columnMetadatas.add(1, columnMetadata);
    return columnMetadatas;
  }

  protected List<Object[]> toRecords(SearchResultData resultData,
      int[] posMappings) {
    List<Object[]> records = new LinkedList<>();
    int fieldCount = resultData.getFieldsDataCount() + 2;
    Iterator idIt = getIdIterator(resultData.getIds());
    Iterator idScoreIt = resultData.getScoresList().iterator();
    List<Iterator> fieldIterators = getFieldIterators(
        resultData.getFieldsDataList());
    while (idIt.hasNext()) {
      Object[] record = new Object[fieldCount];
      record[0] = idIt.next();
      record[1] = idScoreIt.next();
      int i = 0;
      for (Iterator it : fieldIterators) {
        record[posMappings[i++] + 2] = it.next();
      }
      records.add(record);
    }
    return records;
  }

  protected List<Object[]> toRecords(QueryResults queryResults,
      int[] posMappings) {
    List<Object[]> records = new LinkedList<>();
    List<Iterator> fieldIterators = getFieldIterators(
        queryResults.getFieldsDataList());
    while (true) {
      Object[] record = new Object[fieldIterators.size()];
      int i = 0;
      int hasValue = 0;
      for (Iterator it : fieldIterators) {
        if (!it.hasNext()) {
          i++;
          continue;
        }
        record[posMappings[i++]] = it.next();
        hasValue++;
      }
      if (hasValue == 0)
        break;
      records.add(record);
    }
    return records;
  }

  protected Iterator getIdIterator(IDs ids) {
    if (ids.hasIntId()) {
      LongArray longArray = ids.getIntId();
      return longArray.getDataList().iterator();
    } else {
      StringArray stringArray = ids.getStrId();
      return stringArray.getDataList().iterator();
    }
  }

  protected List<Iterator> getFieldIterators(List<FieldData> fieldDatas) {
    List<Iterator> fieldIterators = new LinkedList<>();
    for (FieldData fieldData : fieldDatas) {
      FieldDataWrapper dataWrapper = new FieldDataWrapper(fieldData);
      fieldIterators.add(dataWrapper.getFieldData().iterator());
    }
    return fieldIterators;
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
    buildFromClause(builder, selectorImpl.getTables());
    buildSelectClause(builder, selectorImpl.getRecordSetOperator());
    Map<String, Object> paramMap = new HashMap<>();
    if (selectorImpl.getWhere() != null) {
      buildWhereClause(builder, selectorImpl.getWhere());
    }
    if (selectorImpl.getLimit() != null) {
      buildLimitClause(builder, selectorImpl.getLimit());
    } else {
      builder.withTopK(10);
      builder.withOffset(0);
    }
    if (paramMap.size() > 0) { // 暂时无用
      Gson gson = new GsonBuilder().create();
      builder.withParams(gson.toJson(paramMap));
    }
    return builder;
  }

  protected void buildFromClause(BuilderProxy builder, Tables tables) {
    if (!(tables.getQueryable() instanceof CommonTable)) {
      throw new UnsupportedOperationException(
          "Unsupport multi tables operation!");
    }
    CommonTable commonTable = (CommonTable) tables.getQueryable();
    builder.withCollectionName(commonTable.getTableMetadata().getName());
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
      builder.withExpr(stringBuilder.toString());
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
      stringBuilder.append(inExpression.getLeftOperand().toString());
      stringBuilder.append(" in ");
      String v = inExpression.getRightOperand().toString();
      StringBuilder temp = new StringBuilder();
      temp.append('[');
      temp.append(v.substring(1, v.length() - 1));
      temp.append(']');
      stringBuilder.append(temp);
    } else if (operand instanceof Function) {
      buildFunction(builder, (Function) operand, stringBuilder);
    } else {
      stringBuilder.append(operand.toString());
    }
  }

  protected void buildFunction(BuilderProxy builder, Function function,
      StringBuilder stringBuilder) {
    if (function.getName().equals(RESERVED_FUNC_PARTITIONBY)) {
      PartitionBy partitionBy = (PartitionBy) function;
      builder.withPartitionNames(partitionBy.getPartitions());
    } else if (function.getName().equals(RESERVED_FUNC_VMATCH)) {
      VMatch vMatch = (VMatch) function;
      builder.withVectorFieldName(vMatch.getVectorName());
      builder.withVectors(vMatch.getVectorArray());
      builder.withMetricType(vMatch.getMetricType());
    } else if (function.getName().equals(RESERVED_FUNC_CONSISTENCYLEVEL)) {
      ConsistencyLevel consistencyLevel = (ConsistencyLevel) function;
      builder.withConsistencyLevel(consistencyLevel.getConsistencyLevel());
    } else if (function.getName().equals(RESERVED_FUNC_GRACEFUL_TIME)) {
      GracefulTime gracefulTime = (GracefulTime) function;
      builder.withGracefulTime(gracefulTime.getGracefulTime());
    } else if (function.getName().equals(RESERVED_FUNC_GUARANTEE_TIMESTAMP)) {
      GuaranteeTimestamp guaranteeTimestamp = (GuaranteeTimestamp) function;
      builder.withGuaranteeTimestamp(
          guaranteeTimestamp.getGuaranteeTimestamp());
    } else if (function.getName().equals(RESERVED_FUNC_ROUND_DECIMAL)) {
      RoundDecimal roundDecimal = (RoundDecimal) function;
      builder.withRoundDecimal(roundDecimal.getRoundDecimal());
    } else if (function.getName().equals(RESERVED_FUNC_TRAVEL_TIMESTAMP)) {
      TravelTimestamp travelTimestamp = (TravelTimestamp) function;
      builder.withTravelTimestamp(travelTimestamp.getTravelTimestamp());
    } else if (function.getName().equals(RESERVED_FUNC_NPROBE)) {
      NProbe nProbe = (NProbe) function;
      builder.withNProbe(nProbe.getnProbe());
    } else if (function.getName().equals(RESERVED_FUNC_EF)) {
      Ef ef = (Ef) function;
      builder.withEf(ef.getEf());
    } else if (function.getName().equals(RESERVED_FUNC_SEARCHK)) {
      SearchK searchK = (SearchK) function;
      builder.withSearchK(searchK.getSearchK());
    }
  }

  protected void buildLimitClause(BuilderProxy builder, Limit limit) {
    LimitMetadata limitMetadata = limit.getLimitMetadata();
    builder.withTopK(limitMetadata.getValue());
    if (limitMetadata.getOffset() != 0)
      builder.withOffset(limitMetadata.getOffset());
  }
}
