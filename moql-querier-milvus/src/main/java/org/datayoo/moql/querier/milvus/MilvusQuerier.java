package org.datayoo.moql.querier.milvus;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.*;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.dml.SearchParam;
import io.milvus.response.FieldDataWrapper;
import org.apache.commons.lang3.Validate;
import org.datayoo.moql.*;
import org.datayoo.moql.core.*;
import org.datayoo.moql.core.factory.MoqlFactoryImpl;
import org.datayoo.moql.core.table.CommonTable;
import org.datayoo.moql.core.table.SelectorTable;
import org.datayoo.moql.metadata.ColumnMetadata;
import org.datayoo.moql.metadata.LimitMetadata;
import org.datayoo.moql.metadata.SelectorMetadata;
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
    OperandFactory operandFactory = new OperandFactoryImpl();
    operandFactory.registFunction(VMatch.FUNCTION_NAME, VMatch.class.getName());
    operandFactory.registFunction(PartitionBy.FUNCTION_NAME,
        PartitionBy.class.getName());
    operandFactory.registFunction(ConsistencyLevel.FUNCTION_NAME,
        ConsistencyLevel.class.getName());
    operandFactory.registFunction(GracefulTime.FUNCTION_NAME,
        GracefulTime.class.getName());
    operandFactory.registFunction(GuaranteeTimestamp.FUNCTION_NAME,
        GuaranteeTimestamp.class.getName());
    operandFactory.registFunction(TravelTimestamp.FUNCTION_NAME,
        TravelTimestamp.class.getName());
    operandFactory.registFunction(RoundDecimal.FUNCTION_NAME,
        RoundDecimal.class.getName());
    operandFactory.registFunction(NProbe.FUNCTION_NAME, NProbe.class.getName());
    operandFactory.registFunction(Ef.FUNCTION_NAME, Ef.class.getName());
    operandFactory.registFunction(SearchK.FUNCTION_NAME,
        SearchK.class.getName());
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
      SearchParam searchParam = buildSearchParam(selectorDefinition);
      R<SearchResults> result = milvusClient.search(searchParam);
      return toRecordSet((SelectorMetadata) selectorDefinition, result);
    } catch (MoqlException e) {
      throw new IOException("Parse failed!", e);
    }
  }

  public SearchParam buildSearchParam(String sql) throws IOException {
    try {
      SelectorDefinition selectorDefinition = parseMoql(sql);
      return buildSearchParam(selectorDefinition);
    } catch (MoqlException e) {
      throw new IOException("Parse failed!", e);
    }
  }

  protected RecordSet toRecordSet(SelectorMetadata selectorMetadata,
      R<SearchResults> result) {
    RecordSetMetadata recordSetMetadata = new RecordSetMetadata(
        getOutputColumns(selectorMetadata.getColumns().getColumns()), null);
    return new RecordSetImpl(recordSetMetadata, new Date(), new Date(),
        toRecords(result.getData().getResults()));
  }

  protected List getOutputColumns(List<ColumnMetadata> columnMetadatas) {
    ColumnMetadata columnMetadata = new ColumnMetadata("id", "id");
    columnMetadatas.add(0, columnMetadata);
    columnMetadata = new ColumnMetadata("idScore", "idScore");
    columnMetadatas.add(1, columnMetadata);
    return columnMetadatas;
  }

  protected List<Object[]> toRecords(SearchResultData resultData) {
    List<Object[]> records = new LinkedList<>();
    int fieldCount = resultData.getFieldsDataCount() + 2;
    Iterator idIt = getIdIterator(resultData.getIds());
    Iterator idScoreIt = resultData.getScoresList().iterator();
    List<Iterator> fieldIterators = getFieldIterators(resultData);
    while (idIt.hasNext()) {
      Object[] record = new Object[fieldCount];
      record[0] = idIt.next();
      record[1] = idScoreIt.next();
      int i = 2;
      for (Iterator it : fieldIterators) {
        record[i++] = it.next();
      }
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

  protected List<Iterator> getFieldIterators(SearchResultData resultData) {
    List<FieldData> fieldDatas = resultData.getFieldsDataList();
    List<Iterator> fieldIterators = new LinkedList<>();
    for (FieldData fieldData : fieldDatas) {
      FieldDataWrapper dataWrapper = new FieldDataWrapper(fieldData);
      fieldIterators.add(dataWrapper.getFieldData().iterator());
    }
    return fieldIterators;
  }

  protected SearchParam buildSearchParam(SelectorDefinition selectorDefinition)
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
    SearchParam.Builder builder = SearchParam.newBuilder();
    buildFromClause(builder, selectorImpl.getTables());
    buildSelectClause(builder, selectorImpl.getRecordSetOperator());
    Map<String, Object> paramMap = new HashMap<>();
    if (selectorImpl.getWhere() != null) {
      buildWhereClause(builder, selectorImpl.getWhere(), paramMap);
    }
    if (selectorImpl.getLimit() != null) {
      buildLimitClause(builder, selectorImpl.getLimit(), paramMap);
    }
    if (paramMap.size() > 0) {
      Gson gson = new GsonBuilder().create();
      builder.withParams(gson.toJson(paramMap));
    }
    return builder.build();
  }

  protected void buildFromClause(SearchParam.Builder builder, Tables tables) {
    if (!(tables.getQueryable() instanceof CommonTable)) {
      throw new UnsupportedOperationException(
          "Unsupport multi tables operation!");
    }
    CommonTable commonTable = (CommonTable) tables.getQueryable();
    builder.withCollectionName(commonTable.getTableMetadata().getName());
  }

  protected void buildSelectClause(SearchParam.Builder builder,
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

  protected void buildWhereClause(SearchParam.Builder builder,
      Condition condition, Map<String, Object> paramMap) throws MoqlException {
    StringBuilder stringBuilder = new StringBuilder();
    buildOperand(builder, condition.getOperand(), stringBuilder, paramMap);
    if (stringBuilder.length() > 0) {
      builder.withExpr(stringBuilder.toString());
    }
  }

  protected void buildOperand(SearchParam.Builder builder, Operand operand,
      StringBuilder stringBuilder, Map<String, Object> paramMap)
      throws MoqlException {
    if (operand instanceof ParenExpression) {
      ParenExpression parenExpression = (ParenExpression) operand;
      buildOperand(builder, parenExpression.getOperand(), stringBuilder,
          paramMap);
    } else if (operand instanceof OperandExpression) {
      OperandExpression operandExpression = (OperandExpression) operand;
      buildOperand(builder, operandExpression.getRightOperand(), stringBuilder,
          paramMap);
    } else if (operand instanceof NotExpression
        || operand instanceof OrExpression
        || operand instanceof AndExpression) {
      AbstractOperationExpression logicExpression = (AbstractOperationExpression) operand;
      StringBuilder temp1 = new StringBuilder();
      buildOperand(builder, logicExpression.getLeftOperand(), temp1, paramMap);
      if (temp1.length() > 0) {
        stringBuilder.append(temp1);
      }
      StringBuilder temp2 = new StringBuilder();
      buildOperand(builder, logicExpression.getRightOperand(), temp2, paramMap);
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
      buildFunction(builder, (Function) operand, stringBuilder, paramMap);
    } else {
      stringBuilder.append(operand.toString());
    }
  }

  protected void buildFunction(SearchParam.Builder builder, Function function,
      StringBuilder stringBuilder, Map<String, Object> paramMap) {
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
      paramMap.put("nprobe", nProbe.getnProbe());
    } else if (function.getName().equals(RESERVED_FUNC_EF)) {
      Ef ef = (Ef) function;
      paramMap.put("ef", ef.getEf());
    } else if (function.getName().equals(RESERVED_FUNC_SEARCHK)) {
      SearchK searchK = (SearchK) function;
      paramMap.put("search_k", searchK.getSearchK());
    }
  }

  protected void buildLimitClause(SearchParam.Builder builder, Limit limit,
      Map<String, Object> paramMap) {
    LimitMetadata limitMetadata = limit.getLimitMetadata();
    builder.withTopK(limitMetadata.getValue());
    if (limitMetadata.getOffset() != 0)
      paramMap.put("offset", limitMetadata.getOffset());
  }
}
