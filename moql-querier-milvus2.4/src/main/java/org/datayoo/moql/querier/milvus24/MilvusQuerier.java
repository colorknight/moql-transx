package org.datayoo.moql.querier.milvus24;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.HybridSearchReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.datayoo.moql.*;
import org.datayoo.moql.core.*;
import org.datayoo.moql.core.factory.MoqlFactoryImpl;
import org.datayoo.moql.core.join.InnerJoin;
import org.datayoo.moql.core.table.CommonTable;
import org.datayoo.moql.core.table.SelectorTable;
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

  public static final String RESERVED_FUNC_DROP_RATIO_SEARCH = "dropRatioSearch";
  public static final String RESERVED_FUNC_SEARCH_RANGE = "searchRange";
  public static final String RESERVED_FUNC_RRF_RANKER = "rrfRanker";
  public static final String RESERVED_FUNC_WEIGHTED_RANKER = "weightedRanker";
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
    operandFactory.registFunction(DropRatioSearch.FUNCTION_NAME,
        DropRatioSearch.class.getName(), classLoader);
    operandFactory.registFunction(RRFRanker.FUNCTION_NAME,
        RRFRanker.class.getName(), classLoader);
    operandFactory.registFunction(SearchRange.FUNCTION_NAME,
        SearchRange.class.getName(), classLoader);
    operandFactory.registFunction(WeightedRanker.FUNCTION_NAME,
        WeightedRanker.class.getName(), classLoader);
    operandFactory.registFunction(GracefulTime.FUNCTION_NAME,
        GracefulTime.class.getName(), classLoader);
    operandFactory.registFunction(GuaranteeTimestamp.FUNCTION_NAME,
        GuaranteeTimestamp.class.getName(), classLoader);
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

  protected MilvusClientV2 milvusClient;

  public MilvusQuerier() {
  }

  public MilvusQuerier(MilvusClientV2 milvusClient) {
    Validate.notNull(milvusClient, "milvusClient is null!");
    this.milvusClient = milvusClient;
  }

  @Override
  public void connect(String[] serverIps, Properties properties)
      throws IOException {
    Validate.notEmpty(serverIps, "serverIps is null!");
    ConnectConfig.ConnectConfigBuilder builder = ConnectionBuilderHelper.createConnectionBuilder(
        serverIps[0], properties);
    milvusClient = new MilvusClientV2(builder.build());
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
    sql = sql.replaceAll("`", StringUtils.EMPTY);
    try {
      SelectorDefinition selectorDefinition = parseMoql(sql);
      decorateSelectorDefinition(selectorDefinition);
      SearchBuilderProxy searchBuilderProxy = createBuilder(selectorDefinition);
      if (searchBuilderProxy.isSearchMode()) {
        if (!searchBuilderProxy.isHybird()) {
          SearchReq searchReq = (SearchReq) searchBuilderProxy.build();
          SearchResp searchResp = milvusClient.search(searchReq);
          return toSearchRecordSet((SelectorMetadata) selectorDefinition,
              searchResp);
        } else {
          HybridSearchReq searchReq = (HybridSearchReq) searchBuilderProxy.build();
          SearchResp searchResp = milvusClient.hybridSearch(searchReq);
          return toSearchRecordSet((SelectorMetadata) selectorDefinition,
              searchResp);
        }
      } else {
        QueryReq queryReq = (QueryReq) searchBuilderProxy.build();
        QueryResp queryResp = milvusClient.query(queryReq);
        return toQueryRecordSet((SelectorMetadata) selectorDefinition,
            queryResp);
      }
    } catch (MoqlException e) {
      throw new IOException("Parse failed!", e);
    }
  }

  protected String getCollectionName(TablesMetadata tablesMetadata) {
    QueryableMetadata queryableMetadata = tablesMetadata.getTables().get(0);
    if (!(queryableMetadata instanceof TableMetadata)) {
      throw new UnsupportedOperationException(
          "Unsupport multi tables operation!");
    }
    return ((TableMetadata) queryableMetadata).getValue();
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
          List<CreateCollectionReq.FieldSchema> fieldSchemas = getFieldSchemas(
              collectionName);
          selectorMetadata.getColumns()
              .setColumns(getCollectionFields(fieldSchemas));
        }
      }
    } else {
      throw new UnsupportedOperationException("");
    }
  }

  protected List<CreateCollectionReq.FieldSchema> getFieldSchemas(
      String collection) {
    DescribeCollectionResp respDescribeCollection = milvusClient.describeCollection(
        DescribeCollectionReq.builder().collectionName(collection).build());
    return respDescribeCollection.getCollectionSchema().getFieldSchemaList();
  }

  protected List<ColumnMetadata> getCollectionFields(
      List<CreateCollectionReq.FieldSchema> fieldSchemas) {
    List<ColumnMetadata> columnMetadatas = new LinkedList<>();
    for (CreateCollectionReq.FieldSchema fieldSchema : fieldSchemas) {
      ColumnMetadata columnMetadata = new ColumnMetadata(fieldSchema.getName(),
          fieldSchema.getName());
      columnMetadata.setDataType(fieldSchema.getDataType());
      columnMetadatas.add(columnMetadata);
    }
    return columnMetadatas;
  }

  public SearchReq buildSearchReq(String sql) throws IOException {
    try {
      SelectorDefinition selectorDefinition = parseMoql(sql);
      decorateSelectorDefinition(selectorDefinition);
      SearchBuilderProxy searchBuilderProxy = createBuilder(selectorDefinition);
      return (SearchReq) searchBuilderProxy.build();
    } catch (MoqlException e) {
      throw new IOException("Parse failed!", e);
    }
  }

  public QueryReq buildQueryReq(String sql) throws IOException {
    try {
      SelectorDefinition selectorDefinition = parseMoql(sql);
      decorateSelectorDefinition(selectorDefinition);
      SearchBuilderProxy searchBuilderProxy = createBuilder(selectorDefinition);
      return (QueryReq) searchBuilderProxy.build();
    } catch (MoqlException e) {
      throw new IOException("Parse failed!", e);
    }
  }

  public HybridSearchReq buildHybirdReq(String sql) throws IOException {
    try {
      SelectorDefinition selectorDefinition = parseMoql(sql);
      decorateSelectorDefinition(selectorDefinition);
      SearchBuilderProxy searchBuilderProxy = createBuilder(selectorDefinition);
      return (HybridSearchReq) searchBuilderProxy.build();
    } catch (MoqlException e) {
      throw new IOException("Parse failed!", e);
    }
  }

  protected RecordSet toSearchRecordSet(SelectorMetadata selectorMetadata,
      SearchResp searchResp) {
    List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
    List<ColumnMetadata> columnMetadatas = selectorMetadata.getColumns()
        .getColumns();
    List<Object[]> records = new LinkedList<>();
    RecordSetMetadata recordSetMetadata = new RecordSetMetadata(
        buildOutputColumns(columnMetadatas), null);
    for (List<SearchResp.SearchResult> searchResult : searchResults) {
      for (SearchResp.SearchResult sr : searchResult) {
        records.add(readRecord(columnMetadatas, sr));
      }
    }
    return new RecordSetImpl(recordSetMetadata, new Date(), new Date(),
        records);
  }

  protected Object[] readRecord(List<ColumnMetadata> origColumns,
      SearchResp.SearchResult searchResult) {
    Object[] record = new Object[origColumns.size() + 2];
    int i = 0;
    record[i++] = searchResult.getId();
    record[i++] = searchResult.getScore();
    Map<String, Object> entity = searchResult.getEntity();
    for (ColumnMetadata columnMetadata : origColumns) {
      record[i++] = entity.get(columnMetadata.getName());
    }
    return record;
  }

  protected RecordSet toQueryRecordSet(SelectorMetadata selectorMetadata,
      QueryResp queryResp) {
    List<QueryResp.QueryResult> queryResults = queryResp.getQueryResults();
    List<ColumnMetadata> columnMetadatas = selectorMetadata.getColumns()
        .getColumns();
    List<Object[]> records = new LinkedList<>();
    RecordSetMetadata recordSetMetadata = new RecordSetMetadata(
        (List) columnMetadatas, null);
    for (QueryResp.QueryResult qr : queryResults) {
      records.add(readRecord(columnMetadatas, qr));
    }
    return new RecordSetImpl(recordSetMetadata, new Date(), new Date(),
        records);
  }

  protected Object[] readRecord(List<ColumnMetadata> origColumns,
      QueryResp.QueryResult queryResult) {
    Object[] record = new Object[origColumns.size()];
    int i = 0;
    Map<String, Object> entity = queryResult.getEntity();
    for (ColumnMetadata columnMetadata : origColumns) {
      record[i++] = entity.get(columnMetadata.getName());
    }
    return record;
  }

  protected List buildOutputColumns(List<ColumnMetadata> columnMetadatas) {
    List<ColumnMetadata> columns = new LinkedList<>();
    ColumnMetadata columnMetadata = new ColumnMetadata("id", "id");
    columns.add(0, columnMetadata);
    columnMetadata = new ColumnMetadata("idScore", "idScore");
    columns.add(1, columnMetadata);
    columns.addAll(columnMetadatas);
    return columns;
  }

  protected SearchBuilderProxy createBuilder(
      SelectorDefinition selectorDefinition) throws MoqlException {
    Selector selector = moqlFactory.createSelector(selectorDefinition);
    if (selector instanceof SetlectorImpl)
      throw new UnsupportedOperationException("Unsupport 'set' operation!");
    SelectorImpl selectorImpl = (SelectorImpl) selector;
    if (selectorImpl.getHaving() != null)
      throw new UnsupportedOperationException(
          "Unsupport having clause operation!");
    if (selectorImpl.getOrder() != null)
      throw new UnsupportedOperationException(
          "Unsupport order clause operation!");
    SearchBuilderProxy builder = new SearchBuilderProxy();
    buildFromClause(builder, selectorImpl.getTables().getQueryable());
    buildSelectClause(builder, selectorImpl.getRecordSetOperator());
    if (selectorImpl.getWhere() != null) {
      buildWhereClause(builder, selectorImpl.getWhere());
    }
    if (selectorImpl.getLimit() != null) {
      buildLimitClause(builder, selectorImpl.getLimit());
    } else {
      builder.withTopK(10);
      builder.withOffset(0);
    }
    return builder;
  }

  protected void buildFromClause(SearchBuilderProxy builder,
      Queryable queryable) throws MoqlException {
    if (queryable == null)
      return;
    if (queryable instanceof CommonTable) {
      CommonTable commonTable = (CommonTable) queryable;
      builder.withCollectionName(commonTable.getTableMetadata().getValue());
    } else if (queryable instanceof SelectorTable) {
      SelectorTable selectorTable = (SelectorTable) queryable;
      SearchBuilderProxy annBuilder = new SearchBuilderProxy(true);
      SelectorImpl selectorImpl = (SelectorImpl) selectorTable.getSelector();
      buildWhereClause(annBuilder, selectorImpl.getWhere());
      builder.addAnnSearchRequest((AnnSearchReq) annBuilder.build());
    } else {
      buildFromClause(builder, (Join) queryable);
    }
  }

  protected void buildFromClause(SearchBuilderProxy builder, Join join)
      throws MoqlException {
    if (join instanceof InnerJoin) {
      InnerJoin innerJoin = (InnerJoin) join;
      buildFromClause(builder, innerJoin.getLeftQueryable());
      buildFromClause(builder, innerJoin.getRightQueryable());
    } else {
      throw new UnsupportedOperationException(
          String.format("Unsupport %s tables operation!",
              join.getJoinMetadata().getJoinType().name()));
    }
  }

  protected void buildSelectClause(SearchBuilderProxy builder,
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
    if (recordSetOperator instanceof Group) {
      Group group = (Group) recordSetOperator;
      if (group.getGroupMetadatas().size() != 1) {
        throw new IllegalStateException("Invalid group columns!");
      }
      GroupMetadata groupMetadata = group.getGroupMetadatas().get(0);
      builder.withGroup(groupMetadata.getColumn());
    }
  }

  protected void buildWhereClause(SearchBuilderProxy builder,
      Condition condition) throws MoqlException {
    StringBuilder stringBuilder = new StringBuilder();
    buildOperand(builder, condition.getOperand(), stringBuilder);
    if (stringBuilder.length() > 0) {
      builder.withFilter(stringBuilder.toString());
    }
  }

  protected void buildOperand(SearchBuilderProxy builder, Operand operand,
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

  protected void buildFunction(SearchBuilderProxy builder, Function function,
      StringBuilder stringBuilder) {
    if (function.getName().equals(RESERVED_FUNC_PARTITIONBY)) {
      PartitionBy partitionBy = (PartitionBy) function;
      builder.withPartitionNames(partitionBy.getPartitions());
    } else if (function.getName().equals(RESERVED_FUNC_VMATCH)) {
      VMatch vMatch = (VMatch) function;
      builder.withVector(vMatch.getVectorName(), vMatch.getMetricType(),
          vMatch.getVectors());
    } else if (function.getName().equals(RESERVED_FUNC_CONSISTENCYLEVEL)) {
      ConsistencyLevel consistencyLevel = (ConsistencyLevel) function;
      builder.withConsistencyLevel(consistencyLevel.getConsistencyLevel());
    } else if (function.getName().equals(RESERVED_FUNC_SEARCH_RANGE)) {
      SearchRange searchRange = (SearchRange) function;
      builder.withRange(searchRange.getMetricType(), searchRange.getRadius(),
          searchRange.getRangeFilter());
    } else if (function.getName().equals(RESERVED_FUNC_RRF_RANKER)) {
      RRFRanker rrfRanker = (RRFRanker) function;
      builder.withRanker(rrfRanker.getRrfRanker());
    } else if (function.getName().equals(RESERVED_FUNC_WEIGHTED_RANKER)) {
      WeightedRanker weightedRanker = (WeightedRanker) function;
      builder.withRanker(weightedRanker.getWeightedRanker());
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
    } else if (function.getName().equals(RESERVED_FUNC_DROP_RATIO_SEARCH)) {
      DropRatioSearch dropRatioSearch = (DropRatioSearch) function;
      builder.withDropRatio(dropRatioSearch.getDropRatio());
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

  protected void buildLimitClause(SearchBuilderProxy builder, Limit limit) {
    LimitMetadata limitMetadata = limit.getLimitMetadata();
    builder.withTopK(limitMetadata.getValue());
    if (limitMetadata.getOffset() != 0)
      builder.withOffset(limitMetadata.getOffset());
  }
}
