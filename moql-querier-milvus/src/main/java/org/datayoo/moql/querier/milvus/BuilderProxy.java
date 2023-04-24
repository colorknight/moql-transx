package org.datayoo.moql.querier.milvus;

import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.FieldData;
import io.milvus.param.MetricType;
import io.milvus.param.dml.QueryParam;
import io.milvus.param.dml.SearchParam;
import org.apache.commons.lang3.Validate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author tangtadin
 * @version 1.0
 * @description: TODO
 * @date 2023/4/24 00:47
 */
public class BuilderProxy {

  protected SearchParam.Builder searchParamBuilder;
  protected QueryParam.Builder queryParamBuilder;

  protected Map<String, Object> paramMap = new HashMap<>();

  protected boolean search = false;

  public BuilderProxy() {
    this.searchParamBuilder = SearchParam.newBuilder();
    searchParamBuilder.withConsistencyLevel(ConsistencyLevelEnum.STRONG);
    paramMap.put("offset", 0);
    this.queryParamBuilder = QueryParam.newBuilder();
    queryParamBuilder.withOffset(0l);
    queryParamBuilder.withConsistencyLevel(ConsistencyLevelEnum.STRONG);
  }

  public BuilderProxy withCollectionName(String collectionName) {
    searchParamBuilder.withCollectionName(collectionName);
    queryParamBuilder.withCollectionName(collectionName);
    return this;
  }

  public BuilderProxy withOutFields(List<String> fields) {
    searchParamBuilder.withOutFields(fields);
    queryParamBuilder.withOutFields(fields);
    return this;
  }

  public BuilderProxy withExpr(String expr) {
    expr = expr.replace('\'', '"');
    searchParamBuilder.withExpr(expr);
    queryParamBuilder.withExpr(expr);
    return this;
  }

  public BuilderProxy withPartitionNames(List<String> partitionNames) {
    searchParamBuilder.withPartitionNames(partitionNames);
    queryParamBuilder.withPartitionNames(partitionNames);
    return this;
  }

  public BuilderProxy withVectorFieldName(String vectorFieldName) {
    search = true;
    searchParamBuilder.withVectorFieldName(vectorFieldName);
    return this;
  }

  public BuilderProxy withVectors(List<?> vectors) {
    searchParamBuilder.withVectors(vectors);
    return this;
  }

  public BuilderProxy withMetricType(MetricType metricType) {
    searchParamBuilder.withMetricType(metricType);
    return this;
  }

  public BuilderProxy withConsistencyLevel(
      ConsistencyLevelEnum consistencyLevel) {
    searchParamBuilder.withConsistencyLevel(consistencyLevel);
    queryParamBuilder.withConsistencyLevel(consistencyLevel);
    return this;
  }

  public BuilderProxy withGracefulTime(Long gracefulTime) {
    searchParamBuilder.withGracefulTime(gracefulTime);
    queryParamBuilder.withGracefulTime(gracefulTime);
    return this;
  }

  public BuilderProxy withGuaranteeTimestamp(Long ts) {
    searchParamBuilder.withGuaranteeTimestamp(ts);
    queryParamBuilder.withGuaranteeTimestamp(ts);
    return this;
  }

  public BuilderProxy withRoundDecimal(Integer decimal) {
    searchParamBuilder.withRoundDecimal(decimal);
    return this;
  }

  public BuilderProxy withTravelTimestamp(Long ts) {
    searchParamBuilder.withTravelTimestamp(ts);
    queryParamBuilder.withTravelTimestamp(ts);
    return this;
  }

  public BuilderProxy withNProbe(Long nProbe) {
    paramMap.put("nProbe", nProbe);
    return this;
  }

  public BuilderProxy withEf(Long ef) {
    paramMap.put("ef", ef);
    return this;
  }

  public BuilderProxy withSearchK(Long searchK) {
    paramMap.put("search_k", searchK);
    return this;
  }

  public BuilderProxy withTopK(int topK) {
    searchParamBuilder.withTopK(topK);
    queryParamBuilder.withLimit((long) topK);
    return this;
  }

  public BuilderProxy withOffset(int offset) {
    paramMap.put("offset", offset);
    queryParamBuilder.withOffset((long) offset);
    return this;
  }

  public BuilderProxy withParams(String params) {
    searchParamBuilder.withParams(params);
    return this;
  }

  public Object build() {
    if (search)
      return searchParamBuilder.build();
    return queryParamBuilder.build();
  }

  public boolean isSearchMode() {
    return search;
  }
}
