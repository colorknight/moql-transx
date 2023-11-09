package org.datayoo.moql.querier.tcvector;

import com.tencent.tcvectordb.model.param.dml.*;

import java.util.List;

public class BuilderProxy {
  SearchByVectorParam.Builder searchBuilder;

  QueryParam.Builder queryBuilder;

  protected boolean search = false;

  public BuilderProxy() {
    searchBuilder = SearchByVectorParam.newBuilder();
    searchBuilder.withLimit(1);

    queryBuilder = QueryParam.newBuilder();
  }

  public BuilderProxy withFilter(String expr) {
    expr = expr.replace('\'', '"');
    searchBuilder.withFilter(new Filter(expr));
    queryBuilder.withFilter(new Filter(expr));
    return this;
  }

  public BuilderProxy withOutFields(List<String> fields) {
    queryBuilder.withOutputFields(fields);
    searchBuilder.withOutputFields(fields);
    return this;
  }

  public boolean isSearchMode() {
    return search;
  }

  public BuilderProxy withSearchVectors(List<List<Double>> vectors) {
    this.search = true;
    searchBuilder.withVectors(vectors);
    return this;
  }

  public BuilderProxy withQueryId(List<String> documentIds) {
    queryBuilder.withDocumentIds(documentIds);
    return this;
  }

  public BuilderProxy withLimit(int limit) {
    queryBuilder.withLimit(limit);
    searchBuilder.withLimit(limit);
    return this;
  }

  public BuilderProxy withOffset(int offset) {
    queryBuilder.withOffset(offset);
    return this;
  }

  public SearchByVectorParam buildSearch() {

    return searchBuilder.build();
  }

  public BuilderProxy withParams(Params params) {
    searchBuilder.withParams(params);
    return this;
  }

  public QueryParam buildQuery() {
    return queryBuilder.build();
  }

}
