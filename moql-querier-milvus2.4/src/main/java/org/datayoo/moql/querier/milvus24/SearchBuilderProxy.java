package org.datayoo.moql.querier.milvus24;

import com.google.gson.Gson;
import io.milvus.param.MetricType;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.HybridSearchReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.ranker.BaseRanker;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author tangtadin
 * @version 1.0
 * @description: TODO
 * @date 2023/4/24 00:47
 */
public class SearchBuilderProxy {

  protected SearchReq.SearchReqBuilder searchParamBuilder;
  protected QueryReq.QueryReqBuilder queryParamBuilder;

  protected HybridSearchReq.HybridSearchReqBuilder hybirdSearchParamBuilder;

  protected AnnSearchReq.AnnSearchReqBuilder annSearchParamBuilder;

  protected Map<String, Object> searchParamMap = new HashMap<>();
  protected Map<String, Object> paramMap = new HashMap<>();

  protected List<AnnSearchReq> annSearchReqs = new LinkedList<>();

  protected boolean search = false;
  protected boolean hybird = false;

  protected boolean annSearch = false;

  public SearchBuilderProxy() {
    this.searchParamBuilder = SearchReq.builder();
    searchParamBuilder.consistencyLevel(ConsistencyLevel.STRONG);
    this.queryParamBuilder = QueryReq.builder();
    queryParamBuilder.offset(0l);
    queryParamBuilder.consistencyLevel(ConsistencyLevel.STRONG);
    this.hybirdSearchParamBuilder = HybridSearchReq.builder();
    hybirdSearchParamBuilder.consistencyLevel(ConsistencyLevel.STRONG);
    searchParamMap.put("params", paramMap);
  }

  public SearchBuilderProxy(boolean annSearch) {
    this.searchParamBuilder = SearchReq.builder();
    searchParamBuilder.consistencyLevel(ConsistencyLevel.STRONG);
    this.queryParamBuilder = QueryReq.builder();
    queryParamBuilder.offset(0l);
    queryParamBuilder.consistencyLevel(ConsistencyLevel.STRONG);
    this.hybirdSearchParamBuilder = HybridSearchReq.builder();
    hybirdSearchParamBuilder.consistencyLevel(ConsistencyLevel.STRONG);
    this.annSearch = true;
    this.annSearchParamBuilder = AnnSearchReq.builder();
    searchParamMap.put("params", paramMap);
  }

  public SearchBuilderProxy withCollectionName(String collectionName) {
    if (annSearch) {
      throw new IllegalStateException("Unsupport in annSearch mode!");
    }
    searchParamBuilder.collectionName(collectionName);
    queryParamBuilder.collectionName(collectionName);
    hybirdSearchParamBuilder.collectionName(collectionName);
    return this;
  }

  public SearchBuilderProxy withOutFields(List<String> fields) {
    if (annSearch) {
      throw new IllegalStateException("Unsupport in annSearch mode!");
    }
    searchParamBuilder.outputFields(fields);
    queryParamBuilder.outputFields(fields);
    hybirdSearchParamBuilder.outFields(fields);
    return this;
  }

  public SearchBuilderProxy withFilter(String expr) {
    if (annSearch || hybird) {
      throw new IllegalStateException("Unsupport in annSearch or hybird mode!");
    }
    expr = expr.replace('\'', '"');
    searchParamBuilder.filter(expr);
    queryParamBuilder.filter(expr);
    return this;
  }

  public SearchBuilderProxy withPartitionNames(List<String> partitionNames) {
    if (annSearch) {
      throw new IllegalStateException("Unsupport in annSearch mode!");
    }
    searchParamBuilder.partitionNames(partitionNames);
    queryParamBuilder.partitionNames(partitionNames);
    hybirdSearchParamBuilder.partitionNames(partitionNames);
    return this;
  }

  public SearchBuilderProxy withVector(String vectorFieldName,
      MetricType metricType, List<?> vectors) {
    if (hybird) {
      throw new IllegalStateException("Unsupport in hybird mode!");
    }
    search = true;
    searchParamMap.put("metric_type", metricType.name());
    searchParamBuilder.annsField(vectorFieldName);
    annSearchParamBuilder.vectorFieldName(vectorFieldName);
    searchParamBuilder.data(vectors);
    annSearchParamBuilder.vectors(vectors);
    return this;
  }

  public SearchBuilderProxy withRange(MetricType metricType, float radius,
      float range_filter) {
    if (annSearch) {
      throw new IllegalStateException("Unsupport in annSearch mode!");
    }
    search = true;
    searchParamMap.put("metric_type", metricType.name());
    paramMap.put("radius", radius);
    paramMap.put("range_filter", range_filter);
    return this;
  }

  public SearchBuilderProxy withGroup(String filedName) {
    if (annSearch) {
      throw new IllegalStateException("Unsupport in annSearch mode!");
    }
    search = true;
    searchParamBuilder.groupByFieldName(filedName);
    return this;
  }

  public SearchBuilderProxy withDropRatio(float dropRatio) {
    search = true;
    paramMap.put("drop_ratio_search", dropRatio);
    return this;
  }

  public SearchBuilderProxy addAnnSearchRequest(AnnSearchReq annSearchReq) {
    if (annSearch) {
      throw new IllegalStateException("Unsupport in annSearch mode!");
    }
    search = true;
    hybird = true;
    annSearchReqs.add(annSearchReq);
    return this;
  }

  public SearchBuilderProxy withRanker(BaseRanker baseRanker) {
    if (annSearch) {
      throw new IllegalStateException("Unsupport in annSearch mode!");
    }
    search = true;
    hybird = true;
    hybirdSearchParamBuilder.ranker(baseRanker);
    return this;
  }

  public SearchBuilderProxy withConsistencyLevel(
      ConsistencyLevel consistencyLevel) {
    searchParamBuilder.consistencyLevel(consistencyLevel);
    queryParamBuilder.consistencyLevel(consistencyLevel);
    return this;
  }

  public SearchBuilderProxy withGracefulTime(Long gracefulTime) {
    if (annSearch) {
      throw new IllegalStateException("Unsupport in annSearch mode!");
    }
    search = true;
    searchParamBuilder.gracefulTime(gracefulTime);
    return this;
  }

  public SearchBuilderProxy withGuaranteeTimestamp(Long ts) {
    if (annSearch) {
      throw new IllegalStateException("Unsupport in annSearch mode!");
    }
    search = true;
    searchParamBuilder.guaranteeTimestamp(ts);
    return this;
  }

  public SearchBuilderProxy withRoundDecimal(Integer decimal) {
    if (annSearch) {
      throw new IllegalStateException("Unsupport in annSearch mode!");
    }
    search = true;
    searchParamBuilder.roundDecimal(decimal);
    return this;
  }

  public SearchBuilderProxy withNProbe(Long nProbe) {
    if (annSearch) {
      throw new IllegalStateException("Unsupport in annSearch mode!");
    }
    search = true;
    paramMap.put("nProbe", nProbe);
    return this;
  }

  public SearchBuilderProxy withEf(Long ef) {
    if (annSearch) {
      throw new IllegalStateException("Unsupport in annSearch mode!");
    }
    search = true;
    paramMap.put("ef", ef);
    return this;
  }

  public SearchBuilderProxy withSearchK(Long searchK) {
    if (annSearch) {
      throw new IllegalStateException("Unsupport in annSearch mode!");
    }
    search = true;
    paramMap.put("search_k", searchK);
    return this;
  }

  public SearchBuilderProxy withTopK(int topK) {
    searchParamBuilder.topK(topK);
    queryParamBuilder.limit((long) topK);
    hybirdSearchParamBuilder.topK(topK);
    annSearchParamBuilder.topK(topK);
    return this;
  }

  public SearchBuilderProxy withOffset(int offset) {
    if (annSearch || hybird) {
      throw new IllegalStateException("Unsupport in hybird or annSearch mode!");
    }
    searchParamBuilder.offset(offset);
    queryParamBuilder.offset((long) offset);
    return this;
  }

  public Object build() {
    if (search) {
      if (!hybird) {
        if (annSearch) {
          Gson gson = new Gson();
          annSearchParamBuilder.params(gson.toJson(searchParamMap));
          return annSearchParamBuilder.build();
        } else {
          searchParamBuilder.searchParams(searchParamMap);
          return searchParamBuilder.build();
        }
      } else {
        hybirdSearchParamBuilder.searchRequests(annSearchReqs);
        return hybirdSearchParamBuilder.build();
      }
    }
    return queryParamBuilder.build();
  }

  public boolean isSearchMode() {
    return search;
  }

  public boolean isHybird() {
    return hybird;
  }

  public boolean isAnnSearch() {
    return annSearch;
  }
}
