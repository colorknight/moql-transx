package org.datayoo.moql.querier.milvus;

import io.milvus.param.dml.SearchParam;
import junit.framework.TestCase;

import java.io.IOException;

/**
 * @author tangtadin
 * @version 1.0
 * @description: TODO
 * @date 2023/2/24 10:15
 */
public class MilvusQuerierTest extends TestCase {

  public void testBuildQueryParam() {

    MilvusQuerier milvusQuerier = new MilvusQuerier();
    String sql = "select col1, col2 from t where col3 = 4 and vMatch(vec, 'L2', '[[1.0, 2.0, 3.0],[1.1,2.1,3.1]]') and col4 in ('a', 'b', 'c') and consistencyLevel('STRONG') and nProbe(10) limit 10,5";
    try {
      SearchParam searchParam = milvusQuerier.buildSearchParam(sql);
      System.out.println(searchParam);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void testBuildQueryParam2() {
    MilvusQuerier milvusQuerier = new MilvusQuerier();
    String sql = "select ip.src from " +
        "ip3 ip group by ip.src";
    try {
      SearchParam searchParam = milvusQuerier.buildSearchParam(sql);
      System.out.println(searchParam);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
