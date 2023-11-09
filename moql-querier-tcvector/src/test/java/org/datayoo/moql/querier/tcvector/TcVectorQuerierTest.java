package org.datayoo.moql.querier.tcvector;

import com.tencent.tcvectordb.client.VectorDBClient;
import com.tencent.tcvectordb.model.param.database.ConnectParam;
import com.tencent.tcvectordb.model.param.enums.ReadConsistencyEnum;
import junit.framework.TestCase;
import org.datayoo.moql.RecordSet;

import java.io.IOException;

public class TcVectorQuerierTest extends TestCase {

  protected VectorDBClient vectorDBClient;

  protected String url = "http://lb-ogjw4ifb-j6fo4aubx9rg3cv0.clb.ap-beijing.tencentclb.com:30000";

  protected String key = "ZEBsqPPSctjG4FFz8fXP6k7q5KIpDKAmXwN2RFK6";

  public void testQuery() throws IOException {
    TcVectorQuerier querier = new TcVectorQuerier(vectorDBClient);
    String sql = "select * from datayoo.book where id = '0001' limit 10";

    RecordSet recordSet = querier.query(sql);

    System.out.println(recordSet.getRecords().size());
  }

  public void testSearch() throws IOException {
    TcVectorQuerier querier = new TcVectorQuerier(vectorDBClient);

    String sql = "select * from datayoo.book where withVectors('[[0.3123, 0.43, 0.213], [0.5123, 0.63, 0.413]]') limit 2";

    RecordSet recordSet = querier.query(sql);

    System.out.println(recordSet.getRecords().size());
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    ConnectParam connectParam = ConnectParam.newBuilder().withUrl(url)
        .withUsername("root").withKey(key).withTimeout(30).build();
    vectorDBClient = new VectorDBClient(connectParam,
        ReadConsistencyEnum.EVENTUAL_CONSISTENCY);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    vectorDBClient = null;
  }
}
