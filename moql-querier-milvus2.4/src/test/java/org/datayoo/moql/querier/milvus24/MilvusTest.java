package org.datayoo.moql.querier.milvus24;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.*;
import org.datayoo.moql.RecordSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class MilvusTest {

  MilvusClientV2 milvusClient;

  @Before
  public void setUp() throws Exception {
    String url = String.format("%s://%s:%d", "http" ,"datayoo05"
        , 19530);
    ConnectConfig connectConfig = ConnectConfig.builder()
        .uri(url)
        .build();

    milvusClient = new MilvusClientV2(connectConfig);
  }

  @After
  public void tearDown() throws Exception {
    if (milvusClient == null) {
      return;
    }
    try {
      milvusClient.close();
    } finally {
      milvusClient = null;
    }
  }

  @Test
  public void query1() throws IOException {
    String collection  = "hybrid_search_collection";
    String sql = "select * from hybrid_search_collection a, "
        + "(select * from hybrid_search_collection where vmatch(dense, 'L2', '[[1.0, 2.0, 3.0],[1.1,2.1,3.1]]')) b, "
        + "(select * from hybrid_search_collection where vmatch(sparse, 'IP', '[{\"4286132664\":0.1764169}, {\"2908500734\":0.1764169}]')) c "
        + " limit 5";

    LoadCollectionReq loadCollectionReq = LoadCollectionReq.builder()
        .collectionName(collection)
        .build();

    milvusClient.loadCollection(loadCollectionReq);

    MilvusQuerier milvusQuerier = new MilvusQuerier(milvusClient);
    RecordSet recordSet = milvusQuerier.query(sql);

    ReleaseCollectionReq releaseCollectionReq = ReleaseCollectionReq.builder()
        .collectionName(collection)
        .build();

    milvusClient.releaseCollection(releaseCollectionReq);

    System.out.println(recordSet.getRecords().size());
  }
}
