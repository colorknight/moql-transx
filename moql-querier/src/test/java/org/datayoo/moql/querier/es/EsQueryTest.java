package org.datayoo.moql.querier.es;

import junit.framework.TestCase;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.datayoo.moql.RecordSet;
import org.datayoo.moql.SelectorDefinition;
import org.datayoo.moql.engine.MoqlEngine;
import org.datayoo.moql.parser.MoqlParser;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.*;

public class EsQueryTest extends TestCase {
  protected RestHighLevelClient restHighLevelClient;

  public void test01() throws IOException {
    open();
    String sql = "select titleWords.index,titleWords.term from zz_news_entity-202205";
    EsDataQuerier esDataQuerier = new EsDataQuerier();
    esDataQuerier.bind(restHighLevelClient.getLowLevelClient());

    String[] array = sql.split("(?i)from");
    String tableNames = array[array.length - 1].trim().split(" ")[0];

    sql = sql.replace(tableNames, "table");
    Properties properties = new Properties();
    Properties indexNameMappings = new Properties();
    indexNameMappings.put("table", tableNames);
    properties.put(EsDataQuerier.INDEX_NAME_MAPPINGS, indexNameMappings);
    RecordSet recordSet = esDataQuerier.query(sql, properties);

    System.out.println(recordSet.getRecords().size());
    close();
  }

  public void open() throws IOException {
    if (restHighLevelClient != null) {
      return;
    }

    String url = "172.30.30.4:19200";

    List<Map<String, Object>> address = new LinkedList<>();
    if (StringUtils.isNotEmpty(url)) {
      String[] addressArray = StringUtils.split(url, ",");
      for (String addr : addressArray) {
        String host = addr.split(":")[0];
        String port = addr.split(":")[1];
        Map<String, Object> connectionInfo = new HashMap<>();
        connectionInfo.put("host", host);
        connectionInfo.put("port", Integer.valueOf(port));
        address.add(connectionInfo);
      }
    } else {
      Map<String, Object> connectionInfo = new HashMap<>();
      connectionInfo.put("host", "172.30.30.4");
      connectionInfo.put("port", 9200);
      address.add(connectionInfo);
    }
    restHighLevelClient = restHighLevelClient(address, "", "");
  }

  public static RestHighLevelClient restHighLevelClient(
      List<Map<String, Object>> address, String username, String password) {
    // 拆分地址
    List<HttpHost> hostLists = new ArrayList<>();
    for (Map<String, Object> hostInfo : address) {
      String host = (String) hostInfo.get("host");
      int port = (int) hostInfo.get("port");
      hostLists.add(new HttpHost(host, port, "http"));
    }
    // 转换成 HttpHost 数组
    HttpHost[] httpHost = hostLists.toArray(new HttpHost[] {});
    // 构建连接对象
    RestClientBuilder builder = RestClient.builder(httpHost);
    if (StringUtils.isNotEmpty(username)) {
      final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(AuthScope.ANY,
          new UsernamePasswordCredentials(username, password));
      builder.setHttpClientConfigCallback(
          f -> f.setDefaultCredentialsProvider(credentialsProvider));
    }

    // socket通信超时时间
    // 异步连接延时配置
    builder.setRequestConfigCallback(requestConfigBuilder -> {
      requestConfigBuilder.setConnectTimeout(300 * 1000);
      requestConfigBuilder.setSocketTimeout(300 * 1000);
      requestConfigBuilder.setConnectionRequestTimeout(300 * 1000);
      return requestConfigBuilder;
    });

    return new RestHighLevelClient(builder);
  }

  public void close() throws IOException {
    if (restHighLevelClient != null) {
      restHighLevelClient.close();
      restHighLevelClient = null;
    }
  }
}
