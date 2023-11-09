package org.datayoo.moql.querier.es;

import junit.framework.TestCase;

public class EsQueryTest extends TestCase {
//  protected RestHighLevelClient restHighLevelClient;
//
//  public void test01() throws IOException {
//    open();
//    String sql = "select siteName,_id,_index,_type from target_index" ;
//    EsDataQuerier esDataQuerier = new EsDataQuerier();
//    esDataQuerier.bind(restHighLevelClient.getLowLevelClient());
//
//    String[] array = sql.split("(?i)from");
//    String tableNames = array[array.length - 1].trim().split(" ")[0];
//
//    sql = sql.replace(tableNames, "table");
//    Properties properties = new Properties();
//    Properties indexNameMappings = new Properties();
//    indexNameMappings.put("table", tableNames);
//    properties.put(EsDataQuerier.INDEX_NAME_MAPPINGS, indexNameMappings);
//    RecordSet recordSet = esDataQuerier.query(sql, properties);
//
//    System.out.println(recordSet.getRecords().size());
//    close();
//  }
//
//  public void test02() throws IOException {
//    open();
//    String sql = "select * from target_index" ;
//    EsDataQuerier esDataQuerier = new EsDataQuerier();
//    esDataQuerier.bind(restHighLevelClient.getLowLevelClient());
//
//    String[] array = sql.split("(?i)from");
//    String tableNames = array[array.length - 1].trim().split(" ")[0];
//
//    sql = sql.replace(tableNames, "table");
//    Properties properties = new Properties();
//    Properties indexNameMappings = new Properties();
//    indexNameMappings.put("table", tableNames);
//    properties.put(EsDataQuerier.INDEX_NAME_MAPPINGS, indexNameMappings);
//    RecordSet recordSet = esDataQuerier.query(sql, properties);
//
//    System.out.println(recordSet.getRecords().size());
//    close();
//  }
//
//  public void open() throws IOException {
//    if (restHighLevelClient != null) {
//      return;
//    }
//
//    String url = "172.31.179.128:9200";
//
//    List<Map<String, Object>> address = new LinkedList<>();
//    if (StringUtils.isNotEmpty(url)) {
//      String[] addressArray = StringUtils.split(url, ",");
//      for (String addr : addressArray) {
//        String host = addr.split(":")[0];
//        String port = addr.split(":")[1];
//        Map<String, Object> connectionInfo = new HashMap<>();
//        connectionInfo.put("host", host);
//        connectionInfo.put("port", Integer.valueOf(port));
//        address.add(connectionInfo);
//      }
//    } else {
//      Map<String, Object> connectionInfo = new HashMap<>();
//      connectionInfo.put("host", "172.30.30.4");
//      connectionInfo.put("port", 9200);
//      address.add(connectionInfo);
//    }
//    restHighLevelClient = restHighLevelClient(address, "elastic", "datayoo123");
//  }
//
//  public static RestHighLevelClient restHighLevelClient(
//      List<Map<String, Object>> address, String username, String password) {
//    // 拆分地址
//    List<HttpHost> hostLists = new ArrayList<>();
//    for (Map<String, Object> hostInfo : address) {
//      String host = (String) hostInfo.get("host");
//      int port = (int) hostInfo.get("port");
//      hostLists.add(new HttpHost(host, port, "http"));
//    }
//    // 转换成 HttpHost 数组
//    HttpHost[] httpHost = hostLists.toArray(new HttpHost[] {});
//    // 构建连接对象
//    RestClientBuilder builder = RestClient.builder(httpHost);
//    if (StringUtils.isNotEmpty(username)) {
//      final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//      credentialsProvider.setCredentials(AuthScope.ANY,
//          new UsernamePasswordCredentials(username, password));
//      builder.setHttpClientConfigCallback(
//          f -> f.setDefaultCredentialsProvider(credentialsProvider));
//    }
//
//    // socket通信超时时间
//    // 异步连接延时配置
//    builder.setRequestConfigCallback(requestConfigBuilder -> {
//      requestConfigBuilder.setConnectTimeout(300 * 1000);
//      requestConfigBuilder.setSocketTimeout(300 * 1000);
//      requestConfigBuilder.setConnectionRequestTimeout(300 * 1000);
//      return requestConfigBuilder;
//    });
//
//    return new RestHighLevelClient(builder);
//  }
//
//  public void close() throws IOException {
//    if (restHighLevelClient != null) {
//      restHighLevelClient.close();
//      restHighLevelClient = null;
//    }
//  }
}
