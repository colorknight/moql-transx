package org.datayoo.moql.querier.mongodb;

import junit.framework.TestCase;

public class MongoDBQuerierTest extends TestCase {
//
//  protected MongoDBQuerier dataQuerier = new MongoDBQuerier();
//
//  @Override
//  public void setUp() throws Exception {
//    String[] serverIps = new String[] { "172.30.30.8" };
//    Properties properties = new Properties();
//    dataQuerier.connect(serverIps, properties);
//    super.setUp();
//  }
//
//  @Override
//  public void tearDown() throws Exception {
//    dataQuerier.disconnect();
//    super.tearDown();
//  }
//
//  public void testMongoFind() {
//    MongoClient mongoClient = MongoClients
//        .create("mongodb://172.30.30.8:27017/?ssh=true");
//    MongoDatabase mongoDatabase = mongoClient.getDatabase("bracket");
//    MongoCollection mongoCollection = mongoDatabase
//        .getCollection("sengeeArchive.files");
//    mongoCollection.find().forEach(doc -> System.out.println(doc.toString()));
//    mongoClient.close();
//  }
//
//  public void testCommonQuery() {
//    String sql = "select archive.* from bracket.sengeeArchive.files archive";
//    try {
//      RecordSet recordSet = dataQuerier.query(sql);
//      outputRecordSet(recordSet);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }
//
//  public void testProjectQuery() {
//    String sql = "select archive.length, archive.filename,  archive.metadata from bracket.sengeeArchive.files archive";
//    try {
//      RecordSet recordSet = dataQuerier.query(sql);
//      outputRecordSet(recordSet);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }
//
//  public void testIsQuery() {
//    String sql = "select archive.* from bracket.sengeeArchive.files archive where archive.metadata is not null";
//    try {
//      RecordSet recordSet = dataQuerier.query(sql);
//      outputRecordSet(recordSet);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }
//
//  public void testSortQuery() {
//    String sql = "select archive.* from bracket.sengeeArchive.files archive where archive.metadata is not null order by archive.length desc";
//    try {
//      RecordSet recordSet = dataQuerier.query(sql);
//      outputRecordSet(recordSet);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }
//
//  // 1
//  public void testSimpleQuery() {
//    String sql = "select w.dns, w.ip, w.port from mydb.web w";
//    queryAndOutput(sql);
//  }
//
//  // 2
//  public void testConditionQuery() {
//    String sql = "select u.src, u.proto, u.sport from mydb.ip3 u where src='127.0.0.1' and sport=2.0";
//    queryAndOutput(sql);
//  }
//
//  // 3
//  public void testConditionQuery2() {
//    String sql =
//        "select w.dns, w.ip from mydb.web w where (w.port=443 or w.port=8080) and "
//            + "w.ip='127.0.0.1' or w.ip='127.0.0.2'";
//    queryAndOutput(sql);
//  }
//
//  // 4 ?
//  public void testGroupOrder() {
//    String sql = "select ip.src, ip.proto, max(ip.sport) sport, min(ip.sport) from mydb.ip3 ip group by ip.src, ip.proto order by ip.src desc limit 2 ";
//    queryAndOutput(sql);
//  }
//
//  // 5 ?
//  public void testHaving() {
//    String sql = "select ip.src, ip.proto, max(ip.sport) sport, min(ip.sport) from mydb.ip3 ip group by ip.src, ip.proto having sport > 2";
//    queryAndOutput(sql);
//  }
//
//  // 6
//  public void testNEConditionQuery() {
//    String sql = "select w.dns, w.ip  from mydb.web w where w.ip<>'127.0.0.2'";
//    queryAndOutput(sql);
//  }
//
//  // 7
//  public void testNotOrEqualQuery() {
//    String sql = "select w.dns, w.ip from mydb.web w where not w.ip='127.0.0.2' or w.ip = '127.0.0.5'";
//    queryAndOutput(sql);
//  }
//
//  // 8 nin
//  public void testNotInQuery() {
//    String sql = "select w.dns, w.ip from mydb.web w where not w.ip in ('127.0.0.1','127.0.0.3')";
//    queryAndOutput(sql);
//  }
//
//  // 9 nor
//  public void testNotAndQuery() {
//    String sql = "select w.dns, w.ip from mydb.web w where not (w.ip='127.0.0.1' and w.ip='127.0.0.2' )";
//    queryAndOutput(sql);
//  }
//
//  // 10
//  public void testAndOrConditionQuery() {
//    String sql = "select w.dns, w.ip, w.country, w.text  from mydb.w w where w.port=443 and w.country in ('美国','澳大利亚') or w.port = 123 and w.dns = '255.255.255.234'";
//    queryAndOutput(sql);
//  }
//
//  // 11
//  public void testLikeConditionQuery() {
//    String sql = "select w.dns, w.ip from mydb.w w where w.country like '美%'";
//    queryAndOutput(sql);
//  }
//
//  // 12
//  public void testIsConditionQuery() {
//    String sql = "select w.dns, w.ip from mydb.web w where w.region is null";
//    queryAndOutput(sql);
//    sql = "select w.dns, w.ip  from mydb.web w where w.region is not null";
//    queryAndOutput(sql);
//  }
//
//  // 12-2
//  public void testIsConditionQuery2() {
//    String sql = "select ip.src, ip.proto from mydb.ip3 ip where ip.src is null";
//    queryAndOutput(sql);
//    sql = "select ip.src, ip.proto from mydb.ip3 ip where ip.src is not null";
//    queryAndOutput(sql);
//  }
//
//  // 13
//  public void testParenConditionQuery() {
//    String sql = "select w.dns, w.ip  from mydb.web w where w.region is null and (w.port = 88 or w.port = 443)";
//    queryAndOutput(sql);
//  }
//
//  // 14
//  public void testLimitQuery() {
//    String sql = "select w.dns, w.ip  from mydb.web w where w.dns='255.255.255.2' limit 2,2";
//    queryAndOutput(sql);
//  }
//
//  // 15
//  public void testLeftjoinQuery() {
//    String sql = "select item,price from mydb.orders o left join mydb.inventory i on o.item = i.sku where item <> null limit 1,2";
//    queryAndOutput(sql);
//  }
//
//  // 16 ?
//  public void testDateQuery() {
//    String sql = "select w.dns, w.ip, w.time from mydb.w w where w.time is not null)";
//    queryAndOutput(sql);
//    sql = "select w.dns, w.ip from mydb.w w where w.time > ISODate('2018-09-18T09:54:00Z')";
//    queryAndOutput(sql);
//  }
//
//  // 17
//  public void testTextQuery() {
//    String sql = "select w.dns, w.ip from mydb.w w where text ='coffee shop'";
//    queryAndOutput(sql);
//  }
//
//  // 18 ?
//  public void testCount() {
//    String sql = "select ip.src, ip.proto, count(ip.src) from mydb.ip3 ip group by ip.src, ip.proto ";
//    queryAndOutput(sql);
//  }
//
//  // 19 ?
//  public void testCount2() {
//    String sql = "select count(ip.src) count from mydb.ip3 ip";
//    queryAndOutput(sql);
//  }
//
//  // 20
//  public void testSelectAll() {
//    String sql = "select ip.* from mydb.ip3 ip ";
//    queryAndOutput(sql);
//  }
//
//  protected void outputRecordSet(RecordSet recordSet) {
//    RecordSetDefinition recordSetDefinition = recordSet
//        .getRecordSetDefinition();
//    StringBuffer sbuf = new StringBuffer();
//    for (ColumnDefinition column : recordSetDefinition.getColumns()) {
//      sbuf.append(column.getName());
//      sbuf.append("    ");
//    }
//    System.out.println(sbuf.toString());
//    for (Object[] record : recordSet.getRecords()) {
//      StringBuffer sb = new StringBuffer();
//      for (int i = 0; i < record.length; i++) {
//        if (record[i] != null) {
//          sb.append(record[i].toString());
//        } else {
//          sb.append("NULL");
//        }
//        sb.append(" ");
//      }
//      System.out.println(sb.toString());
//    }
//    System.out.println("------------------------------------------------");
//  }
//
//  protected void queryAndOutput(String sql) {
//    try {
//      RecordSet recordSet = dataQuerier.query(sql);
//      outputRecordSet(recordSet);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }

}
