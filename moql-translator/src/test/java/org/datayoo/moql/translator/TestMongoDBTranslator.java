package org.datayoo.moql.translator;

import junit.framework.TestCase;
import org.datayoo.moql.MoqlException;
import org.datayoo.moql.sql.SqlDialectType;
import org.datayoo.moql.translator.query.Process;

public class TestMongoDBTranslator extends TestCase {
  // 1
  public void testSimpleQuery() {
    String sql = "select w.item, w.name from db.web w";
    testMongoDialect(sql);
  }

  // 2
  public void testConditionQuery() {
    String sql = "select u.name, u.age from users u where name='joe' and age=27";
    testMongoDialect(sql);
  }

  // 3
  public void testConditionQuery2() {
    String sql =
        "select w.dns, w.ip from web w where (w.port=443 or w.port=8080) and "
            + "w.ip='127.0.0.1' or w.ip='127.0.0.2'";
    testMongoDialect(sql);
  }

  // 4
  public void testGroupOrder() {
    String sql = "select ip.src, ip.proto, max(ip.sport) sport, min(ip.sport) from ip3 ip group by ip.src, ip.proto order by ip.src desc limit 2 ";
    testMongoDialect(sql);
  }

  // 5
  public void testHaving() {
    String sql = "select ip.src, ip.proto, max(ip.sport) sport, min(ip.sport) from ip3 ip where ip.ip is not null group by ip.src, ip.proto having ip.proto < 1000";
    testMongoDialect(sql);
  }

  // 6
  public void testNEConditionQuery() {
    String sql = "select w.dns, w.ip  from web w where w.port<>443";
    testMongoDialect(sql);
  }

  public void testDistinctQuery() {
    // unsupport distinct
    //    String sql = "select distinct w.port from web w where w.port<1000";
    //    testMongoDialect(sql);
  }

  // 7
  public void testNotEqualQuery() {
    String sql = "select w.dns, w.ip from web w where not w.port=443 or w.port = 88";
    testMongoDialect(sql);
  }

  // 8 nin
  public void testNotInQuery() {
    String sql = "select w.dns, w.ip from web w where not w.country in ('美国','澳大利亚')";
    testMongoDialect(sql);
  }

  // 9 nor
  public void testNotAndQuery() {
    String sql = "select w.dns, w.ip from web w where not (w.port=443 and w.country in ('美国','澳大利亚'))";
    testMongoDialect(sql);
  }

  // 10
  public void testAndOrConditionQuery() {
    String sql = "select w.dns, w.ip from web w where w.port=443 and w.country in ('美国','澳大利亚') or w.port = 88 and w.ip_num between 10000 and 1900000000";
    testMongoDialect(sql);
  }

  // 11
  public void testLikeConditionQuery() {
    String sql = "select w.dns, w.ip from web w where w.country like '美%'";
    testMongoDialect(sql);
  }

  // 12
  public void testIsConditionQuery() {
    String sql = "select w.dns, w.ip from web w where w.region is null";
    testMongoDialect(sql);
    sql = "select w.dns, w.ip  from web w where w.region is not null";
    testMongoDialect(sql);
  }

  // 13
  public void testParenConditionQuery() {
    String sql = "select w.dns, w.ip  from web w where w.region is null and (w.port = 88 or w.port = 443)";
    testMongoDialect(sql);
  }

  // 14
  public void testLimitQuery() {
    String sql = "select w.dns, w.ip  from web w where w.port<>443 limit 10,20";
    testMongoDialect(sql);
  }

  // 15
  public void testLeftjoinQuery() {
    String sql = "select w.dns, w.ip from web w left join jmr j on j.dns = w.dns where w.port<>443 limit 10,20";
    testMongoDialect(sql);
  }

  // 16
  public void testDateQuery() {
    String sql = "select w.dns, w.ip from web w where w.createTime > ISODate('2019-09-18T09:54:00.000Z')";
    testMongoDialect(sql);
  }

  // 17
  public void testTextQuery() {
    String sql = "select w.dns, w.ip from web w where text('coffee shop')";
    testMongoDialect(sql);
  }

  // 18 new
  public void testCount() {
    String sql = "select ip.src, ip.proto, count(ip.src) from ip3 ip group by ip.src, ip.proto ";
    testMongoDialect(sql);
  }

  // 19
  public void testCount2() {
    String sql = "select count(ip.src) from ip3 ip";
    testMongoDialect(sql);
  }

  // 20
  public void testSelectAll() {
    String sql = "select ip.* from ip3 ip ";
    testMongoDialect(sql);
  }

  protected void testMongoDialect(String sql) {
    try {
      String mongodb = MoqlTranslator
          .translateMoql2Dialect(sql, SqlDialectType.MONGODB);
      mongodb = mongodb.trim();
      System.out.println(mongodb);
      Process process = new Process(mongodb);
      String mongoQuery = process.process();
      System.out.println(mongoQuery);
    } catch (MoqlException e) {
      e.printStackTrace();
    }
  }

}
