package org.datayoo.moql.translator;

import junit.framework.TestCase;
import org.datayoo.moql.MoqlException;
import org.datayoo.moql.sql.SqlDialectType;
import org.datayoo.moql.translator.query.Process;

/**
 * @author tangtadin
 * @version 1.0
 * @description: TODO
 * @date 2022/2/23 9:38 AM
 */
public class TestDynamodbTranslator extends TestCase {

  // 1
  public void testSimpleQuery() {
    String sql = "select w.* from web.index w";
    testDynamodbDialect(sql);
  }

  // 2
  public void testConditionQuery() {
    String sql = "select u.name, u.age from users u where u.name='joe' and age=27";
    testDynamodbDialect(sql);
  }

  // 3
  public void testConditionQuery2() {
    String sql =
        "select w.dns, w.ip from web w where (w.port=443 or w.port=8080) and "
            + "w.ip='127.0.0.1' or w.ip='127.0.0.2'";
    testDynamodbDialect(sql);
  }

  // 4
  public void testGroupOrder() {
    String sql = "select ip.src, ip.proto, max(ip.sport) sport, min(ip.sport) from ip3 ip group by ip.src, ip.proto order by ip.src desc limit 2 ";
    // throw exception
    testDynamodbDialect(sql);
  }

  // 7
  public void testNotEqualQuery() {
    String sql = "select w.dns, w.ip from web w where not w.port=443 or w.port = 88";
    testDynamodbDialect(sql);
  }

  // 8 nin
  public void testNotInQuery() {
    String sql = "select w.dns, w.ip from web w where not w.country in ('美国','澳大利亚')";
    testDynamodbDialect(sql);
  }

  // 9 nor
  public void testNotAndQuery() {
    String sql = "select w.dns, w.ip from web w where not (w.port=443 and w.country in ('美国','澳大利亚'))";
    testDynamodbDialect(sql);
  }

  // 10
  public void testAndOrConditionQuery() {
    String sql = "select w.dns, w.ip from web w where w.port=443 and w.country in ('美国','澳大利亚') or w.port = 88 and w.ip_num between 10000 and 1900000000";
    testDynamodbDialect(sql);
  }

  // 11
  public void testLikeConditionQuery() {
    String sql = "select w.dns, w.ip from web w where w.country like '美%'";
    testDynamodbDialect(sql);
  }

  // 12
  public void testIsConditionQuery() {
    String sql = "select w.dns, w.ip from web w where w.region is null";
    testDynamodbDialect(sql);
    sql = "select w.dns, w.ip  from web w where w.region is not null";
    testDynamodbDialect(sql);
  }

  // 13
  public void testParenConditionQuery() {
    String sql = "select w.dns, w.ip  from web w where w.region is null and (w.port = 88 or w.port = 443)";
    testDynamodbDialect(sql);
  }

  protected void testDynamodbDialect(String sql) {
    try {
      String partiSql = MoqlTranslator
          .translateMoql2Dialect(sql, SqlDialectType.DYNAMODB);
      partiSql = partiSql.trim();
      System.out.println(partiSql);
    } catch (MoqlException e) {
      e.printStackTrace();
    }
  }
}
