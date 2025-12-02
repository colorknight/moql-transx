import junit.framework.TestCase;
import org.apache.http.HttpHost;
import org.datayoo.moql.RecordSet;
import org.datayoo.moql.querier.es.EsDataQuerier;
import org.elasticsearch.client.RestClient;

import java.io.IOException;

public class EsDataQuerierTest extends TestCase {
  public void testKnn() throws IOException {
    String sql = "select t.id from web t where knn(t.image_vector, '[0.3, 0.1, 1.2]', 10 ,100)";
    EsDataQuerier esDataQuerier = new EsDataQuerier();
    RecordSet recordSet = esDataQuerier.query(sql);

    System.out.printf(recordSet.toString());
  }

  public void testCount() throws IOException {
    EsDataQuerier esDataQuerier = new EsDataQuerier();
    HttpHost httpHosts = new HttpHost("192.168.0.204", 9200);
    RestClient restClient = RestClient.builder(httpHosts).build();
    esDataQuerier.bind(restClient);

    RecordSet recordSet = esDataQuerier.query(
        "select count(1) as cnt from aircraft");
    restClient.close();
  }

  public void testCountAndSum() throws IOException {
    EsDataQuerier esDataQuerier = new EsDataQuerier();
    HttpHost httpHosts = new HttpHost("192.168.0.204", 9200);
    RestClient restClient = RestClient.builder(httpHosts).build();
    esDataQuerier.bind(restClient);

    RecordSet recordSet = esDataQuerier.query(
        "select count(1) as cnt, sum(CSCOUNT) as s1 from aircraft");
    restClient.close();
    System.out.printf(recordSet.toString());
  }
}
