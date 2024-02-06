import org.datayoo.moql.RecordSet;
import org.datayoo.moql.querier.es.EsDataQuerier;

import java.io.IOException;

public class EsDataQuerierTest{
  public static void main(String[] args) throws IOException {
    String sql = "select t.id from web t where knn(t.image_vector, '[0.3, 0.1, 1.2]', 10 ,100)";
    EsDataQuerier esDataQuerier = new EsDataQuerier();
    RecordSet recordSet = esDataQuerier.query(sql);

    System.out.printf(recordSet.toString());
  }
}
