package org.datayoo.moql.querier.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import junit.framework.TestCase;
import org.datayoo.moql.ColumnDefinition;
import org.datayoo.moql.RecordSet;
import org.datayoo.moql.RecordSetDefinition;
import org.datayoo.moql.querier.es.CommonSupplementReader;
import org.datayoo.moql.querier.es.EsDataQuerier;

import java.io.IOException;
import java.util.Properties;

public class MongodbQuerierTest extends TestCase {

  protected MongodbQuerier dataQuerier = new MongodbQuerier();

  @Override
  public void setUp() throws Exception {
    String[] serverIps = new String[] { "172.30.30.8" };
    Properties properties = new Properties();
    dataQuerier.connect(serverIps, properties);
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    dataQuerier.disconnect();
    super.tearDown();
  }

  public void testMongoFind() {
    MongoClient mongoClient = MongoClients
        .create("mongodb://172.30.30.8:27017/?ssh=true");
    MongoDatabase mongoDatabase = mongoClient.getDatabase("bracket");
    MongoCollection mongoCollection = mongoDatabase
        .getCollection("sengeeArchive.files");
    mongoCollection.find().forEach(doc -> System.out.println(doc.toString()));
    mongoClient.close();
  }

  public void testCommonQuery() {
    String sql = "select archive.* from bracket.sengeeArchive.files archive";
    try {
      RecordSet recordSet = dataQuerier.query(sql);
      outputRecordSet(recordSet);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void testProjectQuery() {
    String sql = "select archive.length, archive.filename,  archive.metadata from bracket.sengeeArchive.files archive";
    try {
      RecordSet recordSet = dataQuerier.query(sql);
      outputRecordSet(recordSet);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void testIsQuery() {
    String sql = "select archive.* from bracket.sengeeArchive.files archive where archive.metadata is not null";
    try {
      RecordSet recordSet = dataQuerier.query(sql);
      outputRecordSet(recordSet);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void testSortQuery() {
    String sql = "select archive.* from bracket.sengeeArchive.files archive where archive.metadata is not null order by archive.length desc";
    try {
      RecordSet recordSet = dataQuerier.query(sql);
      outputRecordSet(recordSet);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected void outputRecordSet(RecordSet recordSet) {
    RecordSetDefinition recordSetDefinition = recordSet
        .getRecordSetDefinition();
    StringBuffer sbuf = new StringBuffer();
    for (ColumnDefinition column : recordSetDefinition.getColumns()) {
      sbuf.append(column.getName());
      sbuf.append("    ");
    }
    System.out.println(sbuf.toString());
    for (Object[] record : recordSet.getRecords()) {
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < record.length; i++) {
        if (record[i] != null) {
          sb.append(record[i].toString());
        } else {
          sb.append("NULL");
        }
        sb.append(" ");
      }
      System.out.println(sb.toString());
    }
    System.out.println("------------------------------------------------");
  }
}
