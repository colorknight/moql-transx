package org.datayoo.moql.querier.mongodb;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.client.*;
import org.apache.commons.lang3.Validate;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.datayoo.moql.ColumnDefinition;
import org.datayoo.moql.MoqlException;
import org.datayoo.moql.RecordSet;
import org.datayoo.moql.SelectorDefinition;
import org.datayoo.moql.core.RecordSetImpl;
import org.datayoo.moql.core.RecordSetMetadata;
import org.datayoo.moql.metadata.ColumnMetadata;
import org.datayoo.moql.metadata.SelectorMetadata;
import org.datayoo.moql.operand.OperandFactory;
import org.datayoo.moql.operand.factory.OperandFactoryImpl;
import org.datayoo.moql.parser.MoqlParser;
import org.datayoo.moql.querier.DataQuerier;
import org.datayoo.moql.querier.SupplementReader;
import org.datayoo.moql.querier.util.SelectorDefinitionUtils;
import org.datayoo.moql.sql.SqlDialectType;
import org.datayoo.moql.sql.mongodb.MongoDBTranslator;
import org.datayoo.moql.translator.MoqlTranslator;
import org.datayoo.moql.util.StringFormater;

import java.io.IOException;
import java.util.*;

public class MongodbQuerier implements DataQuerier {

  public static String PROP_MONGO_PORT = "mongodb.port";
  public static String PROP_MONGO_DATABASE = "mongodb.database";
  public static String PROP_MONGO_SVC_URL = "mongodb.serviceUrl";

  public static String DEFAULT_MONGO_SVC_URL = "mongodb://%s:%p/?ssh=true";

  protected MongoClient mongoClient;

  protected String mongoServiceUrl;

  protected OperandFactory operandFactory = new OperandFactoryImpl();

  @Override
  public void connect(String[] serverIps, Properties properties)
      throws IOException {
    if (mongoClient != null)
      return;
    Validate.notEmpty(serverIps, "serverIps is empty!");
    int port = 27017;
    if (properties != null) {
      Object obj = properties.get(PROP_MONGO_PORT);
      if (obj != null)
        port = Integer.valueOf(obj.toString());
    }
    mongoServiceUrl = properties.getProperty(PROP_MONGO_SVC_URL);
    if (mongoServiceUrl == null) {
      mongoServiceUrl = StringFormater
          .format("mongodb://{}:{}/?ssh=true", serverIps[0], port);
    }
    mongoClient = MongoClients.create(mongoServiceUrl);
  }

  @Override
  public void disconnect() throws IOException {
    if (mongoClient == null)
      return;
    try {
      mongoClient.close();
    } catch (Throwable t) {
      mongoClient = null;
    }
  }

  @Override
  public RecordSet query(String sql) throws IOException {
    return query(sql, (Properties) null, null);
  }

  @Override
  public RecordSet query(String sql, Properties queryProps) throws IOException {
    return query(sql, queryProps, null);
  }

  @Override
  public RecordSet query(String sql, SupplementReader supplementReader)
      throws IOException {
    return query(sql, null, supplementReader);
  }

  @Override
  public RecordSet query(String sql, Properties queryProps,
      SupplementReader supplementReader) throws IOException {
    Validate.notEmpty(sql, "sql is empty!");
    if (queryProps == null) {
      queryProps = new Properties();
    }
    try {
      SelectorDefinition selectorDefinition = MoqlParser.parseMoql(sql);
      String query = MoqlTranslator
          .translateMetadata2Sql(selectorDefinition, SqlDialectType.MONGODB);
      Map<String, JsonElement> clauseMap = parse2ClauseMap(query);
      JsonElement je = clauseMap.remove(MongoDBTranslator.JE_QUERY_COLLECTION);
      String t = je.getAsString();
      int index = t.indexOf('.');
      String database = t.substring(0, index);
      String table = t.substring(index + 1);
      MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
      MongoCollection mongoCollection = mongoDatabase.getCollection(table);
      je = clauseMap.remove(MongoDBTranslator.JE_QUERY_TYPE);
      String queryType = je.getAsString();
      if (queryType.equals("find")) {
        return query(mongoCollection, clauseMap, selectorDefinition);
      } else {
        return aggregate(mongoCollection, clauseMap, selectorDefinition);
      }
    } catch (MoqlException e) {
      throw new IOException("Parse failed!", e);
    }
  }

  protected Map<String, JsonElement> parse2ClauseMap(String query) {
    JsonArray ja = (JsonArray) new JsonParser().parse(query);
    Map<String, JsonElement> clauseMap = new HashMap();
    String queryType = null;
    String[] tableSegs = null;
    for (int i = 0; i < ja.size(); i++) {
      JsonObject jo = (JsonObject) ja.get(i);
      for (Map.Entry<String, JsonElement> entry : jo.entrySet()) {
        clauseMap.put(entry.getKey(), entry.getValue());
      }
    }
    return clauseMap;
  }

  protected RecordSet query(MongoCollection mongoCollection,
      Map<String, JsonElement> clauseMap,
      SelectorDefinition selectorDefinition) {
    JsonElement je = clauseMap.get("$match");
    FindIterable findIterable;
    if (je != null) {
      findIterable = mongoCollection.find(toBson(je));
    } else {
      findIterable = mongoCollection.find();
    }
    je = clauseMap.get("$project");
    if (je != null) {
      findIterable = findIterable.projection(toBson(je));
    }
    je = clauseMap.get("$sort");
    if (je != null) {
      findIterable = findIterable.sort(toBson(je));
    }
    je = clauseMap.get("$skip");
    if (je != null) {
      findIterable.skip(je.getAsInt());
    }
    je = clauseMap.get("$limit");
    if (je != null) {
      findIterable.limit(je.getAsInt());
    }
    return toRecordSet(findIterable, (SelectorMetadata) selectorDefinition);
  }

  protected Bson toBson(JsonElement jsonElement) {
    return BsonDocument.parse(jsonElement.toString());
  }

  protected RecordSet toRecordSet(MongoIterable mongoIterable,
      SelectorMetadata selectorMetadata) {
    RecordSet recordSet = null;
    List<ColumnDefinition> columnDefinitions = null;
    if (selectorMetadata.getColumns().getColumns().size() == 1) {
      ColumnMetadata columnMetadata = selectorMetadata.getColumns().getColumns()
          .get(0);
      if (columnMetadata.getValue().indexOf('*') == -1) {
        recordSet = SelectorDefinitionUtils
            .createRecordSetWithoutTablePrefix(selectorMetadata);
        columnDefinitions = recordSet.getRecordSetDefinition().getColumns();
      }
    } else {
      recordSet = SelectorDefinitionUtils
          .createRecordSetWithoutTablePrefix(selectorMetadata);
      columnDefinitions = recordSet.getRecordSetDefinition().getColumns();
    }
    for (Object obj : mongoIterable) {
      Document doc = (Document) obj;
      if (recordSet == null) {
        recordSet = buildRecordSet(doc);
        columnDefinitions = recordSet.getRecordSetDefinition().getColumns();
      }
      recordSet.getRecords().add(toRecord(doc, columnDefinitions));
    }
    return recordSet;
  }

  protected RecordSet buildRecordSet(Document doc) {
    List<ColumnDefinition> columnDefinitions = new LinkedList<>();
    for (Map.Entry<String, Object> entry : doc.entrySet()) {
      ColumnMetadata columnMetadata = new ColumnMetadata(entry.getKey(),
          entry.getKey());
      columnDefinitions.add(columnMetadata);
    }
    RecordSetMetadata recordSetMetadata = new RecordSetMetadata(
        columnDefinitions, null);
    return new RecordSetImpl(recordSetMetadata, new Date(), new Date(),
        new LinkedList<Object[]>());
  }

  protected Object[] toRecord(Document doc,
      List<ColumnDefinition> columnDefinitions) {
    Object[] record = new Object[columnDefinitions.size()];
    int i = 0;
    for (ColumnDefinition columnDefinition : columnDefinitions) {
      record[i++] = doc.get(columnDefinition.getName());
    }
    return record;
  }

  protected RecordSet aggregate(MongoCollection mongoCollection,
      Map<String, JsonElement> clauseMap,
      SelectorDefinition selectorDefinition) {
    JsonArray ja = (JsonArray) clauseMap.get("aggs");
    List<Bson> pipeline = new LinkedList<>();
    for (int i = 0; i < ja.size(); i++) {
      pipeline.add(toBson(ja.get(i)));
    }
    return toRecordSet(mongoCollection.aggregate(pipeline),
        (SelectorMetadata) selectorDefinition);
  }

}
