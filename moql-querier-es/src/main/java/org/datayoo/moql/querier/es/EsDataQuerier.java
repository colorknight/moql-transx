package org.datayoo.moql.querier.es;

import com.google.gson.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.datayoo.moql.*;
import org.datayoo.moql.core.RecordSetImpl;
import org.datayoo.moql.metadata.*;
import org.datayoo.moql.operand.OperandContextArrayList;
import org.datayoo.moql.operand.OperandFactory;
import org.datayoo.moql.operand.factory.OperandFactoryImpl;
import org.datayoo.moql.parser.MoqlParser;
import org.datayoo.moql.querier.DataQuerier;
import org.datayoo.moql.querier.SupplementReader;
import org.datayoo.moql.querier.util.SelectorDefinitionUtils;
import org.datayoo.moql.sql.SqlDialectType;
import org.datayoo.moql.translator.MoqlTranslator;
import org.datayoo.moql.util.StringFormater;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.*;

public class EsDataQuerier implements DataQuerier {

  public static String PROP_HTTP_PORT = "elasticsearch.port";
  public static String PROP_ES_SVC_URL = "elasticsearch.serviceUrl";

  public static final String COLUMN_INDEX = "_index";
  public static final String COLUMN_ID = "_id";
  public static final String COLUMN_SCOUR = "_score";
  public static final String COLUMN_TYPE = "_type";

  public static String DOC_COUNT = "doc_count";

  public static final String SCHEME = "http";

  public static final String INDEX_NAME_MAPPINGS = "indexNameMappings";

  protected int maxResultWindow = 10000;

  protected RestClient httpClient;

  protected OperandFactory operandFactory = new OperandFactoryImpl();

  public synchronized void bind(RestClient httpClient) {
    Validate.notNull(httpClient, "httpClient is null!");
    this.httpClient = httpClient;
  }

  @Override
  public synchronized void connect(String[] serverIps, Properties properties)
      throws IOException {
    if (httpClient != null)
      return;
    int port = 9200;
    if (properties != null) {
      Object obj = properties.get(PROP_HTTP_PORT);
      if (obj != null)
        port = Integer.valueOf(obj.toString());
    }

    HttpHost[] httpHosts = new HttpHost[serverIps.length];
    for (int i = 0; i < serverIps.length; i++) {
      httpHosts[i] = new HttpHost(serverIps[i], port, SCHEME);
    }

    httpClient = RestClient.builder(httpHosts).build();
  }

  @Override
  public synchronized void disconnect() throws IOException {
    if (httpClient != null) {
      httpClient.close();
      httpClient = null;
    }
  }

  @Override
  public RecordSet query(String sql) throws IOException {
    Validate.notEmpty(sql, "sql is empty!");
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
    sql = sql.replaceAll("`", StringUtils.EMPTY);
    if (queryProps == null) {
      queryProps = new Properties();
    }

    try {
      SelectorDefinition selectorDefinition = MoqlParser.parseMoql(sql);
      List<String> indexAndTables = getIndexAndTables(selectorDefinition);
      // 转换特殊符号 *
      transformSelectorDefinition(selectorDefinition, indexAndTables,
          queryProps);
      String query = MoqlTranslator.translateMetadata2Sql(selectorDefinition,
          SqlDialectType.ELASTICSEARCH);
      String queryUrl = makeQueryUrl(indexAndTables, queryProps);
      Response response = query(queryUrl, query);
      String data = EntityUtils.toString(response.getEntity());
      return toRecordSet(data, selectorDefinition, supplementReader);
    } catch (MoqlException e) {
      throw new IOException("Parse failed!", e);
    }
  }

  protected void transformSelectorDefinition(
      SelectorDefinition selectorDefinition, List<String> indexAndTables,
      Properties properties) throws IOException {
    SelectorMetadata metadata = (SelectorMetadata) selectorDefinition;
    ColumnsMetadata columnsMetadata = metadata.getColumns();
    List<ColumnMetadata> columns = columnsMetadata.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getName().endsWith("*")) {
        columns.remove(i);
        Properties indexNameMappings = (Properties) properties.get(
            INDEX_NAME_MAPPINGS);
        String indexName;
        if (indexNameMappings == null) {
          indexName = indexAndTables.get(0);
        } else {
          indexName = indexNameMappings.getProperty(indexAndTables.get(0));
        }
        List<ColumnMetadata> indexColumnsMetadata = getIndexColumnsMetadata(
            indexName);
        for (int j = indexColumnsMetadata.size() - 1; j >= 0; j--) {
          ColumnMetadata columnMetadata = indexColumnsMetadata.get(j);
          columns.add(i, columnMetadata);
        }
      }
    }
  }

  private List<ColumnMetadata> getIndexColumnsMetadata(String indexName)
      throws IOException {
    String[] indexNameArray = StringUtils.split(indexName, ",");
    List<String> columnNames = new ArrayList<>();
    for (String index : indexNameArray) {
      Request request = new Request("GET", index);
      Response response = httpClient.performRequest(request);
      String data = EntityUtils.toString(response.getEntity());
      JsonParser jsonParser = new JsonParser();
      JsonObject root = (JsonObject) jsonParser.parse(data);
      JsonObject properties = root.get(index).getAsJsonObject().get("mappings")
          .getAsJsonObject().get("properties").getAsJsonObject();

      properties.entrySet();
      for (Map.Entry<String, JsonElement> map : properties.entrySet()) {
        String name = map.getKey();
        if (!columnNames.contains(name)) {
          columnNames.add(name);
        }
      }
    }
    List<ColumnMetadata> columns = new ArrayList<>(columnNames.size());
    columns.add(new ColumnMetadata(COLUMN_INDEX, COLUMN_INDEX));
    columns.add(new ColumnMetadata(COLUMN_ID, COLUMN_ID));
    columns.add(new ColumnMetadata(COLUMN_TYPE, COLUMN_TYPE));
    columns.add(new ColumnMetadata(COLUMN_SCOUR, COLUMN_SCOUR));
    for (String column : columnNames) {
      columns.add(new ColumnMetadata(column, column));
    }

    return columns;
  }

  protected List<String> getIndexAndTables(
      SelectorDefinition selectorDefinition) {
    SelectorMetadata metadata = (SelectorMetadata) selectorDefinition;
    TablesMetadata tablesMetadata = metadata.getTables();
    List<String> indexAndTables = new LinkedList<String>();
    String index = null;
    for (QueryableMetadata qm : tablesMetadata.getTables()) {
      if (!(qm instanceof TableMetadata)) {
        throw new UnsupportedOperationException("Unsupported join operator!");
      }
      TableMetadata tableMetadata = (TableMetadata) qm;
      String[] segs = tableMetadata.getValue().split(".");
      if (segs.length == 0) {
        segs = new String[] { tableMetadata.getValue() };
      }
      if (indexAndTables.size() == 0) {
        indexAndTables.add(segs[0]);
        index = segs[0];
      } else {
        if (!index.equals(segs[0])) {
          throw new IllegalArgumentException(
              "Index of doc type are different!");
        }
      }
      if (segs.length != 1) {
        indexAndTables.add(segs[1]);
      }
    }
    return indexAndTables;
  }

  protected String makeQueryUrl(List<String> indexAndTables,
      Properties queryProps) {
    StringBuffer sbuf = new StringBuffer();
    sbuf.append("/");
    Properties indexNameMappings = (Properties) queryProps.get(
        INDEX_NAME_MAPPINGS);
    if (indexNameMappings != null) {
      String indexName = indexNameMappings.getProperty(indexAndTables.get(0));
      if (indexName == null)
        indexName = indexAndTables.get(0);
      sbuf.append(indexName);
    } else {
      sbuf.append(indexAndTables.get(0));
    }
    sbuf.append("/");
    if (indexAndTables.size() > 1) {
      for (int i = 1; i < indexAndTables.size(); i++) {
        if (i != 1) {
          sbuf.append(",");
        }
        String indexName = indexNameMappings.getProperty(indexAndTables.get(i));
        if (indexName == null)
          indexName = indexAndTables.get(i);
        sbuf.append(indexName);
      }
      sbuf.append("/");
    }
    sbuf.append("_search?pretty");
    queryProps.remove(INDEX_NAME_MAPPINGS);
    assembleUrlProperties(sbuf, queryProps);
    if (indexNameMappings != null)
      queryProps.put(INDEX_NAME_MAPPINGS, indexNameMappings);
    return sbuf.toString();
  }

  protected void assembleUrlProperties(StringBuffer sbuf,
      Properties queryProps) {
    for (Map.Entry<Object, Object> entry : queryProps.entrySet()) {
      sbuf.append("&");
      sbuf.append(entry.getKey());
      sbuf.append("=");
      sbuf.append(entry.getValue());
    }
  }

  protected Response query(String queryUrl, String query) throws IOException {
    //    HttpPost httpPost = new HttpPost(queryUrl);
    //    StringEntity entity = new StringEntity(query, "utf-8");//解决中文乱码问题
    //    entity.setContentEncoding("UTF-8");
    //    entity.setContentType("application/json");
    //    httpPost.setEntity(entity);
    //    HttpResponse response = httpClient.execute(httpPost);
    //    int status = response.getStatusLine().getStatusCode();
    //    if (status >= 200 && status < 300) {
    //      return response;
    //    } else {
    //      throw new ClientProtocolException(
    //          "Unexpected response status: " + status);
    //    }

    HttpEntity entity = new NStringEntity(query, ContentType.APPLICATION_JSON);
    Request request = new Request("GET", queryUrl);
    request.setEntity(entity);

    Response response = httpClient.performRequest(request);

    return response;
  }

  protected RecordSet toRecordSet(String data,
      SelectorDefinition selectorDefinition,
      SupplementReader supplementReader) {
    JsonParser jsonParser = new JsonParser();
    JsonObject root = (JsonObject) jsonParser.parse(data);
    if (supplementReader != null)
      supplementReader.read(root);
    JsonObject aggHits = (JsonObject) root.get("aggregations");
    if (aggHits != null) {
      return toAggregationRecordSet(aggHits, selectorDefinition);
    } else {
      JsonObject hits = (JsonObject) root.get("hits");
      return toQueryRecordSet(hits, selectorDefinition);
    }
  }

  protected RecordSet toQueryRecordSet(JsonObject jsonObject,
      SelectorDefinition selectorDefinition) {
    RecordSetImpl recordSet = SelectorDefinitionUtils.createRecordSet(
        selectorDefinition);
    Operand[] operands = buildColumnOperands(selectorDefinition);
    JsonArray hitArray = jsonObject.getAsJsonArray("hits");
    List<Object[]> records = recordSet.getRecords();
    for (int i = 0; i < hitArray.size(); i++) {
      JsonObject jo = (JsonObject) hitArray.get(i);
      EntityMap entityMap = toQueryEntityMap(jo);
      Object[] record = toRecord(operands, entityMap);
      records.add(record);
    }
    return recordSet;
  }

  protected RecordSet toAggregationRecordSet(JsonObject jsonObject,
      SelectorDefinition selectorDefinition) {
    RecordSetImpl recordSet = SelectorDefinitionUtils.createRecordSet(
        selectorDefinition);
    Operand[] operands = buildColumnOperands(selectorDefinition);

    List<Object[]> records = recordSet.getRecords();
    List<EntityMap> entityMaps = toAggregationEntityMaps(jsonObject,
        recordSet.getRecordSetDefinition().getGroupColumns());
    for (EntityMap entityMap : entityMaps) {
      Object[] record = toRecord(operands, entityMap);
      records.add(record);
    }
    return recordSet;
  }

  protected Operand[] buildColumnOperands(
      SelectorDefinition selectorDefinition) {
    SelectorMetadata selectorMetadata = (SelectorMetadata) selectorDefinition;
    Operand[] operands = new Operand[selectorMetadata.getColumns().getColumns()
        .size()];
    int i = 0;
    for (ColumnMetadata columnMetadata : selectorMetadata.getColumns()
        .getColumns()) {
      String value = columnMetadata.getValue();
      int index = value.indexOf('(');
      if (index == -1) {
        //        index = value.indexOf('.');
        //        value = value.substring(index + 1);
      } else {
        value = getAggregationFunctionExpression(columnMetadata);
      }
      try {
        if (value.contains("-")) {
          value = "`" + value + "`";
        }
        operands[i++] = operandFactory.createOperand(value);
      } catch (MoqlException e) {
        throw new IllegalArgumentException(
            StringFormater.format("Invalid column value '{}'!",
                columnMetadata.getValue()));
      }
    }
    return operands;
  }

  protected String getAggregationFunctionExpression(
      ColumnMetadata columnMetadata) {
    int index = columnMetadata.getValue().indexOf("count(");
    if (index != -1) {
      index = columnMetadata.getValue().indexOf("true");
      if (index == -1)
        return DOC_COUNT;
    }
    return columnMetadata.getName();
  }

  protected EntityMap toQueryEntityMap(JsonObject jsonObject) {
    Map<String, Object> record = new HashMap<String, Object>();
    toMap(jsonObject, record);
    return new EntityMapImpl(record);
  }

  protected void toMap(JsonObject jsonObject, Map<String, Object> record) {
    for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
      if (entry.getKey().equals("_source")) {
        toMap((JsonObject) entry.getValue(), record);
        continue;
      }
      JsonElement je = entry.getValue();
      record.put(entry.getKey(), getObject(je));
    }
  }

  protected Object getObject(JsonElement je) {
    if (je instanceof JsonObject) {
      return toMap((JsonObject) je);
    } else if (je instanceof JsonArray) {
      return toList((JsonArray) je);
    } else if (je instanceof JsonPrimitive) {
      return getValue((JsonPrimitive) je);
    }
    return null;
  }

  protected Map<String, Object> toMap(JsonObject jo) {
    Map<String, Object> map = new HashMap<>();
    for (Map.Entry<String, JsonElement> entry : jo.entrySet()) {
      JsonElement je = entry.getValue();
      map.put(entry.getKey(), getObject(je));
    }
    return map;
  }

  protected OperandContextArrayList toList(JsonArray ja) {
    OperandContextArrayList arrayList = new OperandContextArrayList(ja.size());
    for (int i = 0; i < ja.size(); i++) {
      JsonElement je = ja.get(i);
      arrayList.add(getObject(je));
    }
    return arrayList;
  }

  //  protected void toArrayRecord(String prefix, JsonArray array,
  //      Map<String, Object> record) {
  //    // 先把旧的数据集加入结果
  //    record.put(prefix, array.toString());
  //    Map<String, JsonArray> arrayMap = new HashMap<>();
  //    for (JsonElement element : array) {
  //      if (element instanceof JsonObject) {
  //        JsonObject jsonObject = (JsonObject) element;
  //        Set<Map.Entry<String, JsonElement>> set = jsonObject.entrySet();
  //
  //        for (Map.Entry<String, JsonElement> entry : set) {
  //          String key = prefix + "." + entry.getKey();
  //          if (arrayMap.get(key) == null) {
  //            JsonArray jsonArray = new JsonArray();
  //            jsonArray.add(entry.getValue());
  //            arrayMap.put(key, jsonArray);
  //          } else {
  //            JsonArray value = arrayMap.get(key);
  //            value.add(entry.getValue());
  //            arrayMap.put(key, value);
  //          }
  //        }
  //      }
  //      if (element instanceof JsonPrimitive) {
  //        if (record.get(prefix) == null) {
  //          Object value = getValue((JsonPrimitive) element);
  //          record.put(prefix, value);
  //        }
  //
  //      }
  //    }
  //    for (Map.Entry<String, JsonArray> entry : arrayMap.entrySet()) {
  //      toArrayRecord(entry.getKey(), entry.getValue(), record);
  //    }
  //  }

  protected List<EntityMap> toAggregationEntityMaps(JsonObject jsonObject,
      List<ColumnDefinition> groupColumns) {
    List<EntityMap> entityMaps = new LinkedList<EntityMap>();
    Map<String, Object> head = new HashMap<String, Object>();
    String[] groupNodeNames = extractGroupNodeNames(groupColumns);
    toAggregationEntityMaps(jsonObject, entityMaps, head, groupNodeNames, 0);
    return entityMaps;
  }

  protected String[] extractGroupNodeNames(
      List<ColumnDefinition> groupColumns) {
    String[] groupNodeNames = new String[groupColumns.size()];
    int i = 0;
    for (ColumnDefinition columnDefinition : groupColumns) {
      int index = columnDefinition.getValue().indexOf('.');
      groupNodeNames[i++] = columnDefinition.getValue().substring(index + 1);
    }
    return groupNodeNames;
  }

  protected void toAggregationEntityMaps(JsonObject jsonObject,
      List<EntityMap> entityMaps, Map<String, Object> head,
      String[] groupNodeNames, int offset) {
    if (groupNodeNames.length - 1 == offset) {
      toAggretaionEntityMaps(jsonObject, entityMaps, head,
          groupNodeNames[offset]);
    } else {
      JsonObject groupNode = (JsonObject) jsonObject.get(
          groupNodeNames[offset]);
      JsonArray jsonArray = (JsonArray) groupNode.get("buckets");
      for (int i = 0; i < jsonArray.size(); i++) {
        JsonObject jo = (JsonObject) jsonArray.get(i);
        head.put(groupNodeNames[offset], jo.get("key").getAsString());
        toAggregationEntityMaps(jo, entityMaps, head, groupNodeNames,
            offset + 1);
      }
    }
  }

  protected void toAggretaionEntityMaps(JsonObject jsonObject,
      List<EntityMap> entityMaps, Map<String, Object> head, String key) {
    JsonObject groupNode = (JsonObject) jsonObject.get(key);
    JsonArray jsonArray = (JsonArray) groupNode.get("buckets");
    for (int i = 0; i < jsonArray.size(); i++) {
      JsonObject jo = (JsonObject) jsonArray.get(i);
      Map<String, Object> map = toMap(jo, head, key);
      EntityMap entityMap = new EntityMapImpl(map);
      entityMaps.add(entityMap);
    }
  }

  protected Map<String, Object> toMap(JsonObject jsonObject,
      Map<String, Object> head, String key) {
    Map<String, Object> map = new HashMap<String, Object>(head);
    for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
      if (entry.getKey().equals("key")) {
        map.put(key, entry.getValue().getAsString());
        continue;
      }
      if (entry.getKey().equals(DOC_COUNT)) {
        map.put(DOC_COUNT, entry.getValue().getAsInt());
        continue;
      }
      JsonPrimitive value;
      if (entry.getValue() instanceof JsonObject) {
        value = ((JsonObject) entry.getValue()).getAsJsonPrimitive("value");
      } else {
        value = (JsonPrimitive) entry.getValue();
      }
      map.put(entry.getKey(), getValue(value));
    }
    return map;
  }

  protected Object getValue(JsonPrimitive value) {
    if (value.isNumber()) {
      Number number = value.getAsNumber();
      double d = number.doubleValue();
      long l = number.longValue();
      if (d - l > 0) {
        return d;
      } else {
        return l;
      }
    } else if (value.isBoolean())
      return value.getAsBoolean();
    else
      return value.getAsString();
  }

  protected Object[] toRecord(Operand[] operands, EntityMap entityMap) {
    Object[] record = new Object[operands.length];
    for (int i = 0; i < operands.length; i++) {
      record[i] = operands[i].operate(entityMap);
    }
    return record;
  }

  public int getMaxResultWindow() {
    return maxResultWindow;
  }

  public void setMaxResultWindow(int maxResultWindow) {
    this.maxResultWindow = maxResultWindow;
  }
}
