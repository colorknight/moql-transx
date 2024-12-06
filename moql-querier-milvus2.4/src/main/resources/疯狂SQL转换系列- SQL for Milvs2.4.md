​	鉴于Milvus仍在不停的迭代新版本，推出新功能，其SDK目前并不稳定。目前其2.4版本的SDK接口已与之前的2.2版本有了较大的差别，功能上也有了一定的调整。为此，我们重新提供了针对[Milvus2.4](https://github.com/colorknight/moql-transx/tree/master/moql-querier-milvus2.4)版本的语法转换功能。由于Milvus文档有些内容写的不是特别详实，亦或时间仓促，我们没有正确理解其功能含义，转换工作可能存在一些问题，还请发现问题的朋友多多指出。

​	我们选择Milvus2.4版本进行语法支持的主要原因是，2.4版本是Milvus目前最新的版本，该版本引入了可以建立多个向量索引的能力。在微软推出的[GraphRAG](https://github.com/microsoft/graphrag)技术中，用到了多向量检索的技术，而[Milvus2.2](https://github.com/colorknight/moql-transx/tree/master/moql-querier-milvus)版只支持单向量，无法复刻GraphRAG的技术理念。为此，我们选中了2.4版本进行语法支持。对于未来Milvus版本发生改变，我们是否能及时支持，主要取决于我们在[HuggingFists](https://github.com/Datayoo/HuggingFists)项目中是否会用到这样的技术特性。本次升级的主要动力来自于我们希望使用HuggingFists采用低代码方式来实践GraphRAG项目中的技术理念。当然，如果有朋友对未来某个版本有语法转换需求也可以给我们提出需求，我们会尽量满足。

​	言归正传，为了能让使用者以类似访问关系数据库的交互体验访问Milvus向量数据库。MOQL Transx继续秉承能SQL化检索数据库就SQL化检索数据库的宗旨。为用户提供了一套可以检索Milvus2.4向量数据库的SQL语法，并提供了检索接口。使用者可通过该接口输入SQL语句，获得结构化的数据结果，如下列代码示例：

```
// 构建Milvus客户端
String url = String.format("%s://%s:%d", "http" ,"datayoo05", 19530);
ConnectConfig connectConfig = ConnectConfig.builder().uri(url).build();
milvusClient = new MilvusClientV2(connectConfig);
// 装载Collection
LoadCollectionReq loadCollectionReq = LoadCollectionReq.builder().collectionName("hybrid_search_collection").build();
milvusClient.loadCollection(loadCollectionReq);
// 使用Milvus客户端创建Milvus查询器
MilvusQuerier milvusQuerier = new MilvusQuerier(milvusClient);
String sql = "select * from hybrid_search_collection a, "
// 用子检索语句检索向量字段
+ "(select * from hybrid_search_collection where vmatch(dense, 'L2', '[[1.0, 2.0, 3.0],[1.1,2.1,3.1]]')) b, "
// 用子检索语句检索稀疏向量
+ "(select * from hybrid_search_collection where vmatch(sparse, 'IP', '[{\"2\":0.1764169}, {\"5\":0.1764169}]')) c limit 5";
// 检索Collection
RecordSet recordSet = milvusQuerier.query(sql);
```

​	Milvus2.4的SDK相较Milvus2.2有了不少改变，主要是方法参数上的变动比较大。比如Milvus2.2中SearchParam在Milvus2.4中变成了SearchReq。但SDK的整体思路没有太大的变化，我们在这里就不再赘述了。下表将给出Milvus2.4查询接口的参数与SQL语法的对照关系，其中的粗体为Milvus2.4新增的检索能力，删除线标记的语法部分在Milvus2.2中有效，由于未在2.4的文档中找到相应的说明，故标为删除：

| Milvus查询参数接口                  | SQL语法                                               |
| ----------------------------------- | ----------------------------------------------------- |
| collectionName(table)               | from table                                            |
| outFields(outFields)                | select outFields                                      |
| filter(expr)                        | where expr                                            |
| annsField, params:metric_type, data | vMatch(fieldName, metricType, vectors)                |
| consistencyLevel                    | consistencyLevel(['STRONG'\|'BOUNDED'\|'Eventually']) |
| topK(k)                             | limit offset, k                                       |
| offset(o)                           | limit offset, k                                       |
| ~~params:nProbe~~                   | ~~nProbe(long)~~                                      |
| ~~params:ef~~                       | ~~ef(long)~~                                          |
| ~~params:search_k~~                 | ~~searchK(long)~~                                     |
| partitionNames                      | partitionBy(String[])                                 |
| roundDecimal                        | roundDecimal(int)                                     |
| travelTimestamp                     | travelTimestamp(long)                                 |
| guaranteeTimestamp                  | guaranteeTimestamp(long)                              |
| **range**                           | searchRange(metricType, radius, rangeFilter)          |
| **group**                           | group by fieldName                                    |
| **dropRatio**                       | dropRatioSearch(dropRatio)                            |
| **searchRequests**                  | from table, **[queryExpression]{0,10}**               |
| **ranker**                          | rrfRanker(k), weightedRanker(float[])                 |

( 注：queryExpression是一个表示向量检索的子SQL语句)
