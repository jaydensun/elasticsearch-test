package com.jayden.estest;

import com.alibaba.fastjson.JSONObject;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author Jayden Sun（089245）
 * @since 2020/1/8
 */
public class App {

    public static final String INDEX_NAME = "jayden-index-test";
    public static final String TYPE = "doc";

    public static void main(String[] args) throws IOException, InterruptedException {
        Settings settings = Settings.builder()
                .put("cluster.name", "es-shunlu-5.4.1").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.203.202.95"), 9300));
        System.out.println(client);

        indexOps(client);

//        System.out.println(client.prepareIndex(INDEX_NAME, TYPE, "3")
//                .setSource(jsonBuilder()
//                        .startObject()
//                        .field("image", "app/deploy")
//                        .field("image3", "a")
//                        .startArray("counts").startObject()
//                        .field("month", 2010)
//                        .field("count", 2)
//                        .endObject().endArray()
//                        .endObject())
//                .get());

        Item item = new Item();
        item.setCategory("手机2");
        item.setCounts(getCounts("2011", 22));
        System.out.println(client.prepareIndex(INDEX_NAME, TYPE, "7")
                .setSource((Map) JSONObject.parse(JSONObject.toJSONString(item)), XContentType.JSON)
                .get());

        System.out.println(client.prepareUpdate(INDEX_NAME, TYPE, "7")
                .setDoc("image", "/app/deploy 2").get());

        System.out.println(client.prepareUpdate(INDEX_NAME, TYPE, "7")
                .setDoc("counts", JSONObject.parse(JSONObject.toJSONString(getCounts("2010", 8, "2011", 3)))).get());

        BoolQueryBuilder monthQueryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("counts.month", "2010"))                ;
        Thread.sleep(2000);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchPhraseQuery("category", "手机2"))
//                .must(QueryBuilders.matchQuery("price", "3498"))
                .must(QueryBuilders.nestedQuery("counts", monthQueryBuilder, ScoreMode.None));
        System.out.println(client.prepareSearch(INDEX_NAME).setQuery(boolQueryBuilder)
                .addSort(SortBuilders.fieldSort("counts.count").order(SortOrder.ASC).setNestedFilter(monthQueryBuilder).setNestedPath("counts"))
                .get());

    }

    private static void indexOps(TransportClient client) throws IOException {
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        System.out.println(indicesAdminClient.prepareDelete(INDEX_NAME).setIndicesOptions(IndicesOptions.fromOptions(true, true
                , true, true)).get());

        System.out.println(indicesAdminClient.prepareCreate(INDEX_NAME).get());
        System.out.println(jsonBuilder()
                .startObject()
                .field("properties")
                .startObject()
                .field("counts")
                .startObject().field("type", "nested")
                .endObject()
                .endObject()
                .endObject().string());
        System.out.println(indicesAdminClient.preparePutMapping(INDEX_NAME)
                .setType(TYPE)
                .setSource(JSONObject.parseObject("{\"dynamic_templates\":[{\"strings_not_analyzed\":{\"mapping\":{\"type\":\"keyword\"},\"match_mapping_type\":\"string\"}}]}", Map.class))
                .get());
        System.out.println(indicesAdminClient.preparePutMapping(INDEX_NAME)
                .setType(TYPE)
                .setSource(JSONObject.parseObject("{\"properties\":{\"counts\":{\"type\":\"nested\"}}}", Map.class))
                .get());
    }

    private static List<ItemSub> getCounts(Object... keyValues) {
        List<ItemSub> map = new ArrayList<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            Object keyValue = keyValues[i];
            map.add(new ItemSub(keyValue.toString(), Integer.parseInt(keyValues[i + 1].toString())));
        }
        return map;
    }
}
