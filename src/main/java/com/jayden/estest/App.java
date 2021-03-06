package com.jayden.estest;

import com.alibaba.fastjson.JSONObject;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

//        indexOps(client);

        System.out.println(client.prepareDelete(INDEX_NAME, TYPE, "1").get());

        Item item = new Item();
        item.setCategory("深圳中国");
        item.setCounts(getCounts("2011", 22));
        String id = "374";
        for (int i = 0; i < 1; i++) {
            id = String.valueOf(Integer.parseInt(id) + 1);
            System.out.println(client.prepareIndex(INDEX_NAME, TYPE, id)
                    .setSource(JSONObject.parseObject(JSONObject.toJSONString(item), Map.class))
                    .get());

//        String nullValue = "image value";
            String nullValue = null;
            System.out.println(client.prepareUpdate(INDEX_NAME, TYPE, id)
                    .setDoc("image", nullValue, "pos", "40.12,-71.34")
                    .get());

            System.out.println(client.prepareUpdate(INDEX_NAME, TYPE, id)
                    .setDoc("counts", JSONObject.parse(JSONObject.toJSONString(getCounts("2012", 5, "2013", 6)))).get());
        }

        BoolQueryBuilder monthQueryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("counts.month", "2010"));
        Thread.sleep(2000);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("category", "手机2"))
//                .must(QueryBuilders.matchQuery("price", "3498"))
                .must(QueryBuilders.nestedQuery("counts", monthQueryBuilder, ScoreMode.None));
        System.out.println(client.prepareSearch(INDEX_NAME).setQuery(boolQueryBuilder)
                .addSort(SortBuilders.fieldSort("counts.count").order(SortOrder.DESC).setNestedFilter(monthQueryBuilder).setNestedPath("counts"))
                .get());

        BoolQueryBuilder distanceQueryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("category", "手机2"))
                .filter(QueryBuilders.geoDistanceQuery("pos").distance(200, DistanceUnit.KILOMETERS).point(40, -70));
        System.out.println(client.prepareSearch(INDEX_NAME).setQuery(distanceQueryBuilder)
                .addSort(SortBuilders.geoDistanceSort("pos", 40, -70).order(SortOrder.DESC))
                .addSort(SortBuilders.fieldSort("_uid").order(SortOrder.ASC))
                .setFrom(0).setSize(5)
                .get());

        SearchResponse searchResponse = client.prepareSearch(INDEX_NAME).setQuery(QueryBuilders.nestedQuery("counts", QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("counts.count", 2)), ScoreMode.None))
                .addAggregation(AggregationBuilders.filter("name1", QueryBuilders.nestedQuery("counts", QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("counts.month", "2010")).must(QueryBuilders.matchQuery("counts.count", 0)), ScoreMode.None)))
                .addAggregation(AggregationBuilders.filter("name3", QueryBuilders.nestedQuery("counts", QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("counts.month", "2011")).must(QueryBuilders.matchQuery("counts.count", 3)), ScoreMode.None)))
                .addAggregation(AggregationBuilders.filter("name5", QueryBuilders.nestedQuery("counts", QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("counts.month", "2020")).must(QueryBuilders.matchQuery("counts.count", 3)), ScoreMode.None)))
                .setFrom(0).setSize(0)
                .get();
        System.out.println(searchResponse);
        System.out.println(searchResponse.getAggregations().getAsMap());
        System.out.println(searchResponse.getAggregations().asMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, x -> ((Filter)x.getValue()).getDocCount())));

    }

    private static void indexOps(TransportClient client) throws IOException {
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        System.out.println(indicesAdminClient.prepareDelete(INDEX_NAME).setIndicesOptions(IndicesOptions.fromOptions(true, true
                , true, true)).get());

        System.out.println(indicesAdminClient.prepareCreate(INDEX_NAME).get());
        System.out.println(indicesAdminClient.preparePutMapping(INDEX_NAME)
                .setType(TYPE)
                .setSource(JSONObject.parseObject("{\"dynamic_templates\":[{\"strings_not_analyzed\":{\"mapping\":{\"type\":\"keyword\"},\"match_mapping_type\":\"string\"}}]}", Map.class))
                .get());
        System.out.println(indicesAdminClient.preparePutMapping(INDEX_NAME)
                .setType(TYPE)
                .setSource(JSONObject.parseObject("{\"properties\":{\"counts\":{\"type\":\"nested\"},\"pos\":{\"type\":\"geo_point\"}}}", Map.class))
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
