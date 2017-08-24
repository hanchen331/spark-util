package com.es.test

import com.spark.es.util.ElasticsearchManagerTool
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.action.search.SearchType
import org.apache.spark.SparkContext
import org.elasticsearch.spark._
import org.apache.spark.SparkConf
object Test {
  val address = "/192.168.10.115"
  val clusterName = "zhiziyun"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("esRDDtest").setMaster("local")
    conf.set("es.nodes", address)
    conf.set("es.port", "9200")
    conf.set("cluster.name", clusterName)
    val sc = new SparkContext(conf)
    sc
    .esRDD("test/testType")
    .collect()
    .foreach(println)
    
  }
  def esTest() {
    val client = ElasticsearchManagerTool.getESClient(address, clusterName)

    val r = client.prepareGet("sdr_urlinfo", "urlinfo", "abcedsa.afaf")
    println(r.get.getSource)

    val response = client.prepareSearch("sdr_urlinfo")
      .setTypes("urlinfo")
      //.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
      .setQuery(QueryBuilders.prefixQuery("url", "abcedsa.afaf")) // Query
      //.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
      .setFrom(0)
      .setSize(60)
      .setExplain(true)
      .get();
    println(response)

  }
}