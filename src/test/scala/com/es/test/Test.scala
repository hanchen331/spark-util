package com.es.test

import com.spark.es.util.ElasticsearchManagerTool
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.action.search.SearchType
import org.apache.spark.SparkContext
import org.elasticsearch.spark._
import org.apache.spark.SparkConf
import org.apache.hadoop.conf.Configuration
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.apache.hadoop.io.NullWritable
import org.elasticsearch.hadoop.mr.LinkedMapWritable
object Test {
  val address = "192.168.10.115,192.168.10.110,192.168.10.81"
  val clusterName = "zhiziyun"
  def main(args: Array[String]): Unit = {
    val confs = new SparkConf().setAppName("esRDDtest").setMaster("local")
    
    val conf=new Configuration
    conf.set("es.nodes", "192.168.10.115,192.168.10.110,192.168.10.81")
    conf.setInt("es.port", 9200)
    conf.set("cluster.name", clusterName)
    conf.set("es.resource", "test/testType")
    val sc = new SparkContext(confs)
    sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[NullWritable,LinkedMapWritable]], classOf[NullWritable], classOf[LinkedMapWritable])
    .collect()
    .foreach(println)
    /*.esRDD("test/testType")
    .collect()
    .foreach(println)*/
    
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