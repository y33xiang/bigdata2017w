/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs.bigdata2017w.assignment5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf_7(args: Seq[String]) extends ScallopConf(args){
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "shipdate", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q7 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf_7(argv)

    log.info("Input: " + args.input())
    log.info("Shipdate: " + args.date())
    //   log.info("File type: " + args.text())

    val conf = new SparkConf().setAppName("Shipdate Query")
    val sc = new SparkContext(conf)
    // FileSystem.get(sc.hadoopConfiguration)

    if(args.text()) {
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
      val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
      val customerRDD = sc.textFile(args.input() + "/customer.tbl")
      val inputDate = args.date()
      //  val queryDate = inputDate.split("-")

      val customerrdd = customerRDD
        .map(line => {
          val tokens = line.split("\\|")
          val custkey = tokens(0)
          val name = tokens(1)
          (custkey, name)
        })
        .collectAsMap()
      val customerrddHashmap = sc.broadcast(customerrdd)


      val ordersrdd = ordersRDD

        .map(line => {
          val tokens = line.split("\\|")
          val o_orderkey = tokens(0)
          val o_custkey = tokens(1)
          val o_orderdate = tokens(4)
          val o_shippriority = tokens(7)
          (o_orderkey, (o_custkey, o_orderdate, o_shippriority))
          //   (custkey,(o_orderkey,o_orderdate,o_shippriority ))
        })
        .map(p => (p._1, (customerrddHashmap.value(p._2._1), p._2._2, p._2._3)))
        .filter(p => {
          p._2._2 < inputDate
        })


      val lineitem = lineitemRDD
        .filter(line => {
          val tokens = line.split("\\|")
          tokens(10) > inputDate
        }).map(p => {
        val tokens = p.split("\\|")
        val l_orderkey = tokens(0)
        val l_extendedprice = tokens(5).toDouble
        val l_discount = tokens(6).toDouble

        val l_revenue = l_extendedprice * (1 - l_discount)
        (l_orderkey, l_revenue)
      })
        .reduceByKey(_ + _)
        .cogroup(ordersrdd)
        .filter(p => p._2._2.iterator.hasNext && p._2._1.iterator.hasNext)
        .map(p => (p._2._1.iterator.next(), (p._2._2.iterator.next()._1, p._1, p._2._2.iterator.next()._2, p._2._2.iterator.next()._3)))
        .sortByKey(false)
        .take(10)
        .map(p => (p._2))
        .foreach(println)

    }else if(args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate

      val lineitemRDD = sparkSession.read.parquet(args.input()+"/lineitem")
      val ordersRDD =sparkSession.read.parquet(args.input()+"/orders")
      val customerRDD =sparkSession.read.parquet(args.input()+"/customer")
      val inputDate = args.date()
      //  val queryDate = inputDate.split("-")

      val customerrdd = customerRDD.rdd
        .map(line => {
     //     val tokens = line.split("\\|")
          val custkey = line(0)
          val name = line(1)
          (custkey, name)
        }).collectAsMap()

      val customerrddHashmap = sc.broadcast(customerrdd)


      val ordersrdd = ordersRDD.rdd

        .map(line => {
     //     val tokens = line.split("\\|")
          val o_orderkey = line(0)
          val o_custkey = line(1)
          val o_orderdate = line(4)
          val o_shippriority = line(7)
          (o_orderkey, (o_custkey, o_orderdate, o_shippriority))
          //   (custkey,(o_orderkey,o_orderdate,o_shippriority ))
        })
        .map(p => (p._1, (customerrddHashmap.value(p._2._1), p._2._2, p._2._3)))
        .filter(p => {
          p._2._2.toString < inputDate
        })


      val lineitem = lineitemRDD.rdd
        .filter(line => {

          line(10).toString > inputDate
        })
        .map(p => {
        val l_orderkey = p(0)
        val l_extendedprice = p(5).toString.toDouble
        val l_discount = p(6).toString.toDouble

        val l_revenue = l_extendedprice * (1 - l_discount)
        (l_orderkey, l_revenue)
      })
        .reduceByKey(_+_)
        .cogroup(ordersrdd)
        .filter(p => p._2._2.iterator.hasNext && p._2._1.iterator.hasNext)
        .map(p => (p._2._1.iterator.next(), (p._2._2.iterator.next()._1, p._1, p._2._2.iterator.next()._2, p._2._2.iterator.next()._3)))


        .sortByKey(false)
        .take(10)
        .map(p => (p._2))
        .foreach(println)

    }






  }
}


