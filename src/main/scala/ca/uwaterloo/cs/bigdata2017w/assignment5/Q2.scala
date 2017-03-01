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
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession


class Conf_2(args: Seq[String]) extends ScallopConf(args){
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "shipdate", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q2 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf_2(argv)

    log.info("Input: " + args.input())
    log.info("Shipdate: " + args.date())
    //   log.info("File type: " + args.text())

    val conf = new SparkConf().setAppName("Shipdate Query")
    val sc = new SparkContext(conf)
    // FileSystem.get(sc.hadoopConfiguration)


    if(args.text()) {
      val lineitem = args.input() + "/lineitem.tbl"
      val orders = args.input() + "/orders.tbl"
      val inputDate = args.date()
      //  val queryDate = inputDate.split("-")

      //  val lineitemRDD = sc.textFile(args.input()+"/lineitem.tbl")
      //  val ordersRDD = sc.textFile(args.input()+"/orders.tbl")

      val lineitemRDD = sc.textFile(lineitem)
      val ordersRDD = sc.textFile(orders)

      val lineitemrdd = lineitemRDD
        .filter(line => {
          val tokens = line.split("\\|")
          tokens(10).contains(inputDate)
        })
        .map(a => {
          val tokens = a.split("\\|")
          (tokens(0), tokens(10))
        })
      //  .map(a => a.split("\\|"))
      //  .map(a => (a(0),1))
      //      .take(10).foreach(println)


      val ordersrdd = ordersRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(6))
        })


        .cogroup(lineitemrdd)
        .filter(pair => pair._2._2.iterator.hasNext)
        .map(p => (p._1.toLong, p._2._1.iterator.next()))
        .sortByKey()
        .take(20).foreach(pair => println("(" + pair._2.toList.mkString + "," + pair._1 + ")"))
    }else if(args.parquet()){

      val sparkSession = SparkSession.builder.getOrCreate
    //  val lineitem =sparkSession.read.parquet("TPC-H-0.1-PARQUET/lineitem.tbl")
    //  val orders = sparkSession.read.parquet("TPC-H-0.1-PARQUET/orders.tbl")
      val inputDate = args.date()
      //  val queryDate = inputDate.split("-")

      //  val lineitemRDD = sc.textFile(args.input()+"/lineitem.tbl")
      //  val ordersRDD = sc.textFile(args.input()+"/orders.tbl")

      val lineitemRDD = sparkSession.read.parquet(args.input()+"/lineitem")
      val ordersRDD = sparkSession.read.parquet(args.input()+"/orders")

      val lineitemrdd = lineitemRDD.rdd
        .filter(line => {
          line(10).toString.equals(inputDate)
        })
        .map(a => {
          (a(0), a(10))
        })

      val ordersrdd = ordersRDD.rdd
        .map(line => {
          (line(0),line(6))
        })

        .cogroup(lineitemrdd)
        .filter(pair => pair._2._2.iterator.hasNext)
        .map(p => (p._1.toString.toInt, p._2._1.iterator.next()))
        .sortByKey()
        .take(20).foreach(pair => println("(" + pair._2.toString.toList.mkString + "," + pair._1 + ")"))

    }
 }
}
