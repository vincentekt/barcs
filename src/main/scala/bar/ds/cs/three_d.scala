package bar.ds.cs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object three_d {
  type OptionMap = Map[Symbol, Any]
  val usage = """
    Usage: run_three [--arr_a_path string] [--arr_b_path string] [--arr_c_path string] [--fs string] \n
    --arr_a_path: full path to array a, csv, no-compression \n
    --arr_b_path: full path to array b, csv, no-compression \n
    --arr_c_path: full path to output array c, csv, no-compression \n
    --fs: hadoop or local \n
  """

//  val log = org.apache.log4j.LogManager.getLogger(this.getClass.getName.stripSuffix("$"))

  def args_parse(args: Array[String]): OptionMap = {
    if (args.length == 0) println(usage)
    val arglist = args.toList

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map

        case "--arr_a_path" :: value :: tail =>
          println("--arr_a_path " + value + " " + tail)
          nextOption(map ++ Map('path_a -> value.toString), tail)

        case "--arr_b_path" :: value :: tail =>
          println("--arr_b_path " + value + " " + tail)
          nextOption(map ++ Map('path_b -> value.toString), tail)

        case "--arr_c_path" :: value :: tail =>
          println("--arr_c_path " + value + " " + tail)
          nextOption(map ++ Map('path_c -> value.toString), tail)

        case "--fs" :: value :: tail =>
          println("--fs " + value + " " + tail)
          nextOption(map ++ Map('fs -> value.toString), tail)

        case option :: tail => println("Unknown option "+option)
          sys.exit(1)
      }
    }
    val options = nextOption(Map(), arglist)
    println(options)
    return options
  }

  def main(args: Array[String]): Unit = {

    val cl_args = args_parse(args)

    val sc = new SparkContext(new SparkConf())
    val sqlContext = new SQLContext(sc)

    val fs = cl_args.get('fs).get.toString
    val path_prefix = Map("local" -> "file://", "local" -> "")

    val path_a = path_prefix.get(fs).get + cl_args.get('path_a).get.toString
    val path_b = path_prefix.get(fs).get + cl_args.get('path_b).get.toString
    val path_c = path_prefix.get(fs).get + cl_args.get('path_c).get.toString

    val array_a = sc.textFile(path_a)

    val array_b = sc.textFile(path_b)

//    val array_b_bc = sc.broadcast(array_b)

    val tmp_a = array_a.zipWithIndex().map(_.swap)
    tmp_a.cache()

    val map_a = tmp_a.flatMap{ x =>
      val ele_list = x._2.split(",")
      val zip_list = ele_list.take(ele_list.length - 1) zip ele_list.takeRight(ele_list.length - 1)
      (zip_list ++ (ele_list zip List.fill(ele_list.length)("NA"))).map(k => (k._1 -> k._2) -> x._1.toString)
    }.reduceByKey(_+","+_).mapValues(_.split(","))

    val tmp_b = array_b.zipWithIndex().map(_.swap).mapValues(_.split(","))
    tmp_b.cache()
    val len_b = tmp_b.map(x => x._1.toString -> x._2.length).collectAsMap()
    val len_b_bc = sc.broadcast(len_b)

    val map_b = tmp_b.flatMap{ x =>
      var zip_list = x._2.take(x._2.length - 1) zip x._2.takeRight(x._2.length - 1)
      if (x._2.length == 1){
        zip_list = x._2 zip Array("NA")
      }
      zip_list.map(k => (k._1 -> k._2) -> x._1.toString)
    }.reduceByKey(_+","+_).mapValues(_.split(","))
//
    val pre_output = map_a.join(map_b).map(x => x._2._1 -> x._2._2).flatMapValues(x=>x).map(_.swap).flatMapValues(x=>x)
      .map(x => (x._2, x._1) -> 1).reduceByKey(_+_).mapPartitions { x =>
      val len_b_val = len_b_bc.value

      x.map{x =>
        if(len_b_val.get(x._1._2).get <= (x._2 + 1)){
          x._1._1 -> 1
        } else {
          x._1._1 -> 0
        }
      }
    }.reduceByKey(_+_).mapValues(math.min(_, 1))

    sqlContext.createDataFrame(tmp_a.map(x => x._1.toString -> x._2).leftOuterJoin(pre_output)
      .map(x => (x._1, x._2._1, x._2._2.getOrElse(0) == 1) )).write.mode("Overwrite").format("com.databricks.spark.csv").
      options(Map("delimiter" -> "\t")).save(path_c)
  }
}
