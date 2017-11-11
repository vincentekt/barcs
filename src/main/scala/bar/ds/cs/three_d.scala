package bar.ds.cs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import bar.ds.cs.common.{args_parse, path_prefix, write_spark}

object three_d {

  val usage = """
    Usage: run_three [--arr_a_path string] [--arr_b_path string] [--arr_c_path string] [--fs string] \n
    --arr_a_path: full path to array a, csv, no-compression \n
    --arr_b_path: full path to array b, csv, no-compression \n
    --arr_c_path: full path to output array c, csv, no-compression \n
    --fs: hadoop or local \n
  """

  def main(args: Array[String]): Unit = {

    // Get SparkContext
    val sc = new SparkContext(new SparkConf())
    val sqlContext = new SQLContext(sc)

    // Process arguments
    val (path_a, path_b, path_c) = args_parse(args, usage, Array[String]("path_a", "path_b", "path_c"))

    // Read Array A
    val array_a = sc.textFile(path_a)

    val tmp_a = array_a.zipWithIndex().map(_.swap)
    tmp_a.cache()

    // Extract ("Topic", index) for Array A, e.g.: ((2,3), 0)
    val map_a = tmp_a.flatMap{ x =>
      val ele_list = x._2.split(",")
      val zip_list = ele_list.take(ele_list.length - 1) zip ele_list.takeRight(ele_list.length - 1)
      (zip_list ++ (ele_list zip List.fill(ele_list.length)("NA"))).map(k => (k._1 -> k._2) -> x._1.toString)
    }.reduceByKey(_+","+_).mapValues(_.split(","))


    // Read Array B
    val array_b = sc.textFile(path_b)

    val tmp_b = array_b.zipWithIndex().map(_.swap).mapValues(_.split(","))
    tmp_b.cache()

    // Extract lengths of Array B elements
    val len_b = tmp_b.map(x => x._1.toString -> x._2.length).collectAsMap()
    val len_b_bc = sc.broadcast(len_b)

    // Extract ("Topic", index) for Array B, e.g.: ((2,3), 0)
    val map_b = tmp_b.flatMap{ x =>
      var zip_list = x._2.take(x._2.length - 1) zip x._2.takeRight(x._2.length - 1)
      if (x._2.length == 1){
        zip_list = x._2 zip Array("NA")
      }
      zip_list.map(k => (k._1 -> k._2) -> x._1.toString)
    }.reduceByKey(_+","+_).mapValues(_.split(","))

    // 1. Combine A and B through Topic, e.g.: ((2,3), idx_A, inx_B)
    // 2. Explodes (Flatten) them prior to counting. e.g.: ((2,3), (idx_A_1, idx_A_2), (idx_B_1, idx_B_2)) -->
    // (idx_A_1, idx_B_1), (idx_A_1, idx_B_2), (idx_A_2, idx_B_2), (idx_A_2, idx_B_1)
    val pre_output = map_a.join(map_b).map(x => x._2._1 -> x._2._2).flatMapValues(x=>x).map(_.swap).flatMapValues(x=>x)
      // 3. Get count of interaction between idx_A and idx_B
      .map(x => (x._2, x._1) -> 1).reduceByKey(_+_).mapPartitions { x =>
      val len_b_val = len_b_bc.value
      // 4. If count of interaction is equivalent to length of that B element, then that element of A is true.
      x.map{x =>
        if(len_b_val.get(x._1._2).get <= (x._2 + 1)){
          x._1._1 -> 1
        } else {
          x._1._1 -> 0
        }
      }
      // 5. For every element of A, find if such element exists in B
    }.reduceByKey(_+_).mapValues(math.min(_, 1))

    // Writing output
    val array_c = tmp_a.map(x => x._1.toString -> x._2).leftOuterJoin(pre_output)
      .map(x => (x._1, x._2._1, x._2._2.getOrElse(0) == 1) )

    write_spark(path_c, sqlContext, array_c)
  }
}
