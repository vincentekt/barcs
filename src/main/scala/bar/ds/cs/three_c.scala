package bar.ds.cs

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import java.io._

object three_c {
  type OptionMap = Map[Symbol, Any]
  val usage = """
    Usage: run_three [--arr_a_path string] [--arr_b_path string] [--arr_c_path string] [--fs string] \n
    --arr_a_path: full path to array a, csv, no-compression \n, local
    --arr_b_path: full path to array b, csv, no-compression \n, local | hdfs
    --arr_c_path: full path to output array c, csv, no-compression \n, local
    --fs: hadoop or local \n
  """

  val log = org.apache.log4j.LogManager.getLogger(this.getClass.getName.stripSuffix("$"))

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

    // Get SparkContext
    val sc = new SparkContext(new SparkConf())
    val sqlContext = new SQLContext(sc)

    // Process arguments
    val cl_args = args_parse(args)

    val fs = cl_args.get('fs).get.toString
    val path_prefix = Map("local" -> "file://", "local" -> "")

    val path_a = cl_args.get('path_a).get.toString
    val path_b = path_prefix.get(fs).get + cl_args.get('path_b).get.toString
    val path_c = cl_args.get('path_c).get.toString

    // Read Array A
    val bufferedSource = Source.fromFile(path_a)
    val array_a_lines = (for (line <- bufferedSource.getLines()) yield line).toArray
    bufferedSource.close()

    val tmp_a = array_a_lines.zipWithIndex.map(_.swap)

    // Extract ("Topic", index) for Array A, e.g.: ((2,3), 0)
    val map_a = tmp_a.flatMap{ x =>
      val ele_list = x._2.split(",")
      val zip_list = ele_list.take(ele_list.length - 1) zip ele_list.takeRight(ele_list.length - 1)
      (zip_list ++ (ele_list zip List.fill(ele_list.length)("NA"))).map(k => (k._1 -> k._2) -> x._1.toString)
    }.groupBy(_._1).mapValues(x => x.foldLeft[Array[String]](Array.empty)((s, l) => {s :+ l._2}))

    val map_a_bc = sc.broadcast(map_a.toArray)

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

    // This is where the main logic occur
    val pre_output = map_b.mapPartitions { x =>
      val map_a_val = map_a_bc.value.toMap
      x.map{y =>
        // 1. Combine A and B through Topic, e.g.: ((2,3), idx_A, inx_B)
        (y._1, (map_a_val.get(y._1).getOrElse(Array[String]()), y._2))
      }
        .filter{y => (y._2._1.toList.length > 0) & (y._2._2.toList.length > 0)}
    }
      // 2. Explodes (Flatten) them prior to counting. e.g.: ((2,3), (idx_A_1, idx_A_2), (idx_B_1, idx_B_2)) -->
      // (idx_A_1, idx_B_1), (idx_A_1, idx_B_2), (idx_A_2, idx_B_2), (idx_A_2, idx_B_1)
      .map(x => x._2._1 -> x._2._2).flatMapValues(x=>x).map(_.swap).flatMapValues(x=>x)
      // 3. Get count of interaction between idx_A and idx_B
      .map(x => (x._2, x._1) -> 1).reduceByKey(_+_).mapPartitions { x =>
      val len_b_val = len_b_bc.value

      x.map{x =>
        // 4. If count of interaction is equivalent to length of that B element, then that element of A is true.
        if(len_b_val.get(x._1._2).get <= (x._2 + 1)){
          x._1._1 -> 1
        } else {
          x._1._1 -> 0
        }
      }

      // 5. For every element of A, find if such element exists in B
    }.reduceByKey(_+_).mapValues(math.min(_, 1)).collectAsMap()

    // Writing output
    val file = new File(path_c)
    val bw = new BufferedWriter(new FileWriter(file))
    tmp_a.map(x => Array(x._1, x._2, pre_output.get(x._1.toString).getOrElse(0) == 1)).foreach{x =>
      bw.write(x.mkString("\t") + "\n")
    }

    bw.close()
  }
}
