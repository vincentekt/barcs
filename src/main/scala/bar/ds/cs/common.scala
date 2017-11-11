package bar.ds.cs

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.io.Source

object common {
  private type OptionMap = Map[Symbol, Any]

  val path_prefix = Map("local" -> "file://", "local" -> "")

  def args_parse(args: Array[String], usage: String, hdfs_paths: Array[String]): (String, String, String) = {
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

    val fs = options.get('fs).get.toString

    def get_prefix(path_name: String, hdfs_paths: Array[String]): String ={
      if ((! hdfs_paths.contains(path_name)) & (fs == "local")){
        return "file://"
      }
      else {
        return ""
      }
    }

    val path_a = get_prefix("path_a", hdfs_paths) + options.get('path_a).get.toString
    val path_b = get_prefix("path_b", hdfs_paths) + options.get('path_b).get.toString
    val path_c = get_prefix("path_c", hdfs_paths) + options.get('path_c).get.toString

    return (path_a, path_b, path_c)
  }

  def read_scala(file_path: String): Array[String] = {
    val bufferedSource = Source.fromFile(file_path)
    val text_lines = (for (line <- bufferedSource.getLines()) yield line).toArray
    bufferedSource.close()
    return text_lines
  }

  def write_scala(file_path: String, out_obj: Array[Array[Any]]): Unit = {
    // Writing output
    val file = new File(file_path)
    val bw = new BufferedWriter(new FileWriter(file))
    out_obj.foreach { x => bw.write(x.mkString("\t") + "\n") }
  }

  def write_spark(file_path: String, sqlContext: SQLContext, output_rdd: RDD[(String, String, Boolean)]): Unit = {
    sqlContext.createDataFrame(output_rdd).write.mode("Overwrite").format("com.databricks.spark.csv").
      options(Map("delimiter" -> "\t")).save(file_path)
  }
}
