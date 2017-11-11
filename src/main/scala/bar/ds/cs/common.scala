package bar.ds.cs

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.io.Source

object common {
  private type OptionMap = Map[Symbol, Any]

  def args_parse(args: Array[String], usage: String, hdfs_paths: Array[String]): (String, String, String) = {
    if (args.length == 0) println(usage)
    val arglist = args.toList

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map

        case "--arr_a_path" :: value :: tail =>
          nextOption(map ++ Map('path_a -> value.toString), tail)

        case "--arr_b_path" :: value :: tail =>
          nextOption(map ++ Map('path_b -> value.toString), tail)

        case "--arr_c_path" :: value :: tail =>
          nextOption(map ++ Map('path_c -> value.toString), tail)

        case "--fs" :: value :: tail =>
          nextOption(map ++ Map('fs -> value.toString), tail)

        case option :: tail => println("Unknown option "+option)
          sys.exit(1)
      }
    }
    val options = nextOption(Map(), arglist)
    println(options)

    val fs = options.getOrElse('fs, "").toString

    def get_prefix(path_name: String, hdfs_paths: Array[String]): String ={
      if (hdfs_paths.contains(path_name) & (fs == "local")) "file://" else ""
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
    bw.close()
  }

  def write_spark(file_path: String, sqlContext: SQLContext, output_rdd: RDD[(String, String, Boolean)]): Unit = {
    sqlContext.createDataFrame(output_rdd).write.mode("Overwrite").format("com.databricks.spark.csv").
      options(Map("delimiter" -> "\t")).save(file_path)
  }

  def get_duo_topic_ary(ary: Array[String]): Array[(String, String)] = {
    val num_ele = ary.length
    return ary.take(num_ele - 1) zip ary.takeRight(num_ele - 1)
  }

  def get_mono_topic_ary(ary: Array[String], ip_type: String): Array[(String, String)] = {
    val num_ele = ary.length
    if(ip_type == "A"){
      ary zip List.fill(num_ele)("NA")
    } else {
      if (num_ele == 1) ary zip Array("NA") else Array[(String, String)]()
    }
  }

  def get_topic_members(member_idx: String, raw_str: String, ip_type: String): Array[((String, String), String)] = {
    val ary = raw_str.split(",")
    val duo_topic_ary = get_duo_topic_ary(ary)
    val mono_topic_ary = get_mono_topic_ary(ary, ip_type)
    val all_topic_ary = duo_topic_ary ++ mono_topic_ary

    all_topic_ary.map{case (from, to) => (from, to) -> member_idx}
  }

  def get_local_topic_membership(ary: RDD[(String, String)], ip_type: String): RDD[((String, String), String)] = {
    ary.flatMap{ case (member_idx, line) => get_topic_members(member_idx, line, ip_type)}
  }

  def get_local_topic_membership(ary: Array[(String, String)], ip_type: String): Array[((String, String), String)] = {
    ary.flatMap{ case (member_idx, line) => get_topic_members(member_idx, line, ip_type)}
  }

  def get_global_topic_membership(local_topic_membership: RDD[((String, String), String)]):
  RDD[((String, String), Array[String])] = {
    local_topic_membership.reduceByKey(_+","+_).mapValues(_.split(","))
  }

  def get_global_topic_membership(local_topic_membership: Array[((String, String), String)]):
  Map[(String, String), Array[String]] = {
    local_topic_membership.groupBy(_._1)
      .mapValues(x => x.foldLeft[Array[String]](Array.empty)((s, l) => {s :+ l._2}))
  }

  def combine_A_n_B_by_Topic(rdd_map: RDD[((String, String), Array[String])],
                             dict_map_bc:Broadcast[Array[((String, String), Array[String])]]):
  RDD[((String, String), (Array[String], Array[String]))] = {

    rdd_map.mapPartitions { topic_list =>
      val dict_map = dict_map_bc.value.toMap


      val topic_AB_mem = topic_list.map{
        case (topic, members_A)=> val members_B = dict_map.get(topic).getOrElse(Array[String]())

          (topic, (members_A, members_B))}

      topic_AB_mem.filter{
        case (topic, (members_A, members_B)) => (members_A.toList.length > 0) & (members_B.toList.length > 0)}}
  }

  def explode_members(topic_AB_mem_ary: RDD[((String, String),(Array[String], Array[String]))]): RDD[(String, String)]
  = {
    val members_pairs = topic_AB_mem_ary.map{case(topic, (members_A, members_B)) => members_B -> members_A}
    members_pairs.flatMapValues(x => x).map(_.swap).flatMapValues(x => x)
  }

  def cocounts(topic_AB_mem: RDD[(String, String)]): RDD[((String, String), Int)] ={
    topic_AB_mem.map{case (member_a, member_b) => (member_a, member_b) -> 1}.reduceByKey(_+_)
  }

  def get_a_b_matches(ab_mem_cocounts: RDD[((String, String), Int)], len_b_bc: Broadcast[Array[(String, Int)]]):
  RDD[(String, Int)] = {
    ab_mem_cocounts.mapPartitions { ab_mem_cocounts_list =>
      val len_b_dict = len_b_bc.value.toMap

      ab_mem_cocounts_list.map { case ((member_a, member_b), cocount) =>
        member_a -> (if (len_b_dict.get(member_b).get <= (cocount + 1)) 1 else 0)
      }
    }
  }

  def sum_over_b(a_b_matches: RDD[(String, Int)]): RDD[(String, Int)] = {
    a_b_matches.reduceByKey(_+_).mapValues(math.min(_, 1))
  }

  def add_idx(raw_input: RDD[String]): RDD[(String, String)] = {
    raw_input.zipWithIndex.map{case (line, idx) => (line, idx.toString)}.map(_.swap)
  }

  def add_idx(raw_input: Array[String]): Array[(String, String)] = {
    raw_input.zipWithIndex.map{case (line, idx) => (line, idx.toString)}.map(_.swap)
  }

  def format_output(A_bool: RDD[(String, (String, Option[Int]))]): RDD[(String, String, Boolean)] = {
    A_bool.map {
      case (idx, (ele_a, ele_a_bool)) => (idx, ele_a, ele_a_bool.getOrElse(0) == 1)
    }
  }

  def format_output(A_bool: Array[(String, (String, Option[Int]))]): Array[Array[Any]] = {
    A_bool.map {
      case (idx, (ele_a, ele_a_bool)) => Array(idx, ele_a, ele_a_bool.getOrElse(0) == 1)
    }
  }

}
