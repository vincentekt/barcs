package bar.ds.cs

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import bar.ds.cs.common._

object three_c {

  val usage = """
    Usage: run_three [--arr_a_path string] [--arr_b_path string] [--arr_c_path string] [--fs string] \n
    --arr_a_path: full path to array a, csv, no-compression \n, local
    --arr_b_path: full path to array b, csv, no-compression \n, local | hdfs
    --arr_c_path: full path to output array c, csv, no-compression \n, local
    --fs: hadoop or local \n
  """

  def main(args: Array[String]): Unit = {

    // Get SparkContext
    val sc = new SparkContext(new SparkConf())
    val sqlContext = new SQLContext(sc)

    // Process arguments
    val (path_a, path_b, path_c) = args_parse(args, usage, Array[String]("path_b"))

    //------------------------------------------------------------------------------------------------
    // i) Preprocess array A
    //------------------------------------------------------------------------------------------------
    // Read Array A
    val ary_A = add_idx(read_scala(path_a))

    // Extract ("Topic", index) for Array A, e.g.: ((2,3), 0)
    val a_memberships = get_global_topic_membership(get_local_topic_membership(ary_A, "A"))

    // Broadcast a membership dictionary
    val a_memberships_bc = sc.broadcast(a_memberships.toArray)

    //------------------------------------------------------------------------------------------------
    // ii) Preprocess array B
    //------------------------------------------------------------------------------------------------
    // Read Array B
    val ary_B = add_idx(sc.textFile(path_b))

    // Extract lengths of Array B elements and broadcast them
    val len_b = ary_B.map{case (idx, line) => idx -> line.split(",").length}.collectAsMap()
    val len_b_bc = sc.broadcast(len_b.toArray)

    // Extract ("Topic", index) for Array B, e.g.: ((2,3), 0)
    val b_memberships = get_global_topic_membership(get_local_topic_membership(ary_B, "B"))

    // Extract lengths of Array B elements

    //------------------------------------------------------------------------------------------------
    // iii) Main logic
    //------------------------------------------------------------------------------------------------
    // 1. Combine A and B through Topic, e.g.: ((2,3), Array(idx_A), Array(inx_B))
    val ab_memberships_ary = combine_A_n_B_by_Topic(b_memberships, a_memberships_bc)

    // 2. Explodes (Flatten) them prior to counting. e.g.: ((2,3), (idx_A_1, idx_A_2), (idx_B_1, idx_B_2)) -->
    // (idx_A_1, idx_B_1), (idx_A_1, idx_B_2), (idx_A_2, idx_B_2), (idx_A_2, idx_B_1)

    val ab_pair = explode_members(ab_memberships_ary)

    // 3. Get count of interaction between idx_A and idx_B
    val ab_pair_cocounts = cocounts(ab_pair).map{case ((member_b, member_a), cocount) => ((member_a, member_b), cocount)}

    // 4. If count of interaction is equivalent to length of that B element, then that element of A is true.
    val ab_matches = get_a_b_matches(ab_pair_cocounts, len_b_bc)

    // 5. For every element of A, find if such element exists in B
    val a_bool = sum_over_b(ab_matches).collectAsMap()

    //------------------------------------------------------------------------------------------------
    // iv) Outputing
    //------------------------------------------------------------------------------------------------
    // Format output
    val array_c = format_output(ary_A.map{case (idx, line) => (idx, (line, a_bool.get(idx)))})

    // Writing output
    write_scala(path_c, array_c)
  }
}
