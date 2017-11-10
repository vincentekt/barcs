
## Q3

### Requirement
1. Spark
2. Scala
3. Input files, example of array A and array B can be found in src/main/resources

### Explaination
1. Pure scala
2. Map partition on Array A, broadcast Array B as dictionary
3. Map partition on Array B, broadcast Array A as dictionary
4. Map partition on Array A and Array B

### Commands
1. Question 3 a: ${SPARK_HOME}/spark-submit --class bar.ds.cs.three_a --master "local[2]" /home/vladislav/Projects/barcs/target/scala-2.11/predict-assembly-0.1.jar --arr_a_path /home/vladislav/Documents/arr_a --arr_b_path /home/vladislav/Documents/arr_b --arr_c_path /home/vladislav/Documents/arr_c_local
2. Question 3 b: ${SPARK_HOME}/spark-submit --class bar.ds.cs.three_b --master "local[2]" /home/vladislav/Projects/barcs/target/scala-2.11/predict-assembly-0.1.jar --arr_a_path /home/vladislav/Documents/arr_a --arr_b_path /home/vladislav/Documents/arr_b --arr_c_path /home/vladislav/Documents/arr_c --fs local
3. Question 3 c: ${SPARK_HOME}/spark-submit --class bar.ds.cs.three_c --master "local[2]" /home/vladislav/Projects/barcs/target/scala-2.11/predict-assembly-0.1.jar --arr_a_path /home/vladislav/Documents/arr_a --arr_b_path /home/vladislav/Documents/arr_b --arr_c_path /home/vladislav/Documents/arr_c_local --fs local
4. Question 3 d: ${SPARK_HOME}/spark-submit --class bar.ds.cs.three_d --master "local[2]" /home/vladislav/Projects/barcs/target/scala-2.11/predict-assembly-0.1.jar --arr_a_path /home/vladislav/Documents/arr_a --arr_b_path /home/vladislav/Documents/arr_b --arr_c_path /home/vladislav/Documents/arr_c --fs local

