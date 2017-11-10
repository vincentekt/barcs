
## Q3

### Requirement
1. Spark - 2.1.1
2. Scala - 2.11.6
3. Input files, example of array A and array B can be found in src/main/resources

### Explaination

#### Steps
All of the solutions follow similar steps as follows
1. Extract transition of each of the array elements, think of it as "Topic" extraction or key.
2. Group index of Array A and B elements into respective "Topic". These indeces could present in different "Topic"s.
3. This equivalent to saying in "Topic" **2,3** we have seen *a_1* and *b_1* once, where *a_1* is an element of Array A. 
For *a* to be considered **true**, the number of "Topic" *a_1* and *b_1* seen together must be equivalent to length of 
*b_1*. If this is not fulfilled, maybe *b_2* could come to rescue, as long as there's an element of array B that could 
satisfy the requirement.

#### Code
1. Pure scala.
2. Map partition on Array A, broadcast Array B as dictionary.
3. Map partition on Array B, broadcast Array A as dictionary.
4. Map partition on Array A and Array B and then do a join.

### Commands
1. Question 3 a: ${SPARK_HOME}/spark-submit --class bar.ds.cs.three_a --master "local[2]" /home/vladislav/Projects/barcs/target/scala-2.11/barcs-assembly-0.1.jar --arr_a_path /home/vladislav/Documents/arr_a --arr_b_path /home/vladislav/Documents/arr_b --arr_c_path /home/vladislav/Documents/arr_c_local
2. Question 3 b: ${SPARK_HOME}/spark-submit --class bar.ds.cs.three_b --master "local[2]" /home/vladislav/Projects/barcs/target/scala-2.11/barcs-assembly-0.1.jar --arr_a_path /home/vladislav/Documents/arr_a --arr_b_path /home/vladislav/Documents/arr_b --arr_c_path /home/vladislav/Documents/arr_c --fs local
3. Question 3 c: ${SPARK_HOME}/spark-submit --class bar.ds.cs.three_c --master "local[2]" /home/vladislav/Projects/barcs/target/scala-2.11/barcs-assembly-0.1.jar --arr_a_path /home/vladislav/Documents/arr_a --arr_b_path /home/vladislav/Documents/arr_b --arr_c_path /home/vladislav/Documents/arr_c_local --fs local
4. Question 3 d: ${SPARK_HOME}/spark-submit --class bar.ds.cs.three_d --master "local[2]" /home/vladislav/Projects/barcs/target/scala-2.11/barcs-assembly-0.1.jar --arr_a_path /home/vladislav/Documents/arr_a --arr_b_path /home/vladislav/Documents/arr_b --arr_c_path /home/vladislav/Documents/arr_c --fs local

### Example output
|Index A|Element A|Bool|
|---|---|---|
| 0  | 1,2,3  | true  |
| 1 | 2,3  | true  |
| 2 | 1,44  | true  |
| 3 | 100,6,33  | false   |
| 4 | 7  | true  |
| 5 | 98,99,100,101  | true  |
| 6 | 98,99,100  |false   |
| 7 | 98,99,100,101,102  | true  |
| 8 | 6,7  | true  |

