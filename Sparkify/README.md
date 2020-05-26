This is a Basic Data Exploration Exercise for a mock Music Streaming App called Sparkify using Apache Spark in Scala

Users can listen to music for free but with advertisements on Sparkify. Alternatively, they can also subscribe to the premium ad-free tier.

Users logs have been captured to analyze user behaviour by the Sparkify team to better tailor the Sparkify app for customers.

**Input:**
 Logs of user activity with 10k records (to be placed in src/res/data folder)
 
**Output (Example):**
  Songs Played by Hour of Day
  Labelling of records based on event per user
 
## Schema:

 root
 |-- artist: string (nullable = true)
 |-- auth: string (nullable = true)
 |-- firstName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- itemInSession: long (nullable = true)
 |-- lastName: string (nullable = true)
 |-- length: double (nullable = true)
 |-- level: string (nullable = true)
 |-- location: string (nullable = true)
 |-- method: string (nullable = true)
 |-- page: string (nullable = true)
 |-- registration: long (nullable = true)
 |-- sessionId: long (nullable = true)
 |-- song: string (nullable = true)
 |-- status: long (nullable = true)
 |-- ts: long (nullable = true)
 |-- userAgent: string (nullable = true)
 |-- userId: string (nullable = true)
  Total: 10000  
  
## Group By Functions

Suppose we want to find out which is the most popular time of the day that most users are using Sparkify.

 +-----------+-----+
 |Hour of Day|count|
 +-----------+-----+
 |          0|  484|
 |          1|  430|
 |          2|  362|
 |          3|  295|
 |          4|  257|
 |          5|  248|
 |          6|  369|
 |          7|  375|
 |          8|  456|
 |          9|  454|
 |         10|  382|
 |         11|  302|
 |         12|  352|
 |         13|  276|
 |         14|  348|
 |         15|  358|
 |         16|  375|
 |         17|  249|
 |         18|  216|
 |         19|  228|
 |         20|  251|
 |         21|  339|
 |         22|  462|
 |         23|  479|
 +-----------+-----+
 

## Window Aggregation

Users of Sparkify can choose to downgrade from paid tier to free tier. We want to use this downgrade event to label records into different phases, specifically 1 for before downgrade and 0 for after downgrading for further analysis.

 +------+---------+----------------+-----+--------------------+-------------+-----+
|userId|firstName|            page|level|                song|           ts|phase|
+------+---------+----------------+-----+--------------------+-------------+-----+
|  1138|    Kelly|            Home| paid|                null|1513729066284|    1|
|  1138|    Kelly|        NextSong| paid| Everybody Everybody|1513729066284|    1|
|  1138|    Kelly|        NextSong| paid|               Gears|1513729313284|    1|
|  1138|    Kelly|        NextSong| paid|        Use Somebody|1513729552284|    1|
|  1138|    Kelly|        NextSong| paid|Love Of My Life (...|1513729783284|    1|
...
|  1138|    Kelly|        NextSong| paid|  Louder Than A Bomb|1513766189284|    1|
|  1138|    Kelly|        NextSong| paid|       Just Like You|1513766385284|    1|
|  1138|    Kelly|        NextSong| paid|      You're The One|1513766599284|    1|
|  1138|    Kelly|        NextSong| paid|Turn It Again (Al...|1513766838284|    1|
|  1138|    Kelly|        NextSong| paid|     Everywhere I Go|1513767203284|    1|
|  1138|    Kelly|        NextSong| paid|       Easy Skankin'|1513767413284|    1|
|  1138|    Kelly|        NextSong| paid|               Roses|1513767643284|    1|
|  1138|    Kelly|        NextSong| paid|Killing Me Softly...|1513768012284|    1|
|  1138|    Kelly|        NextSong| paid|The Razor (Album ...|1513768242284|    1|
|  1138|    Kelly|        NextSong| paid|   Idols and Anchors|1513768452284|    1|
|  1138|    Kelly|       Downgrade| paid|                null|1513768453284|    1|
|  1138|    Kelly|Submit Downgrade| paid|                null|1513768454284|    1|
|  1138|    Kelly|            Home| free|                null|1513768456284|    0|
|  1138|    Kelly|        NextSong| free|               Bones|1513814880284|    0|
|  1138|    Kelly|            Home| free|                null|1513821430284|    0|
|  1138|    Kelly|        NextSong| free|Grenouilles Manti...|1513833144284|    0|
+------+---------+----------------+-----+--------------------+-------------+-----+