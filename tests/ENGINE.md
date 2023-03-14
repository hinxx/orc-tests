# computing engine

# spark

https://thenewstack.io/the-good-bad-and-ugly-apache-spark-for-data-science-work/

Spark is an open-source distributed computing framework that promises a clean and pleasurable experience similar to that of Pandas, while scaling to large data sets via a distributed architecture under the hood. In many respects, Spark delivers on its promise of easy-to-use, high-performance analysis on large datasets. However, Spark is not without its quirks and idiosyncrasies that occasionally add complexity.

GOOD

Appealing APIs and Lazy Execution
Spark’s API is truly appealing. Users can choose from multiple languages: Python, R, Scala and Java. Spark offers a data frame abstraction with object-oriented methods for transformations, joins, filters and more..

Easy Conversion
PySpark offers a “toPandas()” method to seamlessly convert Spark DataFrames to Pandas, and its “SparkSession.createDataFrame()” can do the reverse..

Easy Transformations
Pivoting is a challenge for many big data frameworks. In SQL, it typically requires many case statements. Spark has an easy and intuitive way of pivoting a DataFrame.

Open Source Community
Spark has a massive open-source community behind it..

BAD

Cluster Management
Spark is notoriously difficult to tune and maintain.
Ensuring top performance so that it doesn’t buckle under heavy data science workloads is challenging.
If your cluster isn’t expertly managed, this can negate “the Good” as we described above.
Jobs failing with out-of-memory errors is very common and having many concurrent users makes resource management even more challenging.

Debugging
Debugging Spark can be frustrating.
The client-side type checking for “DataFrame” operations in PySpark can catch some bugs (like trying to do operations on fields with incompatible types).
But memory errors and errors occurring within user-defined functions can be difficult to track down.
Error messages can be misleading or suppressed, logging from a PySpark User Defined Function (UDF) is difficult and introspection into current processes is not feasible.

Slowness of PySpark UDFs
PySpark UDFs are much slower and more memory-intensive than Scala and Java UDFs are.
Python UDFs require moving data from the executor’s JVM to a Python interpreter, which is slow.

Hard-to-Guarantee Maximal Parallelism
It can be difficult to ensure Spark parallelizes computations as much as possible.
Spark tries to elastically scale how many executors a job uses based on the job’s needs, but it often fails to scale up on its own.

UGLY

The ugly aspects of Spark tend to fall into two categories:
 aspects of the API that are awkward or don’t make sense and
 a lack of maturity and feature completeness of the Apache Spark project [not sure how much that applies to 3.x].

API Awkwardness
Since much of the Spark API is so elegant, the inelegant parts really stand out. For example, we consider accessing array elements to be an ugly part of Spark life.

Lack of Maturity and Feature Completeness
Spark’s machine learning library lacks some basic features.,, [mostly in < 2.x, so this might be improved in 3.x]


https://www.dominodatalab.com/blog/considerations-for-using-spark-in-your-data-science-stack

Spark sophistication of your data science team.
Spark is written in Scala, and has APIs for Scala, Python, Java, and R.
To make Spark function well, they [developers/scientists] will also need to learn memory- and performance-related topics such as:
Partitions, Nodes, Serialization, Executors, the JVM, and more.

Adopting Spark typically involves retraining your data science organization.

Debugging capabilities of your team.

Debugging Spark can be frustrating, as memory errors and errors occurring within user-defined functions can be difficult to track down.
Error messages can be misleading or suppressed, and sometimes a function that passes local tests fails when running on the cluster.
Figuring out the root cause in those cases is challenging.
PySpark errors will show both Java stack trace errors as well as references to the Python code.

IT challenges with maintaining Spark.

Spark is notoriously difficult to tune and maintain.
IT typically does not have deep expertise in Spark-specific memory and cluster management so ensuring that the cluster does not buckle under heavy data science workloads and many concurrent users is challenging.
If your cluster is not expertly managed, performance can be abysmal, and jobs failing with out-of-memory errors can occur often.

https://visual-flow.com/blog/6-apache-spark-alternatives-for-etl

Nevertheless, we cannot say that Apache Spark is a truly universal tool.
One of the key problems of Spark is the high complexity of creating and maintaining applications and processes based on it, due to the need for a high degree of technical acumen.
Moreover, if latency is a critical factor, Apache Spark won’t do, and you would be better off looking into different Spark competitors.
Let’s take a look at them, as it’s always better to have a few trump cards in your sleeve.

https://www.reddit.com/r/dataengineering/comments/jigmuc/apache_spark_declining_trends_the_end_of_the_era/

We've had dozens of silver bullets over the last 25 years in this space, but two standouts over the last ten are Hadoop & Spark.

Both over-promised and were over-used by shops with small data needs that thought they needed a massive solution, by shops with medium data needs that underestimated the admin required by these solutions and as well as how good the alternatives are.

Even genuine big-data shops have discovered that Spark isn't the best job for every big data problem: event-driven micro-batch pipelines using S3, SQS, AWS lambdas & Kubernetes can be much better, there's other streaming solutions like Flink, and there's other, simpler SaaS MPP databases (ex: Snowflake).

But this doesn't mean it's going away. Like Gartner likes to illustrate - we could be experiencing the "trough of disillusionment" that follows any overly-hyped product or technology, and that often precedes more diminished but continuous and practical use of it.

https://www.reddit.com/r/dataengineering/comments/o02lqu/is_apache_spark_trending_down_why/


My opinion is that there's a couple of things going on...

Spark (w/o databricks) is finicky as fuck. I've wasted hours and hours tuning low level parameters in spark. highly scalable managed sql engines such as redshift, athena snowflake etc provide a much more reliable product for the non expert.

Spark on EMR is getting easier to use and requires less hand tuning to get right due to EMR customized version of spark. It's significantly faster and more reliable than open source spark. Because its better behaved you don't have to go looking on stack overflow for weird exceptions as much.

Because spark in finicky people are realizing for simple tasks they can just fire up lambdas in many cases. Lambda's have better reliability properties as theirs no driver to go down and individual invocations of the lambda don't have as much of a chance to take out everything else.

ML people are moving on to more specialized tools. Spark is great for feature generation but after that other tools are taking over.

The tooling for faster iteration development is not as great as the SQL world. In the sql world people expect to be able to quickly iterate on queries and sql notebooks are also finicky and crash occasionally.

https://www.datanami.com/2019/04/03/apache-spark-is-great-but-its-not-perfect/

If you starve Spark of RAM, fail to grasp how it works, or make some other configuration error, all those Spark performance benefits you hoped to get will go flying out the window.

“It’s relatively easy to write something in Spark, but what happens under the hood in Spark gets complicated,” Venkatrao tells Datanami.
“People just write a piece of code, submit it, and assume magic happens. Well, no, it cannot. You need to go and understand and tune all of that and figure out what’s going on.”

Complexity is a recurring theme in big data analytics, and unfortunately, Spark  did not solve the complexity problem for the benefit of the big data community.


https://www.whizlabs.com/blog/apache-spark-limitations/

10. Manual Optimization
Manual optimization of jobs, as well as datasets, is required while working with Spark. To make partitions, users can specify the number of Spark partitions on their own. For this, the number of partitions to be fixed is required to pass as the parameter of the parallelize method. In order to get the correct partitions and cache, all these partition procedures should be controlled manually.




# dask

https://www.dominodatalab.com/blog/considerations-for-using-spark-in-your-data-science-stack

In 2018, Dask was released to create a powerful parallel computing framework that is extremely usable to Python users, and can run well on a single laptop or a cluster. Dask is lighter weight and easier to integrate into existing code and hardware than Spark.

Whereas Spark adds a significant learning curve involving a new API and execution model, Dask is a pure Python framework, so most data scientists can start using Dask almost immediately. Dask supports Pandas dataframes and Numpy array data structures, so data scientists can continue using the tools they know and love. Dask also integrates tightly with Scikit-learn’s JobLib parallel computing library that enables parallel processing of Scikit-learn code with minimal code changes.

# polars


https://pola-rs.github.io/polars-book/user-guide/introduction.html

Pandas

A very versatile tool for small data. Read 10 things I hate about pandas written by the author himself [https://wesmckinney.com/blog/apache-arrow-pandas-internals/]. Polars has solved all those 10 things. Polars is a versatile tool for small and large data with a more predictable API, less ambiguous and stricter API.


https://www.reddit.com/r/dataengineering/comments/10seqay/using_polars_over_pandas_or_pyspark/

Q: Why is it better than pyspark?

A: Because you don't need spark lol. Spark really sucks when your data fits into one machine's memory, the serialization and redundancy overhead is huge and unnecessary. And maintaining spark clusters sucks or you pay up the nose for someone else to maintain them. Polars is hyper optimized for single-machine performance. It uses all the cores on your machine (leaving single-threaded pandas in the dust), it's extremely memory efficient (thanks rust), and it performs computation lazily.

It certainly won't replace spark as it's not made for bigger-than-memory data, but it should and will replace pandas. I also think people using polars will be able to reconsider whether they need a whole spark cluster for a lot of things if you can just scale up a single machine to have hundreds of GB RAM and dozens of CPUs and then use that machine efficiently.

https://kevinheavey.github.io/modern-polars/scaling.html

Polars doesn’t come with any tooling for running on a cluster, but it does have a streaming mode for larger-than-memory datasets on a single machine. It also uses memory more efficiently than Pandas. These two things mean you can use Polars for much bigger data than Pandas can handle, and hopefully you won’t need tools like Dask or Spark until you’re actually running on a cluster.

https://kevinheavey.github.io/modern-polars/


# pandas

https://wesmckinney.com/blog/apache-arrow-pandas-internals/



# duckdb

This is a standalone SQL and API processing toolkit that can be embedded in an app (c++, python) or used
as a CLI tool to explore data. Data can be ingested from CSV, parquet, sqlite3, or directly from pandas dataframes, 
numpy arrays or pyarrow tables and worked with using SQL.

Utilizes multiple cores, loads data in chunks and allows extentions (parquet and slite3 support are impl. as extensions).

Does not support ingesting ORC format.


# rocksdb

Key-value store DB engine. Not a database, but just and engine.
Saves data in SSTables.

# rocksdb + duckdb

Idea: use RocksDB to ingest PV data into SSTables. Then use DuckDB to work with data by either 1) connect to RocksDB and issue get/scan queries or, 2) work with the SSTable(s) without a running RocksDB.

DuckDB supports pluggable DB engines. Here is support for Sqlite

https://github.com/duckdblabs/sqlite_scanner
https://github.com/duckdblabs/sqlite_scanner/pull/25
https://github.com/duckdb/duckdb/pull/6066

The Postgres scanner is also here:

https://github.com/duckdblabs/postgres_scanner


