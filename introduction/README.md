# MapReduce with Spark:

## Table Of Content:
  1. [Objective](#objective)  
  2. [Requirements](#requirements)  
  3. [Installation](#installation)  
  4. [What is Spark?](#what-is-spark)  
  5. [Who and What is Spark Used For?](#who-and-what-is-spark-used-for)  
  6. [Background](#background)  
     a. [Initialization](#initialization)  
     b. [Resilient Distributed Datasets](#resilient-distributed-datasets)  
     c. [Operations](#operations)  
  7. [Example](#example)  
     a. [Loading Data](#loading-data)  
     b. [Mapping Phase](#mapping-phase)  
     c. [Reduce Phase](#reduce-phase)  
     d. [Saving Data](#saving-data)  
     e. [Submitting a Job](#submitting-a-job)  

## Objective
From this introduction, a student should begin to understand how to translate their understanding of Hadoop’s MapReduce framework to Spark’s MapReduce Framework. Once completed, the student will have an adequate amount of understanding of Spark’s MapReduce framework.

## Requirements
- Hadoop Cluster with Spark installed
- Minimal Knowledge of Python
- PySpark

## Installation
PySpark comes standard with most Spark installation. If you have Ambari installed, both Spark and Spark2 can be installed via the Ambari Web UI.

Otherwise, please ensure that Java 7+ is installed. To check if Java is installed, run the following command:
```
  $ java -version
```
If the command does not run, then Java is not installed. If the version outputted is not `1.7` or above, then please upgrade your Java version.

We are now ready to install Spark. Download Spark [here](https://spark.apache.org/downloads.html) and extract the Spark tar file with the following command:
```
  $ tar xvf <spark.tar>
```

It is suggested that you move the Spark files from your downloads to some standard directory such as `/usr/local/spark`. You can do so by running the following command:
```
  cd <spark_download_location>
  mv <spark_binary> /usr/local/spark
```

Now we need to update the `~/.bashrc` file by adding the path the Spark binary location. To do so, run the following command:
```
  export PATH = $PATH:/usr/local/spark/bin
```

Spark should now be installed. To verify the installation, run the command `pyspark`. You should get a Python REPL with Spark integration.

Installation Adapted from [TutorialPoint](https://www.tutorialspoint.com/apache_spark/apache_spark_installation.htm)

## What is Spark?
On the Apache Spark homepage, Spark claims to be 100x faster than standard Hadoop than MapReduce when performing in-memory computations and 10x faster than Hadoop when performing on-disk computations.

What does this mean? Spark, much like MapReduce, works by distrubuting data across a cluster and processes it parallel; however, unlike your standard MapReduce, most of the data processing occurs in-memory rather than on-disk.

To achieve this, Spark internally maintains what is called Resilient Distributed Datasets (RDDs) which are read-only data stored on a cluster. The RDDs are stored on the cluster in a fault-tolerant. This new data structure was developed to overcome the MapReduce linear dataflow. That is, a typical MapReduce program will read data from disk, run the Map Phase, run the Reduce Phase, and store the Reduced results on disk. [More Information](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)

Spark natively supports Java, Scala, Python, and R. Unlike Hadoop, Spark does not rely on a Streaming API to work with languages other than Java. Furthermore, Spark supports interactive shells for Scala, Python, and R.

Similar to Hadoop, many powerful libraries utilizes Spark's computation engine to perform data analytics. These libraries include Spark SQL, Spark Streaming, MLlib (Machine Learning Library), and GraphX (Graphing Libarary).

Lastly, Spark supports many different distributive computing setups such as Hadoop, HDFS, Cassandra, HBase, and Mesos.

## Who and What is Spark Used For?
  - [eBay](https://spark.apache.org/powered-by.html)
    - Spark Core for transaction logging, aggregation, and analytics
  - [VideoAmp](https://spark.apache.org/powered-by.html)
    - Intelligent video ads targetting specific online and television viewers
  - [MyFitnessPal](https://spark.apache.org/powered-by.html)
    - Clean-up user specified food data to identify high-quality food items
    - Recommendation engine for recipes and foods.
  - [IBM](http://www.ibmbigdatahub.com/blog/what-spark)
    - IBM SPSS: Spark MLlib algorithms are invoked from IBM SPSS Modeler workflows
    - IBM BigSQL: Spark is used to access data from HDFS, S3, HBase, and other NoSQL databases using IBM BigSQL. Spark RDD is returned to IBM BigSQL for processing.
    - IBM InfoSphere Streams: Spark transformation, action, and MLlib functions can be added to existing Stream application for improved data analytics.
    - IBM Cloudant: Spark analyzes data collected on IBM Bluemix.
  - [Uber](https://www.qubole.com/blog/big-data/apache-spark-use-cases/)
    - Spark is used with Kafka and HDFS in a continuous Extract, Transform, Load pipeline. Uber terabytes of raw userdata into structured data to perform more complicated analytics.
  - [Pinterest](https://www.qubole.com/blog/big-data/apache-spark-use-cases/)
    - Spark Streaming is leverage to perform real-time analytics on how users are engaging with Pins. This allows Pinterest to make relevant information navigating the site.
  - [Yahoo](https://www.datanami.com/2014/03/06/apache_spark_3_real-world_use_cases/)
    - Spark MLlib is used to customize Yahoo's homepage news feed for each user.
    - Spark is also used with Hive to allow Yahoo to query advertisement user data for analytics.

## Background
### Initialization
Before we dive into the code, we must first understand how Spark is structured. To use Spark, we must import the SparkContext object. The SparkContext object requires a SparkConf object. In Python, it looks like this:

```python
  from pyspark import SparkContext, SparkConf

  conf = SparkConf().setAppName(appName).setMaster(master)
  sc = SparkContext(conf=conf)
```

In the above code, `appName` is the name of the application shown in some cluster UI. `master` is the cluster URL. For testing, `master` can be set to the string `local`.

### Resilient Distributed Datasets
Spark works internally by manipulating objects called Resilient Distributed Datasets (RDDs). RDDs can be created within PySpark by simply calling the method `parallelize`.

```python
  data = [1, 2, 3, 4, 5]
  dataRDD = sc.parallelize(data)
```

Alternatively, RDDs also support loading files from many different sources. This includes HDFS, Amazon S3, HBase, etc. In addition, Spark supports all of Hadoops InputFormats. Here is how to, read a text file from HDFS:

```python
  data = sc.textFile("hdfs:///tmp/sometextFile.txt")
```

### Operations:
There are two broad categories on RDDs: transformations and actions. A transformation is one in which a new dataset is created from an existing dataset. A `map` operation is a transformation. An action is one in which a value is returned. A `reduce` operation is an action. All transfomrations are lazy. Rather than computing the results immediately, the transformation that was applied is saved and, when needed, will be computed.

By default, each transformation may require Spark to recompute the same transformed RDD. If that is the case, then there is a `persist` or `cache`. These commands are the same and will store the transformed RDD in memory for another call.

## Example
### Loading Data
We will be working with `temperatureSample.txt` from the first day of class. To load data:
```python
  # Import SparkContext.
  from pyspark import SparkContext

  # Construct a Spark Context.
  sc = SparkContext()

  # Load data.
  temperature_data = sc.textFile("hdfs:///tmp/input/temperatureSample.txt")
```
That is, it. If we wanted to check what is in `temperature_data`, we could run the one of the following commands:
```python
  # To see the first entry.
  temperature_data.first()

  # To see all the entries.
  temperature_data.collect()
```

### Mapping Phase
Now that data is loaded, we need to a mapper to extract what we really care about: the year and temperature. To do so, we will create a Python local function.
```python
  def temperature_mapper(line):
    # This is very similar to the first day's Java code with Python list comprehension.

    # Grab the year and convert it from bytestream to string.
    year = str(line[15:19])

    # Grab the temperature and convert it from bytestream to int.
    if (line[87] == "+"):
      air_temperature = int(line[88:92])
    else:
      air_temperature = int(line[87:92])

    # Grab the quality code.
    quality = int(line[92:93])

    # Return a tuple of year and temperature. To further simplify, we will only care about quality codes 1 or 0.
    if (air_temperature != 9999 and (quality == 1 or quality == 0)):
      return (year, air_temperature)
```

With the mapping function define, we can pass its reference like so:
```python
  temperatures = temperature_data.map(temperature_mapper)
```
Now every entry of `temperatures` will be a tuple of `(year, temperature)`.

### Reduce Phase
Suppose we want to get the max, min, and average temperature for each year. Our data so far looks something like this: `[(1950, 11), (1949, 13), (1950, 83)]`. We can then call the `reduceByKey` method. This function is similar to how a combiner works in Hadoop's MapReduce framework. That is, the mapper will perform some minor reduction of the results to send it off to the reducer. In this case, we can have use this method to merge the results of the year to compute our max and min temperatures.

```python
  # Here we are using a lambda function. We could define a function similar to our temperature_mapper; however, that is unnecesary as the logic is not that complicated. The key here is the first element of the tuple, the year, and the a and b are the second element of the tuples that are being compared.
  min_temperature = temperatures.reduceByKey(lambda a, b: min(a, b))
  max_temperature = temperatures.reduceByKey(lambda a, b: max(a, b))
```
Unfortunately, averages are a bit more complicated than the clean one-liners we had above. To compute averages, we need to use the method `aggregateByKey`. This method takes three key parameters: `zeroValue`, `seqFunc`, `combFunc`. The `zeroValue` is the initial starting value for an element in the RDD. The `seqFunc` is a function that takes two parameters. The first of which will be the starting value and the second is a new value with a similar key. This function defines how to combine two functions with the same key value. Lastly, the `combFunc` is a function that also takes two parameters. They are of the same type as the `zeroValue`. This function defines how to merge the results of the same key accross reducers.

 ```python
 # Let's define a tuple (accumulated sum, total count) as the zeroValue. Then, for each value with the same year, we will add the temperature of that year and increment the count (seqFunc). Lastly, to merge our results across reducers, we will combine the accumulated sums and total count of the two.
  avg_temperature = temperatures.aggregateByKey((0, 0),
   lambda accumulator, next_value: (accumulator[0] + next_value, accumulator[1] + 1),
   lambda accumulator1, accumulator2: (accumulator1[0] + accumulator2[0], accumulator1[1] + accumulator2[1]))
 ```

 We are almost done. As of right now, we will have data looking like this `[('1949', (13, 1)), ('1950', (94, 2))]`. All we need to do now to get the average is take the accumulated sum, the first value of the tuple, and divide it by the count, the second value of the tuple. To do this, we need the `mapValues` method. This method will pass in all values for a given key one-by-one into the defined function parameter it receives. The key correspondance with the value's passed will be maintained. Therefore, all we need to do at this point is the map each of the computed tuples and perform the division.

 ```python
  avg_temperature = avg_temperature.mapValues(lambda value: value[0] / value[1])
 ```

### Saving Data
 Now that we have data, the next question is how do we save it. This process is not handled automatically like how Hadoop's MapReduce framework would. Fortunately, the command is as simple as the previous commands were. Here is how you would get it done:

```python
  min_temperature = min_temperature.saveAstextFile("hdfs:///tmp/output")
```
 Please note that there are many other variations of the `saveAs` command such as `saveAsHadoopDataset`. Refer to the API docs for me information.

### Submitting a Job
To submit a job in Spark, we need to invoke the versatile Spark submission tool `spark-submit`. This tool takes a variety of optional parameters; however, for what we are interested in, we need to specify, `--master yarn`. This will run the Spark Job off of the `yarn` cluster setup. To submit the job then, all we need to do is this:

```
  $ spark-submit --master yarn temperature.py
```

Please note that `spark-submit` cluster parameter is incompatible with Python files as of right now.

For more information on the pyspark API go [here](https://spark.apache.org/docs/2.1.0/api/python/pyspark.html).
