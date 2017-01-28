# MapReduce with Spark:
## Objective:
From this introduction, a student should begin to understand how to translate their understanding of Hadoop’s MapReduce framework to Spark’s MapReduce Framework. Once completed, the student will have an adequate amount of understanding of Spark’s MapReduce framework.

## Requirements
- Hadoop Cluster with Spark installed
- Minimal Knowledge of Python
- PySpark

## Background:
### Initialization:
Before we dive into the code, we must first understand how Spark is structured. To use Spark, we must import the SparkContext object. The SparkContext object requires a SparkConf object. In python, it looks like this:

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

## Example:
### Loading data:
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

### Mapping Phase:
Now that data is loaded, we need to a mapper to extract what we really care about: the year and temperature. To do so, we will create a python local function.
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

### Reduce Phase:
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

 ### Saving Data:
 Now that we have data, the next question is how do we save it. This process is not handled automatically like how Hadoop's MapReduce framework would. Fortunately, the command is as simple as the previous commands were. Here is how you would get it done:

```Python
  min_temperature = min_temperature.saveAstextFile("hdfs:///tmp/output")
```
 Please note that there are many other variations of the `saveAs` command such as `saveAsHadoopDataset`. Refer to the API docs for me information.

### Submitting a Job:
To submit a job in Spark, we need to invoke the versatile Spark submission tool `spark-submit`. This tool takes a variety of optional parameters; however, for what we are interested in, we need to specify, `--master yarn`. This will run the Spark Job off of the `yarn` cluster setup. To submit the job then, all we need to do is this:

```
  $ spark-submit --master yarn temperature.py
```

Please note that `spark-submit` cluster parameter is incompatible with python files as of right now.

For more information on the pyspark API go [here](https://spark.apache.org/docs/2.1.0/api/python/pyspark.html).
