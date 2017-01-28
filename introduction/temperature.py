from pyspark import SparkContext


def temperature_mapper(line):
    # This is very similar to the first day's Java code with Python list
    # comprehension.

    # Grab the year and convert it from bytestream to string.
    year = str(line[15:19])

    # Grab the temperature and convert it from bytestream to int.
    if (line[87] == "+"):
        air_temperature = int(line[88:92])
    else:
        air_temperature = int(line[87:92])

    # Grab the quality code.
    quality = int(line[92:93])

    # Return a tuple of year and temperature. To further simplify, we will
    # only care about quality codes 1 or 0.
    if (air_temperature != 9999 and (quality == 1 or quality == 0)):
        return (year, air_temperature)


def main():
    # Create SparkContext and load data.
    sc = SparkContext()
    temperature_data = sc.textFile("hdfs:///tmp/input/temperatureSample.txt")

    temperatures = temperature_data.map(temperature_mapper)

    min_temperature = temperatures.reduceByKey(lambda a, b: min(a, b))
    max_temperature = temperatures.reduceByKey(lambda a, b: max(a, b))
    avg_temperature = temperatures.aggregateByKey((0, 0),
        lambda accumulator, next_value: (accumulator[0] + next_value, accumulator[1] + 1),
        lambda accumulator1, accumulator2: (accumulator1[0] + accumulator2[0],
        accumulator1[1] + accumulator2[1]))
    avg_temperature = avg_temperature.mapValues(lambda value: value[0] / value[1])

    min_temperature.saveAsTextFile("hdfs:///tmp/sparkOutput/minTemp")
    max_temperature.saveAsTextFile("hdfs:///tmp/sparkOutput/maxTemp")
    avg_temperature.saveAsTextFile("hdfs:///tmp/sparkOutput/avgTemp")


if __name__ == "__main__":
    main()
