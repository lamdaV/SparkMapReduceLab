# PySpark MapReduce Word Count Task

## Word Count
Suppose you are given a large text file and you want to know how many times each word appears in the text. You also do not care about casing. That is, the count of `wOrD` is the same as `WoRd`. Furthermore, you do not care about ending punctuations `.`, `,`, and `:`. That is, the count of `word:` is the same as `word`. Lastly, you want the output data for each file to be sorted by the count value in descending order. For example:
```
  // If this were to be your initial results,...
  word1, 2
  word2, 4
  word3, 1

  // ...then it should be this
  word2, 4
  word1, 2
  word3, 1
```

Your task is to write a PySpark script in which it will compute word count according to the formentioned criteria.

For this task, a sample text file called [ffxvSummary]() has been provided for you in this directory. Along with this sample text file, there is also an [expectedWordCount]() file provided. It is not expected that there is a single output; however, you can use this expected file to check your results.

Please consult with the API docs [here](https://spark.apache.org/docs/2.1.0/api/python/pyspark.html).

## Hints
  - The RDD operation `flatMap` may be useful.
  - The RDD operation `sortBy` may be useful.

## Summary Source:
  The source of the summary text was taken from [here](http://finalfantasy.wikia.com/wiki/Final_Fantasy_XV).
