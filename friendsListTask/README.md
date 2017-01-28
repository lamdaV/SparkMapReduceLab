# PySpark MapReduce Lab:

## Friends List Revisited:
Suppose you are running a social network and you want to compute a batch process at the end of every day to find the friends A and B have in common. Your data looks like this:
```
  A,B,C,D       // A has friends B, C, D
  B,A,D         // B has friends D
```
You need to compute for each person the friends they have in common. For example, for the above input, the output should look like this.
```
  ((A, B), (D)) // A and B have friend D in common
```
Your task is to write a PySpark script in which it will compute the friends in common.

For simplicity, you may also assume that if A has B as a friend, B also has A as a friend.

## Hints:
  - It may be helpful to run multiple map task on the same input.
  - Define local functions rather than lambdas.
  - Work with tuples rather than lists.
