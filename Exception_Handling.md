# Exception Handling in Spark Data Frames

A simple try catch block at a place where an exception can occur would **not point us to the actual invalid data**, because the execution happens in executors which runs in different nodes and all transformations in Spark are lazily evaluated and optimized by the Catalyst framework before actual computation.

# Handling the Java GC via.  
