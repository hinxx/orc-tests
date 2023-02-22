# ORC file structure

We need to support the following bits of information:

    * PV name
    * timestamp (ns resolution)
    * scalar integer64, double and string data types (at minimum; possibly other sizes of integers, too)
    * arrays of scalars above (see comment below)


## Notes on arrays

The ORC file format is capable of storing arrays of data for all the data types we would need.
Nevertheless it would use either int64 type under the hood for all integer types and double type
for all floating point types. This means we need to cast each non-int64 element of an array to
int64 element (same for float element cast to double). We can not just memcpy() the original
PV element buffer into the ORC buffer..

This is likely more wasteful in terms of CPU cycles.

hinkokocevar@bd-srv07:/data/data/Code/orc/tests$ gcc -Wall -O3 copy1.c 
hinkokocevar@bd-srv07:/data/data/Code/orc/tests$ ./a.out 8
loop copy of 8 byte data type took 8763679 ns, 8763.679 us
hinkokocevar@bd-srv07:/data/data/Code/orc/tests$ ./a.out 
memcpy of 8 byte data type took 1205879 ns, 1205.879 us

The question is do we need the array of elements to be stored as an array, or could we maybe
treat the array as a blob of bytes (ie. serialized with flatbuffers)? Would such approach
affect the data analysis and/or hinder user interaction with the stored data?

If we assume that majority of the data analysis will involve data lookup by specifying
PV name and time frame, then having arrays as blobs of serialized bytes might not be an
issue. The processing engine would return results to the client and the client would unpack the
blob into actual array of correct type for further use.

On the other hand, if there needs to be some data processing applied to the array elements
(ie. check if any of the array elements has surpassed a threshold level), then the
processing engine would need to unpack the blobs into arrays itself. By doing such check
the client would receive a substantially less data to work with; which is the whole idea
of using a processing engine in the first place..

## Files / folders / sorted data

From 'Spark: The Definitive Guide.pdf':

Ch19, Table partitioning

We discussed table partitioning in Chapter 9, and will only use this section as a reminder. Table
partitioning refers to storing files in separate directories based on a key, such as the date field in
the data. Storage managers like Apache Hive support this concept, as do many of Spark’s built-in
data sources. Partitioning your data correctly allows Spark to skip many irrelevant files when it
only requires data with a specific range of keys. For instance, if users frequently filter by “date”
or “customerId” in their queries, partition your data by those columns. This will greatly reduce
the amount of data that end users must read by most queries, and therefore dramatically increase
speed.
The one downside of partitioning, however, is that if you partition at too fine a granularity, it can
result in many small files, and a great deal of overhead trying to list all the files in the storage
system.

Ch19, The number of files

In addition to organizing your data into buckets and partitions, you’ll also want to consider the
number of files and the size of files that you’re storing. If there are lots of small files, you’re
going to pay a price listing and fetching each of those individual files. For instance, if you’re
reading a data from Hadoop Distributed File System (HDFS), this data is managed in blocks that
are up to 128 MB in size (by default). This means if you have 30 files, of 5 MB each, you’re
going to have to potentially request 30 blocks, even though the same data could have fit into 2
blocks (150 MB total).
Although there is not necessarily a panacea for how you want to store your data, the trade-off can
be summarized as such. Having lots of small files is going to make the scheduler work much
harder to locate the data and launch all of the read tasks. This can increase the network and
scheduling overhead of the job. Having fewer large files eases the pain off the scheduler but it
will also make tasks run longer. In this case, though, you can always launch more tasks than
there are input files if you want more parallelism—Spark will split each file across multiple tasks
assuming you are using a splittable format. In general, we recommend sizing your files so that
they each contain at least a few tens of megatbytes of data.
One way of controlling data partitioning when you write your data is through a write option
introduced in Spark 2.2. To control how many records go into each file, you can specify the
maxRecordsPerFile option to the write operation.

## Partition and Bucket ORC Tables

https://stackoverflow.com/a/51679761