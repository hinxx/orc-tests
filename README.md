# orc tests

wget https://dlcdn.apache.org/orc/orc-1.8.2/orc-1.8.2.tar.gz
tar xf orc-1.8.2.tar.gz

mkdir build-1.8.2
cd build-1.8.2
cmake ../orc-1.8.2 -DBUILD_JAVA=OFF -DCMAKE_BUILD_TYPE=DEBUG -DCMAKE_INSTALL_PREFIX=../install-1.8.2

make -j
make install
cd ..


cd tests
make

# random notes

# Datanode

What it is and what hardware to use?

## https://www.hadoopinrealworld.com/namenode-and-datanode/

DataNode is usually configured with a lot of hard disk space. Because the actual data is stored in the DataNode.

Often the term “Commodity Computers” is misunderstood. Commodity Computers or Nodes does not mean cheap or less powerful hardware, it just means in-expensive computer and deemphasize the need for specialized hardware.

Sample datanode config:

	Processors: 2 Quad Core CPUs running @ 2 GHz
	RAM: 64 GB
	Disk: 12-24 x 1TB SATA
	Network: 10 Gigabit Ethernet


## https://www.redhat.com/en/resources/data-node-configurations-for-openshift-overview


Table 1. Data node configurations optimized for edge computing

 						Base configuration (10TB)							Plus configuration (20TB)
Platform										Single 2U node
CPU						2x Intel Xeon Gold 5318Y processor (24 cores)		2x Intel Xeon Gold 5318Y processor (24 cores)
Memory							96GB											192GB
Data network						2x Intel Ethernet Network Adapter 810-CQDA2 (10GbE)
Management network	  				1x Intel Ethernet Connection X710-DA2 (10GbE)
Metadata cache	  		1x Intel Optane SSD P5800 (400GB)	  				1x Intel Optane SSD P5800X (800GB)
Storage media	  		6x SSD (1.92TB, 2.5-inch SATA, TLC)					6x SSD (3.84TB, 2.5-inch SATA, TLC)


Table 2. Capacity-optimized data node configurations
 						Base configuration (30TB)							Plus configuration (60TB)
Platform										Single 2U node
CPU						2x Intel Xeon Gold 5320 processor (26 cores)		2x Intel Xeon Gold 6330 processor (24 cores)
Memory							96GB											192GB
Data Network	  					2x Intel Ethernet Network Adapter 810-CQDA2 (25 GbE)
Management network					1x Intel Ethernet Connection X710-DA2 (10GbE)
Metadata cache	  		1x Intel Optane SSD P5800X (800GB)	  				2x Intel Optane SSD P5800X (800GB)
Storage media			8x SSD (3.84TB, 2.5-inch SATA, TLC)					16x SSD (3.84TB, 2.5-inch SATA, TLC) 
																			or 8x SSD (7.68TB, 2.5-inch SATA, TLC)

Table 3. I/O performance-optimized data node configurations

 						Base configuration (15TB)								Plus configuration (30TB)
Platform											Single 2U node
CPU						2x Intel Xeon Gold 6338 processor (32 cores)			2x Intel Xeon Gold 6338 processor (32 cores)
Memory								192GB												384GB
Data network			2x Intel Ethernet Network Adapter E810-CQDA2 (50GbE)	2x Intel Ethernet Network Adapter E810-CQDA2 (100GbE)
Management network					1x Intel Ethernet Connection X710-DA2 (10GbE)
Metadata cache			2x Intel Optane SSD P5800X (800GB)						2x Intel Optane SSD P5800X (1.6TB)
Storage media			4x SSD (3.84TB, 2.5-inch U.2 NVMe, TLC)					8x SSD (3.84TB, 2.5-inch U.2 NVMe, TLC)


## https://docs.cloudera.com/documentation/enterprise/release-notes/topics/hardware_requirements_guide.html

## https://www.upgrad.com/blog/hive-vs-spark/

Q: Do we need Hive and Spark?

Differences between Apache Hive and Apache Spark

Usage: – Hive is a distributed data warehouse platform which can store the data in form of tables like relational databases whereas Spark is an analytical platform which is used to perform complex data analytics on big data.
File Management System: – Hive has HDFS as its default File Management System whereas Spark does not come with its own File Management System. It has to rely on different FMS like Hadoop, Amazon S3 etc.
Language Compatibility: – Apache Hive uses HiveQL for extraction of data. Apache Spark support multiple languages for its purpose.
Speed: – The operations in Hive are slower than Apache Spark in terms of memory and disk processing as Hive runs on top of Hadoop.
Read/Write operations: – The number of read/write operations in Hive are greater than in Apache Spark. This is because Spark performs its intermediate operations in memory itself.
Memory Consumption: – Spark is highly expensive in terms of memory than Hive due to its in-memory processing.
Developer: – Apache Hive was initially developed by Facebook, which was later donated to Apache Software Foundation. Apache Spark is developed and maintained by Apache Software Foundation.
Functionalities: – Apache Hive is used for managing the large scale data sets using HiveQL. It does not support any other functionalities. Apache Spark provides multiple libraries for different tasks like graph processing, machine learning algorithms, stream processing etc.
Initial Release: – Hive was initially released in 2010 whereas Spark was released in 2014.



## https://netflixtechblog.com/revisiting-1-million-writes-per-second-c191a84864cc

Nodes: 		i2.xlarge	4	30.5 GiB	1 x 800 GB	

Clients: 	r3.xlarge	4	30.5		1 x 80	


## AWS testing

Test file writing by simulating N machines as data producers, trying to send
data to M machines that do file writing through Apache Kafka.

Test analitics by simulating N machines as clients, trying to schedule jobs
on M machines that run Apache Spark.

https://calculator.aws/
https://aws.amazon.com/ec2/instance-types/

Pay for 100 hours a week

M machines:
	i3.xlarge	4	30.5 GiB	Up to 10 Gigabit	1 x 950 NVMe SSD	$0.359/Hour  x 100 hours x 5 instances  = $179

N machines:
	r5.large	2	16 GiB		Up to 10 Gigabit	EBS only			$0.142/Hour  x 100 hours x 15 instances = $213
	r5.xlarge	4	32 GiB		Up to 10 Gigabit	EBS only			$0.284/Hour  x 100 hours x 15 instances = $426



Pay for 100 hours a week for 5x of i3.xlarge, 15x of r5.xlarge

i3.xlarge: $0.359/Hour x 100 hours x 5 instances  = $179

r5.large:  $0.142/Hour x 100 hours x 15 instances = $213
r5.xlarge: $0.284/Hour x 100 hours x 15 instances = $426

