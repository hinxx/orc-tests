# requirements

* write big enough files
* data rows ordered by timestamp
* data rows ordered by pv names (optional?)

# schema A: using kafka

ingestion stage pulls data from kafka topic(s) and sorts data in memory tables table by timestamp and pv name
the data in memory table should contain smallest possible variation in pv names
memory table size would equal the stripe/row group in the file
multiple memory table contents are written to the same file
pv names in distinct memory tables can be different

update:

we need to use a tool that would forward the PV updates to kafka (CA client)
the forwarder tool will need to be supplied with the defined set of PVs it will monitor
forwarder then serializes and pushes the PV updates one by one into kafka topic
we need to configure PVs to be saved in kafka topics
 how many topics? 1 pv == 1 topic seems unnecessary and will impose huge load on kafka for handling topics
 how to partition pvs into topics? by system, by section, by data type,..?

# schema B: not using kafka, but homemade 'logger'

alternative schema without kafka in the loop

we still need to have a CA client that would monitor the PVs
we can use similar homemade tool to forwarder (logger) that would listen to CA updates and serialize the data (+ metadata)
into files (fast writing, no sorting, aka write-ahead-log like format)
we still need to define a set of PVs that the tool would listen to / work with
one logger would listen to about 50000 PVs (intermixed scalar and waveform)
several of loggers could be ran in parallel to handle the workload (each writing to its own files, possibly
on separate SSD disks)
writing to log files should be done such that data is persisted as soon as possible in order to avoid data loss
persisting should be done in an efficient manner (sized and aligned I/O)

once we have enough of these intermediate files we can start reading them, sorting the data by timestamp and pv name
and then writing that sorted data to large ORC files


# schema C: writing directly to ORC

seems we need to define a known set of PVs no matter what (as in previous schemas above)
if the set of PVs is defined such that the PV names are not too far apart in names then we could relax the
requirement of ordering of the data by PV name (???); not sure how important it would be to keep the data sorted
on two "keys": timestamp and pv name
the pv name "dictionary" will small enough and bounded by design
the data will be ordered by the timestamp that will be bounded with PV timestamps seen between ORC file open / close
writing directly to ORC files imposes certain risks of data loss if the writer dies before properly closing the file!
pv sets of different writers must not overlap

# random

how to know which pv goes to which file
 we need to tell the ingestion tools forwarder/logger/writer (depending in which schema we are) which PVs it uses upfront ie. we
 can not expect that one instance of the tool could handle all the PVs we would have
 one tool could manage about 50000 PV

number of pvs changes with time
 the ingestion tools will need to be dynamically configured to balance the load
 as new PV come the existing defined sets need to be redefined and redeployed to the ingestion tools
 rebalancing might be an "expensive" and intrusive operation and should be dealt with caution

number of pv updates changes with time
some pvs update more frequently than others
 this should not matter to the ingestion tools as long as there are enough PVs that would update fast enough to
 write large ORC files in short enough time


update: not applicable?
pv data is sorted in memory table by timestamp and pv name thus we could write small ranges of pv names to the file
 - what is the desired size of the range before it is written to the file -




use folders and subfolders to store files to improve lookup time

what should folders be
 system names
 section names
 subsection names
 device names

should rows contain split pv name (section, subsection, device, discipline,..) or just complete pv name

clients will look for data using time (frame) and pv name
clients will apply some criteria / conditions to narrow the amount of results

does it make sense to collect data from pvs (in one query) that are not reporting the same measurement?
what is the purpose in taking out bunch of different value and try to do something with them; analysis is not
possible on the whole data set returned.. or is it?

Even if the data type is the same, selecting random pvs and their stored data values
is not really useful.



kafka buffers several days worth of data
 this allows for creating writer data sets that can meet criteria mentioned above
  check the kafka logs
  determine the amount of data that could be persisted to orc files
  create timestamp and pv name sorted data sets for complete orc file(s)
  write data to file(s)
  remove written data from kafka

how to handle residue data at the end of run / before shutdown?
 just write the remainder of the data sorted to as many files as needed
 might not exhibit the same pv name span or timestamp span compared to other files