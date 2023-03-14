write big enough files
data rows ordered by timestamp
data rows ordered by pv names

ingestion stage pulls data from kafka topic(s) and sorts data in memory tables table by timestamp and pv name
the data in memory table should contain smallest possible variation in pv names
memory table size would equal the stripe/row group in the file
multiple memory table contents are written to the same file
pv names in distinct memory tables can be different


how to know which pv goes to which file
number of pvs changes with time
number of pv updates changes with time
some pvs update more frequently than others
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