write big enough files
data rows ordered by timestamp
data rows ordered by pv names

how to know which pv goes to which file
number of pvs changes with time
number of pv updates changes with time
some pvs update more frequently than others
use folders and subfolders to store files to improve lookup time

what should folders be
 system names
 section names
 subsection names
 device names

should rows contain split pv name (section, subsection, device, discipline,..) or just
complete pv name

clients will look for data using time (frame) and pv name
clients will apply some criteria / conditions to narrow the amount of results

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