We have a huge log file structured similar to inputfile.txt. Our aim is segregate those logs based on
first column in the file. Excel is not capable of keeping formatting and exporting the log file into Excel
is time consuming for developer.

This simple mapreduce code segregates the log file into separate log files based on the value in the first 
column. It can take in number of log files as input and produce desired output.