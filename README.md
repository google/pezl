# Name - pezl

split/cat for Google Cloud Storage objects.

# Project status
pezl is a MVP from field personnel and will be updated on an as-needed basis.

# Description
pezl is a command line Golang application to split Google Cloud Storage (GCS) object files. It is a useful tool to preprocess data that is better suited for parallel consumption of data as opposed to a single-threaded download, e.g. Hadoop/Spark. 

For example, data to be consumed by Spark arrives as a single file, 100GB. Spark will use a single worker to download and read into memory the file, despite have a cluster of workers. A traditional approach to this problem is to spin up a GCE VM instance, download the file via gsutil, and then upload the split output files to GCS for Spark to consume in parallel. 

pezl is designed to resemble the interface to the Linux [split](https://man7.org/linux/man-pages/man1/split.1.html) tool.

## Performance
Benchmarking the 'Download-Split-Upload' process described above takes ~40 minutes to complete on a n2-standard-16 VM Instance with a 100GB file. Pezling the same file takes less than 4 minutes, a 10X performance improvement.

# Definitions
## pezl - to split an object into smaller objects, called pezlings
## unpezl - to concatenate pezlings into a single, larger object

# Installation

1. [Install Golang 1.17](https://golang.org/doc/install) (current) on a GCE VM instance.
2. [Setup a pezl project] (https://golang.org/doc/code)
3. Build the pezl executable - `go build -o pezl`


# Usage
```
./pezl --help
Usage of pezl:
  -a int
    	use suffixes of length N (default 2 which provides 676 output files) (default 2)
  -b int
    	put exactly SIZE bytes per output file, default is 1M (default 1048576)
  -d	use numeric suffixes instead of alphabetic
  -h	display this help and exit
  -l	put approximately SIZE bytes of lines per output file (ends pezlings on the first EOL after nSIZE bytes
  -s int
    	specify a single pezling to be created. pezl -s 1 would only create a single pezling from byte 1 to byte 1M.  This is often called in a distributed fashion.
  -t int
    	Specify the number of concurrent goroutines ('threads') to use. (default 100)
  -u	Unpezl (concatenate) files created by pezl
  -v	prints diagnostics while pezling
```


# pezl by bytes - for binary files
`pezl gs://[bucket/key/to/very_large_file] gs://[bucket/key/to/pezling_file_prefix]`

Example on n2-standard-16 (64GB memory): 
- `./pezl -b 1048576000 -t 16 gs://pezl-testing/test/very_large_file gs://pezl-testing/test/segment`
- very_large_file is 99.1GB in size 
- pezling_file_prefix is 'segment'
- each pezling will be 1GB (1048576000 bytes)
- 16 pezl threads to not bust out memory ...
- pezl will produce 102 1GB pezlings named segment_aa, segment_ab, ..., segment_dx in roughly 3 minutes

# pezl by line-bytes - for text files

- `./pezl -b 10485760 -l gs://pezl-testing/test/very_large_file.txt gs://pezl-testing/test/segment`
- very_large_file.txt is 41.1MB in size
- pezling_file_prefix is segment
- each pezling will be 10MB (10485760)
- threads are not specified but will only use as many as needed if less than 100
- pezl will produce 4 10MB files named segment_aa, .., segment_ad and 1 1.1MB file named segment_ae

# unpezl - all pezlings with specified prefix concatenated into a single file
- `./pezl -u gs://pezl-testing/test/segment`
- segment is the prefix for the pezlings which are to be smushed together into a single file
- pezl will produce a single file named 'segment' which is the result of concatenating all files within the directory which appear to be a pezling, e.g. segment_aa, segment_ab. Pezlings are concatenated lexicographically, i.e. .._aa > .._ab > .._ac.

# Authors and acknowledgment
Tim Meyer (baldtim@google.com) and John Stamper (clowndaddy@google.com)

# License
[Apache-2.0](https://www.apache.org/licenses/LICENSE-2.0.txt)

# Disclaimer
This is not an officially supported Google product
