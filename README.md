# CS 6240 Assignment 4
Brian Rouse

# Environment and Software Versions

Described in README.txt files included in Spark and MapReduce directories.

# Build and Execution Steps

1. Create an /input directory containing a test file with input data
2. Use the Makefile provided in each project directory (modifying variables as needed):

* Local standalone version:
```
make switch-standalone
make local
```

* Local pseudo-distributed version:
```
make switch-pseudo
make pseudo (make pseudoq for subsequent runs - HDFS  will be set up the first time only)
```

* AWS Elastic MapReduce (EMR) version:
```
make upload-input-aws
make aws
download-output-aws
```

