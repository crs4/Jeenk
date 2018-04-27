# Jeenk configuration

Please see the [README file](README.md) for a general overview of Jeenk.

## Generalities

Jeenk expects a mandatory properties file, passed via the
`--properties` parameter, as in

```
flink run -c it.crs4.jeenk.reader.runReader Jeenk-assembly-0.1.jar --properties conf/jeenk.conf
```

This file contains the parameters required by Jeenk, one per line,
e.g.,

```
bcl_input_dir = hdfs:///path/to/illumina/dir/
reference = /tmp/ref/some.fasta
kafka_server = 127.0.0.1:9092
reader_flinkpar = 1
[...]
```

The parameters can also be specified via the command line, which takes
precedence over the properties file:

```
flink run -c it.crs4.jeenk.reader.runReader Jeenk-assembly-0.1.jar --properties conf/jeenk.conf --reference /tmp/ref/some.other.fasta
```

If you want to pass all the parameters via the command line (e.g., for
scripting purposes) you may use an empty properties file, or just
`/dev/null`.

### Mandatory parameters

Jeenk expects the following mandatory parameters:

- `bcl_input_dir` : the input directory for the BCL files. It can be
  either local (`file:///path/to/illumina/dir/`) or on HDFS
  (`hdfs:///path/to/illumina/dir/`)
- `cram_output_dir` : the output directory for the CRAM files. It can
  local or on HDFS.
- `reference` : Path to reference file. It must be local in all nodes
  and specified with just the path (i.e., `/tmp/ref/some.fasta`)
- `flink_tmp_dir` : Temp directory for the Flink State Backend, can be
  either local or on HDFS.

### General parameters

Jeenk will then try and guess the following parameters, but it might
be a good idea to set them explicitly

- `flink_server` : Host and port of the Flink server. It is used to
  get the number of slots and Taskmanagers. **Default:**
  "localhost:8081"
- `num_nodes` : number of nodes in the Flink cluster. **Default:**
  number of Flink Taskmanagers.
- `cores_per_node` : number of CPU cores in each node. **Default:**
  Flink total slots / Flink Taskmanagers
- `mem_per_node` : Available RAM (in MB) on each node: **Default:**
  12000

These parameters are used to choose an efficient parallelization for
each tool. However, it is possible (and recommended to achieve maximum
performance) to directly set each level of parallelism, as explained
in the following sections.

### Kafka server and topics

Instead of writing files at each processing step, Jeenk uses Apache
Kafka as a mean to transmit data between its components. This approach
brings scalability, composability and allows a better decoupling of
the processing steps, which can also be run concurrently, with data
streaming seamlessly through the processing pipeline.

The parameters regarding Kafka are:

- `kafka_server` : host and port of the Kafka server. **Default:**
  127.0.0.1:9092
- `kafka_prq` : the prefix of the topics which will contain the
  read-based data. **Default:** flink-prq
- `kafka_aligned` : the prefix of the topics which will contain the
  aligned data. **Default:** flink-aligned

When having Kafka distributed on many nodes/disks it is good practice
to try and and spread the data uniformly among the disks. For this to
happen, the number of partitions of the Kafka topics should be equal
to (or a multiple of) the total number of disks used by the Kafka
server (e.g., if Kafka is ditributed on 3 nodes and using 2 disks per
node, one could choose to use 6 or 12 partitions per topic). Jeenk's
reader and aligner allow to choose the number of partitions by setting
the `reader_kafka_fanout` and `aligner_kafka_fanout` options (see
below).

## BCL reader

The reader takes care of converting the proprietary raw Illumina BCL
files directly from the sequencer's run directory into read-based data
(FASTQ-like), which are sent to a Kafka broker for storage and further
processing (akin to Illumina's *bcl2fastq2*).

The parameters which affect the BCL reader are:

- `num_readers` : Maximum number of concurrent Flink
  jobs. **Default:** `num_nodes`
- `reader_flinkpar` : Parallelism of each Flink job. **Default:** 1
- `reader_grouping` : How many tiles group together into a single
  Flink job. The default ensures a minimum of 8GB per
  subtask. **Default:** min(`mem_per_node`/8000, `cores_per_node`)
- `reader_kafka_fanout` : Number of partitions per each output Kafka
  topic. Increase if needed. **Default:** 1
- `sample_sheet` : Location of the sample sheet. **Default:**
  `bcl_input_dir` + `SampleSheet.csv`
- `adapter` : Adapter to be trimmed (if present). **Default:** ""
- `mismatches` : Allowed number of mismatches. **Default:** 1
- `undet` : Prefix for files containing non-aligned
  reads. **Default:** `Undetermined`
- `base_dir` : Location of BaseCalls directory, relative to
  `bcl_input_dir`. **Default:** `Data/Intensities/BaseCalls/`
- `block_size` : Transposition block size (do not edit unless you know
  what you're doing). **Default:** 2048

**Note:** This processing step opens in parallel *many* files, using a
large number of threads, hence it might be necessary to increase the
the maximum number of allowed user processes in the system (e.g.,
`ulimit -u 65536`). If you want to limit the number of threads, you
can decrease the `reader_grouping` parameter.

### Running the reader

```
flink run -c it.crs4.jeenk.reader.runReader Jeenk-assembly-0.1.jar --properties conf/jeenk.conf
```

## Aligner

The aligning of the reads to a reference genome is done using the
BWA-MEM plugin, through the RAPI library
(http://github.com/crs4/rapi/). The aligner presents a deep hierarchy
of parallelism: we can tune the number of Flink jobs, the parallelism
and the number of tasks within each Flink job, and finally the
parallelism of the RAPI library.

- `numAligners` : Maximum number of concurrent Flink
  jobs. **Default:** num_nodes
- `aligner_flinkpar` : Parallelism of each Flink job. Increase only if
  there are more Taskmanagers than `numAligners`. **Default:** 1
- `aligner_grouping` : Number of tasks grouped into a Flink
  job. **Default:** 3
- `rapi_par` : RAPI parallelism. **Default:** max(1, cores_per_node /
  aligner_flinkpar)
- `rapi_win` : RAPI window size. **Default:** 3360
- `aligner_kafka_fanin` : Parallelism of Kafka readers. Note that we
  must have `aligner_kafka_fanin` ≤
  `reader_kafka_fanout`. **Default:** 1
- `maxpar` : Maximum parallelism for the writing phase. **Default:**
  min(mem_per_node/12000, cores_per_node)
- `aligner_kafka_fanout` : Number of partitions per each output Kafka
  topic. Increase if needed. **Default:** num_nodes *
  maxpar / min(4, num_nodes * maxpar / cores_per_node)
- `aligner_timeout` : If the aligner does not detect new data within
  `aligner_timeout` seconds it will exit. If set to 0 it waits
  indefinitely. **Default:** 0

**Note:** Currently, the RAPI library is *not* thread-safe and hence
calls to it are *synchronized*, to prevent thread interference. Thus
we recommend to run one aligning Flink job per Taskmanager, since more
jobs will not improve the performance. However, if running on
machines with large memory it is possible to have multiple
Taskmanagers running on the same node. For example, if running on 16
nodes, each with 60 GB of RAM available to Flink, we could run 6
Taskmanagers per node, each using 10 GB of RAM, and run the aligner
with parameters `numAligners = 16` and `aligner_flinkpar = 6`,
boosting the performance by a factor 6.

### Running the aligner

```
flink run -c it.crs4.jeenk.aligner.runAligner Jeenk-assembly-0.1.jar --properties conf/jeenk.conf
```

## CRAM writer

This tools writes the aligned reads as space-efficient CRAM files.

- `numWriters` : Maximum number of concurrent Flink jobs. **Default:**
  min(4, num_nodes)
- `writer_flinkpar` : Parallelism of each Flink job. **Default:**
  aligner_kafka_fanout
- `writer_kafka_fanin` : Parallelism of Kafka readers.  Note that we
  must have `writer_kafka_fanin` ≤
  `aligner_kafka_fanout`. **Default:** min(writer_flinkpar,
  aligner_kafka_fanout)
- `writer_grouping` : Number of tasks grouped into a Flink
  job. **Default:** 4
- `writer_timeout` : If the writer does not detect new data within
  `writer_timeout` seconds it will exit. If set to 0 it waits
  indefinitely. **Default:** 0

### Running the CRAM writer

```
flink run -c it.crs4.jeenk.writer.runWriter Jeenk-assembly-0.1.jar --properties conf/jeenk.conf
```
