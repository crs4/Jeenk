# Jeenk: scalable genomics tools, powered by Apache Flink

## Overview

Jeenk is a collection of parallel, distributed tools for genomics,
written within the Apache Flink data streaming framework and using
Apache Kafka for data movement.

Currently it consists of three Flink-based tools:

* A reader, that reads the proprietary raw Illumina BCL files directly
  from the sequencer's run directory and converts them to read-based
  data (FASTQ-like), which are sent to a Kafka broker for storage and
  further processing (akin to Illumina's `bcl2fastq2`);
* An aligner, that aligns the reads to a reference genome using the
  BWA-MEM plugin through the RAPI library
  (http://github.com/crs4/rapi/);
* A CRAM writer, that writes the aligned reads as space-efficient CRAM
  files.

## Compilation

This software has been tested with Apache Flink 1.4 and Java 8.

To compile just run `sbt clean assembly`, which will create a
`Jeenk-assembly-X.Y.jar` file, to be fed to the Flink server.

The first compilation may take a long time, since it will download all
the dependencies.

## Configuration

A template configuration file is provided as `conf/jeenk.conf`. The
file must be edited with the parameters of your Flink and Kafka
configurations.

See the [configuration guide](CONFIGURATION.md) for details on how to
configure and run Jeenk tools.

To setup Flink and Kafka clusters, see the projects' documentation.

## License

Jeenk is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your
option) any later version.

See COPYING for further details.

For alternative licensing arrangements send inquiries to Gianluigi
Zanetti <gianluigi.zanetti@crs4.it>

## Further Reading

- F. Versaci, L. Pireddu and G. Zanetti, *Kafka interfaces for
  composable streaming genomics pipelines,* 2018 IEEE EMBS
  International Conference on Biomedical & Health Informatics (BHI),
  Las Vegas, NV, USA, 2018, pp. 259-262.  doi:10.1109/BHI.2018.8333418
  [URL](http://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=8333418&isnumber=8333343)

- F. Versaci, L. Pireddu and G. Zanetti, *Scalable genomics: From raw
  data to aligned reads on Apache YARN,* 2016 IEEE International
  Conference on Big Data (Big Data), Washington, DC, 2016,
  pp. 1232-1241.  doi:10.1109/BigData.2016.7840727
  [URL](http://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=7840727&isnumber=7840573)

