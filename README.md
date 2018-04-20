# Jeenk: scalable genomics tools, powered by Apache Flink

## Overview

Jeenk is a collection of parallel, distributed tools for genomics,
written within the Apache Flink data streaming framework and using
Apache Kafka for data movement.

Currently, it consists of three tools:

1. A reader, that converts the proprietary raw Illumina BCL files to
   read-base data (FASTQ-like), which are sent to a Kafka broker for
   storage and further processing.
2. An aligner, that aligns the reads to a reference genome, using the
   JRAPI library, which is powered by the standard BWA-MEM aligner.
3. A CRAM writer, that writes the aligned reads as space-efficient
   CRAM files.

## Compilation

To compile just run `sbt clean assembly`, which will create a
`Jeenk-assembly-X.Y.jar` file, to be fed to the Flink server.

The first compilation may take a long time, since it will download all
the dependencies.

## Configuration

A template configuration file, to be modified by the user, is provided
as `conf/jeenk.conf`.

### BCL Reader

```
flink run -c it.crs4.jeenk.reader.runReader Jeenk-assembly-0.1.jar --properties conf/jeenk.conf
```

### Aligner

```
flink run -c it.crs4.jeenk.aligner.runAligner Jeenk-assembly-0.1.jar --properties conf/jeenk.conf
```
### CRAM Writer

```
flink run -c it.crs4.jeenk.writer.runWriter Jeenk-assembly-0.1.jar --properties conf/jeenk.conf
```

## License

Jeenk is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your
option) any later version.

See COPYING for further details.

For alternative licensing arrangements send inquiries to Gianluigi
Zanetti <gianluigi.zanetti@crs4.it>
