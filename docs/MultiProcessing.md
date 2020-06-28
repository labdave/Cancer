# FASTQ Files Multi-Processing
The programs are designed to be executed on a server with multiple CPUs, such as the VM instances on GCP/AWS/Azure, or a Kubernetes node with multiple CPUs. A 16 CPU machine is recommended for processing a single pair of compressed FASTQ (fastq.gz) files. Higher number of CPUs may not be fully utilized due to the bottleneck of decompressing a single file. Programs like demultiplexing are capable of processing multiple pairs of FASTQ files for the same sample at the same time. The bottleneck generally shifts to the speed of the hard disk as the reads are coming from more pairs of FASTQ files.

## The Bottlenecks
In general, there are 3 types resources for computational intensive data processing: CPU, memory and disk performance. With cloud computing, we can easily scale up the CPU and memory in a VM. However, the disk performance is usually not so scalable for a single VM. We can certainly connect multiple SSDs and build a RAID system to achieve high disk performance, but such configurations are usually non-trivial and beyond the discussion of designing a general multi-processing program.

While there is no special design needed for a program to use more memory, most Python programs are running on a single CPU unless they are designed for multi-threading/multi-processing. For single thread programs, CPU is likely to be the bottleneck when we need to do heavy processing. This program uses the `multiprocessing` module to process the data with multiple CPUs. As we add more CPUs, eventually disk performance will become the bottleneck. For processing FASTQ files, since they are large, disk performance is usually limited by the sequential read/write speed.

## Reading Compressed FASTQ Files
FASTQ files usually comes in compressed format (fastq.gz). To process paired-end FASTQ files, we need to read and decompress two files sequentially at the same time. For single thread programs, the decompression can be the bottleneck as it is significantly slower than the file reading speed. This issue can also be easily overlooked in Python tools as there is no native support in Python for multi-threading decompression.

In this program, the [dnaio](https://github.com/marcelm/dnaio/) packages is used to read FASTQ files. Behind the scene it uses [xopen](https://github.com/marcelm/xopen/) to open compressed files. The `xopen` module uses [pigz](https://zlib.net/pigz/) to exploit multi-threading for compressing and decompressing data for a single file.

## Identifying the Bottleneck
When processing FASTQ file, the best situation is to keep both CPU and disk utilization close to 100%. Assuming disk performance is not scalable, disk utilization will be lower if there is not enough CPUs for data decompression or processing. When disk utilization is 100%, adding more CPUs is likely not helpful. However, practically, there are overheads for adding more process to use more CPUs. The optimal number of CPUs will depend on the application and the actual disk performance. More experiments are needed to tweak the number threads for reading FASTQ files (pigz parameters).