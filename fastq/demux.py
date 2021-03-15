import os
import csv
import math
import dnaio
import parasail
import editdistance
import re
import logging
import datetime
from .processor import FASTQProcessor, FASTQWorker
from ..fastq_pair import ReadPair
from ..fastq_file import IlluminaFASTQ, BarcodeStatistics
logger = logging.getLogger(__name__)


class DemultiplexWriter(dict):
    """A dictionary subclass holding barcode adapters and
    the corresponding file-like objects for writing fastq.gz files.

    This class is designed to write read pairs into FASTQ files based on the barcode.

    In the dictionary:
    Each key is a barcode.
    The actual filenames are specified by the paired_end_filenames() method.
    Each value is a file-like object returned by opening a pair of fastq.gz files.

    Attributes:
        barcode_dict: A dictionary mapping barcode to filename prefix.
        prefix_dict: A dictionary mapping filename prefix to file-like object.
            prefix_dict can be used to determine the files with certain prefix are opened.

    This class supports context manager, for example:
        with DemultiplexWriter(barcode_dict) as writer:
            ...PROCESSING CODE HERE...
            writer.write(BARCODE, READ1, READ2)
            ...

    """

    BARCODE_NOT_MATCHED = "NO_MATCH"

    @staticmethod
    def paired_end_filenames(prefix):
        """Maps a prefix to a 2-tuple of filenames (R1, R2)
        This static method defines the output filenames.

        Args:
            prefix (str): Prefix for the filenames, including the full path.

        Returns: A 2-tuple of strings as the filenames for R1 and R2 FASTQ files.

        """
        return prefix + ".R1.fastq.gz", prefix + ".R2.fastq.gz"

    def __init__(self, barcode_dict):
        """Initializes the writer with a dictionary mapping barcode to filename prefix.

        Args:
            barcode_dict: A dictionary mapping barcode to filename prefix
                Each key is a barcode.
                Each value is a prefix for output filename, including the full path.
                The output file will contain the reads corresponds to the barcode.
                If multiple barcodes are mapping to the same prefix,
                    the reads with those barcodes will be written into the same output file pair.

        """
        self.barcode_dict = barcode_dict
        self.prefix_dict = {}
        super().__init__()

    def open(self):
        """Opens the files for writing
        """
        for barcode, prefix in self.barcode_dict.items():
            if not prefix:
                self[barcode] = None
            if prefix in self.prefix_dict.keys():
                self[barcode] = self.prefix_dict[prefix]
            else:
                r1_out, r2_out = DemultiplexWriter.paired_end_filenames(prefix)
                fp = dnaio.open(r1_out, file2=r2_out, mode='w')
                self.prefix_dict[prefix] = fp
                self[barcode] = fp
        return self

    def close(self):
        """Closes the files
        """
        for fp in self.values():
            fp.close()

    def write(self, barcode, read1, read2):
        fp = self.get(barcode)
        if not fp:
            return
        fp.write(read1, read2)

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()


class DemultiplexWorker(FASTQWorker):
    """Represents a worker process for demultiplexing FASTQ reads
    """
    DEFAULT_ERROR_RATE = 0.1

    def __init__(self, barcode_dict, error_rate=None, score=1, penalty=10):
        """Initialize a demultiplex worker process.

        Args:
            barcode_dict: A dictionary mapping barcode to filename prefix.
                The barcode_dict is used to initialize DemultiplexWriter.
            error_rate: Max error rate allowed for a read to be considered as matching a barcode.
                error_rate is used to determine the max distance allowed between the barcode and the read.
            score: Score for one base pair match.
            penalty: Penalty for one unit distance.
        """
        super().__init__()
        self.barcode_dict = barcode_dict
        self.adapters = list(barcode_dict.keys())
        self.min_match_length = round(min([len(adapter) / 2 for adapter in self.adapters]))

        # Set the default values
        self.error_rate = error_rate if error_rate else self.DEFAULT_ERROR_RATE
        self.score = int(score) if str(score).isdigit() else 1
        self.penalty = int(penalty) if str(penalty).isdigit() else 10
        if not self.penalty:
            raise ValueError("Mismatch penalty must be greater than 1.")
        logger.debug("Process %s, Penalty: %s, Error Rate: %s, Score: %s" % (
            os.getpid(), self.penalty, self.error_rate, self.score
        ))

        self.score_matrix = self.create_score_matrix()

    def create_score_matrix(self):
        """Creates a parasail score matrix for alignment
        """
        return parasail.matrix_create("ACGTN", self.score, -1 * self.penalty)

    def semi_global_distance(self, s1, s2):
        score_matrix = self.create_score_matrix()
        result = parasail.sg_de_stats(
            s1, s2, self.penalty, self.penalty, score_matrix
        )
        return (self.score * result.matches - result.score) / self.penalty

    def start(self, in_queue, out_queue):
        """Starts the demultiplexing to process reads from in_queue.
        The number of reads processed are put into the out_queue for counting purpose.

        Args:
            in_queue: A queue holding list of reads to be processed.
                Each item in the in_queue is a list reads so that the frequency of access the queue are reduced.
            out_queue: A queue holding integers for counting purpose.

        Returns:

        """
        active_time = datetime.timedelta()
        batch_count = 0
        with DemultiplexWriter(self.barcode_dict) as writer:
            while True:
                reads = in_queue.get()
                # Keep the starting time for each batch processing
                timer_started = datetime.datetime.now()
                if reads is None:
                    batch_time = (active_time / batch_count) if batch_count else 0
                    logger.debug("Process %s, Active time: %s (%s batches, %s/batch)." % (
                        os.getpid(), active_time, batch_count, batch_time
                    ))
                    return self.counts
                # results = []
                for read_pair in reads:
                    barcode, read1, read2 = self.process_read_pair(read_pair)
                    writer.write(barcode, read1, read2)
                    # results.append(result)

                self.add_count('total', len(reads))
                batch_count += 1
                # Add processing time for this batch
                active_time += (datetime.datetime.now() - timer_started)
                out_queue.put(len(reads))

    def process_read_pair(self, read_pair):
        """Process the read pair

        Sub-class should implement this method to return a 3-tuple, i.e.
            (BARCODE, READ1, READ2)

        """
        raise NotImplementedError


class DemultiplexInlineWorker(DemultiplexWorker):
    """Demultiplex FASTQ reads by Inline barcode at the beginning of the reads

    self.counts will be a dictionary storing the demultiplex statistics, which includes the following keys:
        matched: the number of read pairs matched at least ONE of the adapters
        unmatched: the number of read pairs matching NONE of the adapters
        total: total number of read pairs processed
        Three keys for each adapter, i.e. BARCODE, BARCODE_1 and BARCODE_2
        BARCODE stores the number of reads matching the corresponding adapter.
        BARCODE_1 stores the number of forward reads (pair 1) matching the corresponding adapter.
        BARCODE_2 stores the number of reverse-compliment reads (pair 2) matching the corresponding adapter.

        For the values corresponding to BARCODE keys,
            each READ PAIR will only be counted once even if it is matching multiple adapters.
            The longer barcode will be used if the two reads in a pair is matching different adapters.

        For the values corresponding to BARCODE_1 and BARCODE_2 keys,
            each READ will be counted once.
            The first barcode in self.adapters will be used if the read is matching multiple adapters.

    """
    DEFAULT_ERROR_RATE = 0.2

    def trim_adapters(self, read1, read2):
        """Checks if the beginning of the reads in a read pair matches any adapter.
        If so, trim the reads to remove the matching adapter.

        A read and an adapter are matched by using semi-global alignment without penalty at the end of the read.
        They are considered as MATCHED if the number of substitutions and gaps

        Args:
            read1: Forward read.
            read2: Reverse Compliment read.

        read1 and read2 are dnaio Sequence objects.
        They are passed into this method as references.
        The modifications on read1 and read2 will be preserved after return.

        The score_matrix will be generated if not specified.
        However, if specified, it must be generated by:
            score_matrix = parasail.matrix_create("ACGTN", self.score, -1 * self.penalty)
        Pass a score_matrix avoid this method to generate it every time, which may speed up the overall processing.

        Returns: A 2-tuple indicating whether any adapters are matching the read pair.
            If a read matched an adapter, the matching adapter will be returned in the tuple.
            Otherwise, the corresponding element in the tuple will be None.
            For example, ("ACTGACT", None) indicates barcode "ACTGACT" is matching read1 (forward read).

        See Also:
             https://github.com/marcelm/dnaio/blob/master/src/dnaio/_core.pyx
             https://github.com/jeffdaily/parasail-python#substitution-matrices

        """
        # Trim both read1 and read2 with all adapters before return
        reads = [read1, read2]
        # Indicates whether R1 or R2 matches the adapter.
        matched = [""] * len(reads)
        for i in range(len(reads)):
            read = reads[i]
            for adapter in self.adapters:
                result = parasail.sg_de_stats(
                    adapter, read.sequence[:20], self.penalty, self.penalty, self.score_matrix
                )
                if result.matches <= self.min_match_length:
                    continue
                distance = (self.score * result.matches - result.score) / self.penalty
                max_distance = math.floor(len(adapter) * self.error_rate)
                if distance <= max_distance:
                    matched[i] = adapter
                    read.sequence = read.sequence[result.end_ref + 1:]
                    read.qualities = read.qualities[result.end_ref + 1:]
                    break
        # read1 and read2 are preserved implicitly
        return matched[0], matched[1]

    def process_read_pair(self, read_pair):
        read1, read2 = read_pair
        # Initialize ReadPair to check if read1 and read2 are valid
        read1, read2 = ReadPair(read1, read2).reads
        # read1 and read2 are references
        # The modifications on read1 and read2 will be returned implicitly
        adapter1, adapter2 = self.trim_adapters(read1, read2)

        if adapter1:
            self.add_count("%s_1" % adapter1)
        if adapter2:
            self.add_count("%s_2" % adapter2)

        # The longer adapter has higher priority
        adapter = adapter1 if len(adapter1) > len(adapter2) else adapter2
        if adapter:
            # Count the number of reads matching the longer adapter
            self.add_count(adapter)
            # Sequence matched a barcode
            self.add_count('matched')

        else:
            # Sequence does not match a barcode
            self.add_count('unmatched')
            adapter = DemultiplexWriter.BARCODE_NOT_MATCHED
        return adapter, read1, read2


class DemultiplexDualIndexWorker(DemultiplexWorker):
    def __init__(self, barcode_dict, error_rate=None, score=1, penalty=10):
        super().__init__(barcode_dict, error_rate, score, penalty)
        self.max_error = {adapter: math.floor(len(adapter) * self.error_rate) for adapter in self.adapters}

    def match_adapters(self, barcode):
        # barcode_i7, barcode_i5 = barcode.split("+", 1)

        for adapter in self.adapters:
            mismatch = editdistance.eval(barcode, adapter)
            # adapter_i7, adapter_i5 = adapter.split("+", 1)

            # mismatch = self.semi_global_distance(barcode_i7, adapter_i7) + \
            #     self.semi_global_distance(barcode_i5, adapter_i5)

            # mismatch = editdistance.eval(barcode_i7, adapter_i7) + editdistance.eval(barcode_i5, adapter_i5)
            if mismatch < self.max_error.get(adapter, 0):
                return adapter
        return None

    def process_read_pair(self, read_pair):
        read1, read2 = read_pair
        barcode = ReadPair(read1, read2).barcode
        if re.match(IlluminaFASTQ.dual_index_pattern, barcode):
            barcode = IlluminaFASTQ.convert_barcode(barcode)
        barcode = self.match_adapters(barcode)
        if not barcode:
            # TODO: Write unmatched reads.
            self.add_count("unmatched")
            return DemultiplexWriter.BARCODE_NOT_MATCHED, read1, read2
        self.add_count('matched')
        self.add_count(barcode)
        return barcode, read1, read2


class DemultiplexProcess(FASTQProcessor):
    """Represents a demultiplex process managing multiple readers and workers.
    """
    def __init__(self, worker_class, barcode_dict, error_rate=None, score=1, penalty=10):
        super().__init__(worker_class)
        self.barcode_dict = barcode_dict
        self.error_rate = error_rate
        self.score = score
        self.penalty = penalty

        self.adapters = list(self.barcode_dict.keys())
        self.output_list = []

    def get_worker_args(self, i):
        ident = "Process_%s" % i
        output_dict = {k: os.path.join(self.workspace, "%s_%s" % (k, ident)) for k, v in self.barcode_dict.items()}
        self.output_list.append(output_dict)
        return [output_dict, self.error_rate, self.score, self.penalty]

    def collect_results(self, jobs):
        super().collect_results(jobs)
        prefix_dict = self.prepare_concatenation(self.barcode_dict, self.output_list)
        self.concatenate_fastq(prefix_dict)

    @staticmethod
    def prepare_concatenation(barcode_dict, output_list):
        # prefix_dict is a dict storing the final output prefix as keys, and
        # each value is a list of file prefixes (paths) to be concatenated.
        prefix_dict = {}
        for barcode, file_prefix in barcode_dict.items():
            # The following code will merge the barcodes pointing to the same file path
            path_list = prefix_dict.get(file_prefix, [])
            path_list.extend([
                output_dict.get(barcode)
                for output_dict in output_list
                if output_dict.get(barcode)
            ])
            prefix_dict[file_prefix] = path_list
        prefix_dict = {k: list(set(v)) for k, v in prefix_dict.items()}
        return prefix_dict

    @staticmethod
    def concatenate_fastq(prefix_dict):
        """Concatenates the FASTQ files in a list of directories.
        """
        for prefix, prefix_list in prefix_dict.items():
            logger.debug("Concatenating %s pairs of files." % len(prefix_list))
            r1, r2 = DemultiplexWriter.paired_end_filenames(prefix)
            pair_list = [DemultiplexWriter.paired_end_filenames(p) for p in prefix_list]

            cmd = "cat %s > %s" % (" ".join(p[0] for p in pair_list), r1)
            os.system(cmd)
            logger.debug(r1)
            cmd = "cat %s > %s" % (" ".join(p[1] for p in pair_list), r2)
            os.system(cmd)
            logger.debug(r2)

    @staticmethod
    def parse_barcode_outputs(barcode_outputs):
        """Parses the barcode and output file prefix pairs specified as a list of strings like:
            "BARCODE=PREFIX", or "BARCODE_1 BARCODE_2=PREFIX"

        Args:
            barcode_outputs (list): A list of strings in the format of BARCODE=PREFIX

        Returns: A dictionary, where each key is a barcode, each value is the file_prefix.

        """
        barcode_dict = {}
        for output in barcode_outputs:
            arr = str(output).split("=", 1)
            barcode_list = arr[0].strip().split()
            file_prefix = arr[1] if len(arr) > 1 else None
            for barcode in barcode_list:
                barcode_dict[barcode] = file_prefix
        return barcode_dict


class DemultiplexInline(DemultiplexProcess):
    def __init__(self, barcode_dict, error_rate=None, score=1, penalty=10):
        super().__init__(DemultiplexInlineWorker, barcode_dict, error_rate, score, penalty)

    def save_statistics(self, csv_file_path, sample_name=None, header=None):
        """Saves the demultiplex statistics into a CSV file

        """

        if not header:
            header = ""

        if not sample_name:
            sample_name = ""

        with open(csv_file_path, "w") as f:
            writer = csv.writer(f)
            writer.writerow([
                "sample", "barcode",
                "read1_percent", "read2_percent", "total_percent_%s" % header,
                "total_reads", "%s_reads" % header, "non%s_reads" % header
            ])
            if "total" not in self.counts:
                raise KeyError("Total read count is missing.")
            total = self.counts.get("total")
            if not total:
                return None
            if "unmatched" not in self.counts:
                raise KeyError("Unmatched read count is missing.")
            unmatched = self.counts.get("unmatched")
            for adapter in self.adapters:
                r1 = self.counts.get("%s_1" % adapter, 0)
                r2 = self.counts.get("%s_2" % adapter, 0)
                matched = self.counts.get(adapter, 0)
                writer.writerow([
                    sample_name, adapter, r1 / total, r2 / total, matched / total, total, matched, unmatched
                ])
        print("Statistics saved to %s" % csv_file_path)
        return csv_file_path


class DemultiplexDualIndex(DemultiplexProcess):
    def __init__(self, barcode_dict, error_rate=None, score=1, penalty=10):
        super().__init__(DemultiplexDualIndexWorker, barcode_dict, error_rate, score, penalty)

    @staticmethod
    def determine_adapters(r1):
        barcode_counts = dict()
        counter = 0
        with dnaio.open(r1) as fastq_in:
            for read1 in fastq_in:
                barcode = read1.name.strip().split(":")[-1]
                if re.match(IlluminaFASTQ.dual_index_pattern, barcode):
                    barcode = IlluminaFASTQ.convert_barcode(barcode)
                c = barcode_counts.get(barcode, 0)
                c += 1
                barcode_counts[barcode] = c

                counter += 1
                if counter > 3000:
                    break
        barcode_list = BarcodeStatistics(barcode_counts).major_barcodes()
        return barcode_list
