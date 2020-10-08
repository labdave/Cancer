import multiprocessing
import os
import dnaio
import logging
import datetime
import time
import tempfile
import traceback
from multiprocessing import Process, Manager
logger = logging.getLogger(__name__)


class FASTQWorker:
    """Represents a worker process for processing FASTQ reads
    """

    def __init__(self):
        """Initialize a worker process.

        """
        # Stores the statistics
        # Each sub-class may have different keys for the counts dictionary.
        # The values should be integers.
        # Whether to use this in the sub-class is optional.
        self.counts = dict(matched=0, unmatched=0, total=0)

    def add_count(self, key, val=1):
        """Increments the value of a particular key in self.counts (dictionary)
        """
        c = self.counts.get(key, 0)
        c += val
        self.counts[key] = c
        return self.counts

    def start(self, in_queue, out_queue):
        """Starts processing reads from in_queue.
        The number of reads processed are put into the out_queue for counting purpose.

        The worker will stop when the it gets a None from the in_queue.

        Args:
            in_queue: A queue holding list of reads to be processed.
                Each item in the in_queue is a list reads so that the frequency of accessing the queue are reduced.
            out_queue: A queue holding integers for counting purpose.
                a negative number in out_queue indicates an error/exception

        Returns: self.counts, in which the keys depends on the process_read_pair() method.

        """
        # Keep track of the active processing time.
        active_time = datetime.timedelta()
        batch_count = 0

        # TODO: Pre-start setup and Post-stop clean up
        try:
            while True:
                reads = in_queue.get()
                # Keep the starting time for each batch processing
                timer_started = datetime.datetime.now()
                # None is used to tell the worker to stop processing
                if reads is None:
                    batch_time = (active_time / batch_count) if batch_count else 0
                    logger.debug("Process %s, Active time: %s (%s batches, %s/batch)." % (
                        os.getpid(), active_time, batch_count, batch_time
                    ))
                    return self.counts

                for read_pair in reads:
                    self.process_read_pair(read_pair)

                # Add total processed count
                self.add_count('total', len(reads))
                batch_count += 1

                # Add processing time for this batch
                active_time += (datetime.datetime.now() - timer_started)
                
                # Put the total number of reads processed into the queue.
                out_queue.put(len(reads))
        except Exception as ex:
            logger.error(str(ex) + "\n" + traceback.format_exc())
            # Put a negative number into the queue if there is an error.
            out_queue.put(-1)

    def process_read_pair(self, read_pair):
        """Process the read pair
        """
        raise NotImplementedError


class FASTQProcessor:
    """Processes FASTQ files with multiple CPUs

    Attributes:
        BATCH_SIZE: The number of read pairs to be packed into each item in the processing queue.
            A batch is a basic unit of data for processing
            Each worker will process one batch of data at a time
            Increasing the batch size can reduces the overhead of accessing the reader queue.
            However, large batch size may increase the memory usage and decrease the efficiency of each worker.
    """

    BATCH_SIZE = 5000

    @staticmethod
    def get_identifier(fastq_file):
        """Gets the identifier of the first read in a FASTQ file.

        Args:
            fastq_file: The full path of a FASTQ file.

        """
        with dnaio.open(fastq_file) as f:
            for read in f:
                return read.name.split(" ", 1)[0].split("/")[0]

    @classmethod
    def pair_fastq_files(cls, r1_list, r2_list):
        """Pairs the FASTQ files from two lists by the identifier of the first read

        Args:
            r1_list: A list of FASTQ R1 file paths.
            r2_list: A list of FASTQ R2 file paths.

        Returns: A list of 2-tuples, each is a pair of FASTQ file paths.

        """
        if not r1_list or not r2_list:
            raise ValueError("Invalid R1 or R2.\nR1: %s\nR2: %s" % (r1_list, r2_list))
        # Convert r1_list and r2_list to lists if needed.
        if not isinstance(r1_list, list):
            r1_list = [r1_list]
        if not isinstance(r2_list, list):
            r2_list = [r2_list]

        r1_dict = {cls.get_identifier(f): f for f in r1_list}
        r2_dict = {cls.get_identifier(f): f for f in r2_list}

        if r1_dict.keys() != r2_dict.keys():
            raise ValueError("Unable to pair R1 and R2.\nR1: %s\nR2: %s" % (r1_dict.keys(), r2_dict.keys()))

        # Sort the keys, so that the results are deterministic
        keys = list(r1_dict.keys())
        keys.sort()
        fastq_files = []
        for key in keys:
            fastq_files.append((r1_dict[key], r2_dict[key]))
        return fastq_files

    @staticmethod
    def print_queue_size(q_size_array):
        for s in q_size_array:
            print("*" * s + str(s))

    @staticmethod
    def read_data(fastq_files, queue):
        """Reads read pairs from FASTQ files in to a queue.

        Args:
            fastq_files: A list of 2-tuples containing paired-end FASTQ filenames.
                The reads from the list of files will be read into the queue sequentially.
                To read files in parallel, start a read_data process for each pair of files.
            queue: A queue for holding reads to be processed.
                Each item in the queue will be a list of read pairs (2-tuples).

        """
        enqueue_time = datetime.timedelta()
        batch_count = 0

        process_started = datetime.datetime.now()
        for fastq in fastq_files:
            if isinstance(fastq, str):
                r1 = fastq
                r2 = None
            else:
                r1 = fastq[0]
                r2 = fastq[1]
            with dnaio.open(r1, file2=r2) as fastq_in:
                size = 0
                reads = []
                for read in fastq_in:
                    reads.append(read)
                    size += 1
                    if size > FASTQProcessor.BATCH_SIZE:
                        batch_count += 1

                        timer_started = datetime.datetime.now()
                        queue.put(reads)
                        enqueue_time += (datetime.datetime.now() - timer_started)

                        size = 0
                        reads = []

                timer_started = datetime.datetime.now()
                queue.put(reads)
                enqueue_time += (datetime.datetime.now() - timer_started)

        print('Finished reading files. Total time: %s, Enqueue time: %s, Batch size: %s, Batch count: %s' % (
            datetime.datetime.now() - process_started, enqueue_time, FASTQProcessor.BATCH_SIZE, batch_count
        ))

    @staticmethod
    def start_worker(worker_class, in_queue, out_queue, *args, **kwargs):
        """Starts a new worker
        """
        try:
            return worker_class(*args, **kwargs).start(in_queue, out_queue)
        except:
            # Put a negative number into output queue so that the main process knows there is an error.
            traceback.print_exc()
            out_queue.put(-1)
            return 0

    def __init__(self, worker_class, *args, **kwargs):
        """Initializes a FASTQ processing with a FASTQWorker class or its subclass
        """
        self.pool_size = max(os.cpu_count(), 1)
        logger.debug("Pool Size: %s" % self.pool_size)
        # The parameters for initializing the workers
        # Override the get_worker_args() and get_worker_kwargs() to use different parameters for different workers.
        self.worker_class = worker_class
        self.worker_args = args
        self.worker_kwargs = kwargs
        # Workspace is the path of a temporary directory
        # This directory is only available during the processing
        self.workspace = None

        self.manager = Manager()
        self.pool = []
        self.counts = dict()

        self.readers = []
        # Indicate if any reader is alive
        self.reading = False

        self.reader_queue = self.manager.Queue(max(self.pool_size * 100, 100))
        self.worker_queue = self.manager.Queue()

    def get_worker_args(self, i):
        return self.worker_args

    def get_worker_kwargs(self, i):
        return self.worker_kwargs

    def update_counts(self, counts):
        """Updates the demultiplex statistics.

        Args:
            counts: A dictionary storing the statistics. All values should be numeric.

        This method merges counts from the input argument with self.counts
        Values of the existing keys are added together.
        Keys from counts will be created if they are not already in self.counts

        """
        for k, v in counts.items():
            self.counts[k] = self.counts.get(k, 0) + v
        return self.counts

    def start_readers(self, fastq_files):
        """Starts a reader for each pair of FASTQ file
        """
        reader_pool = multiprocessing.Pool(len(fastq_files))
        self.reading = True
        for fastq_pair in fastq_files:
            reader = reader_pool.apply_async(
                FASTQProcessor.read_data,
                ([fastq_pair], self.reader_queue),
            )
            self.readers.append(reader)
        return self.readers

    def finish_reading(self):
        """Finishes the file reading by putting None into the reader_queue
        """
        for i in range(self.pool_size):
            self.reader_queue.put(None)
        self.reading = False

    def readers_alive(self):
        """Checks if the readers are still reading the files

        Returns: True if a least one reader is still reading the file. Otherwise False
        """
        for reader in self.readers:
            if not reader.ready():
                return True
            if not reader.successful():
                raise IOError("An error occurred when reading the FASTQ files. See logs for more details.")
        return False

    def wait_for_jobs(self, jobs):
        """Waits for the jobs to finish and keep track of the queue size
        """
        reader_q_size = []
        # writer_q_size = []
        counter = 0
        while True:
            reader_q_size.append(self.reader_queue.qsize())
            if self.reading:
                if not self.readers_alive():
                    self.finish_reading()
            time.sleep(2)
            # Jobs won't be ready unless finish reading
            ready = True
            for job in jobs:
                if not job.ready():
                    ready = False
            while not self.worker_queue.empty():
                processed_count = self.worker_queue.get()
                if processed_count < 0:
                    raise ValueError("An error occurred in one of the worker process. See logs for more details.")
                counter += processed_count
            print("{:,} reads/pairs processed.".format(counter))
            if ready:
                break
            time.sleep(3)
        self.print_queue_size(reader_q_size)
        logger.debug("Reader's queue max size: %s" % max(reader_q_size))
        return reader_q_size

    def collect_results(self, jobs):
        """Collects the statistics by merging the dictionary returned by each worker.
        """
        results = [job.get() for job in jobs]
        for job in jobs:
            if not job.successful():
                raise ValueError("An error occurred in one of the worker process. See logs for more details.")
        for r in results:
            self.update_counts(r)
        if len(self.counts.keys()) < 20:
            logger.debug(self.counts)
        return self.counts

    def start(self, fastq_files):
        """Starts processing
        """
        worker_pool = multiprocessing.Pool(self.pool_size)
        worker_jobs = []
        # Create a temporary directory to be used as workspace.
        # Sub-class may use this workspace to write temporary files.
        with tempfile.TemporaryDirectory() as temp_dir:
            self.workspace = temp_dir
            # Start the workers first
            for i in range(self.pool_size):
                args = self.get_worker_args(i)
                kwargs = self.get_worker_kwargs(i)

                job = worker_pool.apply_async(
                    FASTQProcessor.start_worker,
                    (self.worker_class, self.reader_queue, self.worker_queue, *args),
                    kwargs
                )
                worker_jobs.append(job)

            # Start reading the files
            self.start_readers(fastq_files)
            # Wait for the worker_jobs to finish and keep track of the queue size
            self.wait_for_jobs(worker_jobs)
            # Collect the statistics.
            self.collect_results(worker_jobs)

        worker_pool.terminate()
        worker_pool.join()
        logger.debug("Finished Processing FASTQ.")
        return self
