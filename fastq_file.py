import os
import re
import gzip
import json
import logging
from Aries.storage import StorageFile
from Aries.visual.plotly import PlotlyFigure
from Aries.collections import sort_lists
from .genomics.sequence import Sequence
logger = logging.getLogger(__name__)


class ReadIdentifier:
    """Parses the identifier line of a read sequence from a FASTQ file.

    See Also:
        https://support.illumina.com/bulletins/2016/04/fastq-files-explained.html
        https://en.wikipedia.org/wiki/FASTQ_format

    Tested on the following cases:
    @HWUSI-EAS100R:6:73:941:1973#0/1
    @EAS139:136:FC706VJ:2:2104:15343:197393 1:Y:18:ATCACG
    @EAS139:136:FC706VJ:2:2104:15343:197393 1:N:18:1
    @SRR001666.1 071112_SLXA-EAS1_s_7:5:1:817:345 length=36
    @ERR194147.3 HSQ1004:134:C0D8DACXX:3:1101:1318:114841/2
    @NB552316:26:HWFLNBGXF:1:11101:26601:1229 1:N:0:GCACAACT+CAAGTCGT
    @NB552316:79:HNJJ3BGXG:1:11101:10002:13076/1
    """

    INSTRUMENT_PATTERN = r"^(?P<instrument>[\w-]+):(?:(?P<run_id>[0-9]+):)?(?:(?P<flowcell>\w*[a-zA-Z]\w*):)?"
    LOCATION_PATTERN = r"(?P<lane>[0-9]+):(?P<tile>[0-9]+):(?P<x>[0-9]+):(?P<y>[0-9]+)"
    INDEX_PATTERN = r"(?:#(?P<index>[^\/]+))?\/(?P<pair_member>[0-9]+)"

    CONTROL_PATTERN = r"(?P<pair_member>[0-9]+):(?P<is_filtered>[YN]):(?P<control_number>[0-9]+):(?P<index>\S+)"

    SEQ_ID_PATTERN = r"%s%s(?:%s)?" % (INSTRUMENT_PATTERN, LOCATION_PATTERN, INDEX_PATTERN)

    def __init__(self, line):
        if line.startswith("@"):
            line = line[1:]
        self.array = line.split(" ")
        self.identifier = self.array[0].split("/")[0]
        self.__info = None

    @property
    def info(self):
        """A dictionary storing all the information parsed from the identifier line
        """
        if self.__info is None:
            self.__info = dict()
            # description = []
            for s in reversed(self.array):
                # matched = False
                for pattern in [self.CONTROL_PATTERN, self.SEQ_ID_PATTERN]:
                    m = re.match(pattern, s)
                    if m:
                        # matched = True
                        self.__info.update({k: v for k, v in m.groupdict().items() if v})
                        continue
            #     if not matched and s != self.identifier:
            #         description.append(s)
            # self.__info["description"] = " ".join(description)
        return self.__info

    @property
    def description(self):
        """Description includes all characters after the first space.
        """
        # return self.info.get("description")
        return " ".join(self.array[1:]) if len(self.array) > 1 else ""

    @property
    def pair_member(self):
        """The member of a pair, 1 or 2 (paired-end or mate-pair reads only)
        """
        return self.info.get("pair_member")


class FASTQRead:
    """Represents a read in FASTQ file."""
    def __init__(self, lines):
        if len(lines) != 4:
            raise ValueError("lines must be a list of 4 strings.")
        s = [line.decode() if isinstance(line, bytes) else line for line in lines]
        self.identifier = s[0]
        self.sequence = s[1]
        self.description = s[2]
        self.quality = s[3]

    def write_to(self, f):
        f.write(self.identifier.encode())
        f.write(self.sequence.encode())
        f.write(self.description.encode())
        f.write(self.quality.encode())


class FASTQGzip:
    def __init__(self, uri):
        self.uri = uri
        self.gzip = gzip.GzipFile(fileobj=StorageFile.init(uri, "rb"))
        self.current = 0

    def __iter__(self):
        return self

    def __next__(self):
        read = FASTQRead(
            [self.gzip.readline(),
             self.gzip.readline(),
             self.gzip.readline(),
             self.gzip.readline()]
        )
        if read.identifier:
            self.current += 1
            return read
        else:
            raise StopIteration

    @property
    def read_count(self):
        logger.debug("Counting reads in file %s..." % self.uri)
        self.gzip = gzip.GzipFile(fileobj=StorageFile.init(self.uri, "rb").local())
        return len(list(self))


class IlluminaFASTQ:
    processing_progress = {}

    dual_index_pattern = r"[ACGTN]{8}\+[ACGTN]{8}"

    def __init__(self, file_path):
        file_path = str(file_path)
        if not StorageFile(file_path).exists():
            raise FileNotFoundError("File not found at %s." % file_path)

        self.file_path = file_path
        logger.debug("Initialized Illumina FASTQ object.")

    def peek_barcode(self):
        barcode_dict = {}
        with StorageFile.init(self.file_path, 'rb') as f:
            with gzip.GzipFile(fileobj=f) as gz:
                for i, line in enumerate(gz, start=1):
                    if i > 4000:
                        break
                    # The line containing barcode starts with @
                    if not line.startswith(b"@"):
                        continue
                    if isinstance(line, bytes):
                        line = line.decode()
                    # Raw barcode
                    barcode = line.strip().split(":")[-1]

                    if re.match(self.dual_index_pattern, barcode):
                        barcode = self.convert_barcode(barcode)
                        barcode_dict[barcode] = self.__count_barcode(barcode_dict, barcode, i)
        return barcode_dict

    @staticmethod
    def convert_barcode(barcode):
        idx = barcode.split("+")
        i7 = idx[0]
        i5 = Sequence(idx[1]).reverse_complements
        barcode = "%s+%s" % (i7, i5)
        return barcode

    def __process_barcode(self, lines, method):
        """

        Args:
            lines: Iterable lines from FASTQ file.
            method: The method for processing the line containing the barcode.

        Returns:

        """
        barcode_dict = {}
        for i, line in enumerate(lines, start=1):
            if i > 0 and i % 1000000 == 0:
                logger.debug("%s reads processed." % round(i / 4))
            # The line containing barcode starts with @
            if not line.startswith("@"):
                continue
            # Raw barcode
            barcode = line.strip().split(":")[-1]

            if re.match(self.dual_index_pattern, barcode):
                barcode = self.convert_barcode(barcode)
                barcode_dict[barcode] = method(barcode_dict, barcode, i)
        return barcode_dict

    @staticmethod
    def __count_barcode(barcode_dict, barcode, row_number):
        """Increments the number of reads for a particular barcode
        """
        return barcode_dict.get(barcode, 0) + 1

    @staticmethod
    def __group_barcode(barcode_dict, barcode, row_number):
        line_list = barcode_dict.get(barcode, [])
        line_list.append(row_number)
        return line_list

    def group_by_barcode(self, threshold=0):
        with open(self.file_path, 'r') as f:
            barcode_dict = self.__process_barcode(f, self.__group_barcode)
        if threshold > 0:
            barcode_dict = {k: v for k, v in barcode_dict.items() if len(v) > threshold}
        return barcode_dict

    def count_by_barcode(self, threshold=0):
        """Counts the number of reads for each barcode in the FASTQ file.

        Args:
            threshold: Includes only barcodes with number of reads more than threshold.

        Returns:

        """
        with open(self.file_path, 'r') as f:
            logger.debug("Counting number of reads per barcode...")
            barcode_dict = self.__process_barcode(f, self.__count_barcode)
        logger.debug("%s barcodes in the file" % len(barcode_dict.keys()))
        return {k: v for k, v in barcode_dict.items() if v > threshold}


class BarcodeStatistics:
    def __init__(self, barcode_dict):
        """

        Args:
            barcode_dict (dict):  A dictionary where barcodes and keys and the numbers of reads as value
        """
        self.barcode_dict = barcode_dict

    @staticmethod
    def from_json(json_string):
        return BarcodeStatistics(json.loads(json_string))

    def total_reads(self):
        return sum(self.barcode_dict.values())

    def filter_by_reads(self, threshold=0):
        self.barcode_dict = {k: v for k, v in self.barcode_dict.items() if v > threshold}
        return self

    def sort_data(self, max_size=0, reverse=True):
        """

        Args:
            max_size: The maximum number of barcodes and count to be returned.
            reverse: Indicates whether to sort the data in reverse order (from large to small).

        Returns: A 2-tuple: (Number of Reads, Barcode)

        """
        barcodes = []
        counts = []
        for k, v in self.barcode_dict.items():
            barcodes.append(k)
            counts.append(v)
        counts, barcodes = sort_lists(counts, barcodes, reverse=reverse)
        if max_size and len(counts) > max_size:
            counts = counts[:max_size]
            barcodes = barcodes[:max_size]
        return counts, barcodes

    def as_sorted_list(self, max_size=0, reverse=True):
        counts, barcodes = self.sort_data(max_size=max_size, reverse=reverse)
        barcode_list = []
        if not barcodes:
            return barcode_list
        for i in range(len(counts)):
            barcode_list.append({
                "barcode": barcodes[i],
                "count": counts[i]
            })
        # Calculate the mismatches between every barcode in the list and the first barcode in the list
        dominant_barcode = Sequence(barcode_list[0]["barcode"])
        for barcode in barcode_list:
            mismatch = dominant_barcode.match(barcode["barcode"])
            barcode["mismatch"] = mismatch
        return barcode_list

    def major_barcodes(self):
        """Returns a list of major barcodes from the statistics.

        The barcodes will be divided into two groups (clusters) if possible, i.e. major and minor.
        The gap between min. reads major and max. reads minor barcodes must be greater than
            twice the gap between any barcodes within the same group.
        A gap between counter1 and counter2 is defined as counter1 / counter2.
        If such groups does not exist, an empty list will be returned.

        Returns: A list of major barcodes. An empty list will be returned if there are less than 3 barcodes.

        """
        counts, barcodes = self.sort_data()
        if len(counts) < 3:
            return []
        gaps = [counts[i] / counts[i + 1] for i in range(len(counts) - 1)]
        indices = list(range(len(counts) - 1))
        gaps, indices = sort_lists(gaps, indices, reverse=True)
        if gaps[0] > 2 * gaps[1]:
            return barcodes[:indices[0] + 1]
        return []

    def histogram(self, max_bins=20):
        counts, labels = self.sort_data(max_bins)
        return PlotlyFigure().add_trace("Histogram", x=labels, y=counts, histfunc='sum')

    def bar_chart(self, max_size=20):
        counts, labels = self.sort_data(max_size)
        counts.reverse()
        labels.reverse()
        return PlotlyFigure(height=1000, font=dict(
            family='Courier New, monospace'
        )).bar(x=counts, y=labels, orientation='h')
