import sys
import logging
import json
import gzip
import argparse
from datetime import datetime

from .fastq_file import IlluminaFASTQ
from Aries.storage import StorageFile


def configure_argparser(argparser_obj):

    # Pipeline name
    argparser_obj.add_argument("-i", "--input-file",
                               action="store",
                               dest="input_file",
                               required=True,
                               help="Path to input GZipped FASTQ file")

    # Deprecated pipeline flag
    argparser_obj.add_argument("-o", "--output-file",
                               action="store",
                               dest="output_file",
                               required=True,
                               help="Path to JSON file to store the generated barcode stats")

    # Verbosity
    argparser_obj.add_argument("-v",
                               action='count',
                               dest='verbosity',
                               required=False,
                               default=0,
                               help="Increase verbosity of the program."
                                    "Multiple -v's increase the verbosity level:\n"
                                    "   0 = Errors\n"
                                    "   1 = Errors + Warnings\n"
                                    "   2 = Errors + Warnings + Info\n"
                                    "   3 = Errors + Warnings + Info + Debug")


def configure_logging(verbosity):
    # Setting the format of the logs
    FORMAT = '[%(asctime)s]-[%(process)d] %(name)s -- %(levelname)s:%(message)s'

    # Configuring the logging system to the lowest level
    logging.basicConfig(level=logging.DEBUG, format=FORMAT, stream=sys.stderr)

    # Defining the ANSI Escape characters
    BOLD = '\033[1m'
    DEBUG = '\033[92m'
    INFO = '\033[94m'
    WARNING = '\033[93m'
    ERROR = '\033[91m'
    END = '\033[0m'

    # Coloring the log levels
    if sys.stderr.isatty():
        logging.addLevelName(logging.ERROR, "%s%s%s%s%s" % (BOLD, ERROR, "ERROR", END, END))
        logging.addLevelName(logging.WARNING, "%s%s%s%s%s" % (BOLD, WARNING, "WARNING", END, END))
        logging.addLevelName(logging.INFO, "%s%s%s%s%s" % (BOLD, INFO, "INFO", END, END))
        logging.addLevelName(logging.DEBUG, "%s%s%s%s%s" % (BOLD, DEBUG, "DEBUG", END, END))
    else:
        logging.addLevelName(logging.ERROR, "GENERATE_BARCODE_STATS_ERROR")
        logging.addLevelName(logging.WARNING, "GENERATE_BARCODE_STATS_WARNING")
        logging.addLevelName(logging.INFO, "GENERATE_BARCODE_STATS_INFO")
        logging.addLevelName(logging.DEBUG, "GENERATE_BARCODE_STATS_DEBUG")

    # Setting the level of the logs
    level = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG][verbosity]

    logging.getLogger().setLevel(level)

def main():

    # Authenticate with the cluster
    argparser = argparse.ArgumentParser(prog="GenerateBarcodeStats")
    configure_argparser(argparser)

    # Parse the arguments
    args = argparser.parse_args()

    configure_logging(verbosity=args.verbosity)

    logger = logging.getLogger("GENERATE_BARCODE_STATS")

    # start time of parsing
    start = datetime.now()

    logger.info(f'Generating stats started on {start}')

    _analyze_barcode(args.input_file, args.output_file, logger)

    # end time of parsing
    end = datetime.now()

    # total execution time
    total_time = end - start

    logger.info(f'Parsing took total time of {total_time}')
    logger.info(f'Parsing completed at {end}')


def _analyze_barcode(gzip_fastq, json_stats, logger):
    logger.debug(f"Analyzing barcode in {gzip_fastq}")

    logger.debug("Counting reads by barcode...")

    # fastq = File.unzip(gzip_fastq)
    fastq = gzip_fastq.replace(".gz", "")

    with gzip.open(gzip_fastq, 'rb') as gzip_file:
        with open(fastq, "wb") as unzipped_file:
            logger.debug("Unzipping %s to %s ..." % (gzip_fastq, fastq))
            block_size = 1 << 20
            while True:
                block = gzip_file.read(block_size)
                if not block:
                    break
                unzipped_file.write(block)

    barcode_stats = IlluminaFASTQ(fastq).count_by_barcode()

    logger.debug(f"Barcode count: {len(barcode_stats.keys())}")

    with StorageFile.init(json_stats, 'w') as fp:
        json.dump(barcode_stats, fp)

    return json_stats


if __name__ == "__main__":
    main()
