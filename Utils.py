import logging
import os

LOG_DIR = "logs"

def get_logger(name):

	logger = logging.getLogger(name)
	logger.setLevel(logging.DEBUG)
	log_file = "corporate_processing_logs_new.txt"
	ch = logging.FileHandler(os.path.join(LOG_DIR, log_file))
	ch.setLevel(logging.DEBUG)
	formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
	ch.setFormatter(formatter)
	logger.addHandler(ch)
	return logger