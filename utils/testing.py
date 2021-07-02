
import unittest
from ETL import ETLPipeLine
from datetime import datetime


class Test_all(unittest.TestCase):


	def test_time_stamp(self):

		timestamp = ETLPipeLine(filename="")

		self.assertEqual(timestamp.time_stamp("2019-02-13"), "2019-02-13 00:00:00")

		self.assertEqual(timestamp.time_stamp("2019-02"), str(datetime.fromtimestamp(datetime.timestamp(datetime.now()))))

		self.assertEqual(timestamp.time_stamp("12-05-2019"), str(datetime.fromtimestamp(datetime.timestamp(datetime.now()))))

		self.assertEqual(timestamp.time_stamp("12-05-19"), str(datetime.fromtimestamp(datetime.timestamp(datetime.now()))))

		self.assertEqual(timestamp.time_stamp("1a-05-19"), str(datetime.fromtimestamp(datetime.timestamp(datetime.now()))))
		

		print("test_time_stamp() -> passed")





if __name__ == "__main__":

	unittest.main()