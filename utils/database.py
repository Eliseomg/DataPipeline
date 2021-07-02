#import mysql.connector
#from mysql.connector import errorcode
import pymysql.cursors
import pymysql

class Database:

	def __init__(self):



		try:
			# Connect to the database
			self.connection = pymysql.connect(host='localhost',
									 user='root',
									 password='rootroot',
									 db='pruebas',
									 charset='utf8mb4',
									 cursorclass=pymysql.cursors.DictCursor)

		except mysql.connector.Error as err:
			print("Something was wrong... {}".format(err))


	def get_conn(self):

		return self.connection



