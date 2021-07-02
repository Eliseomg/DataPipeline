
"""
For this part you need only charge this script into DataPipeline,
this scrip follow up the process for extract, transform and load data.
"""

# standard imports
import pandas as pd
from datetime import datetime


# import package
from utils.database import Database




class ETLPipeLine:
	"""

	"""

	def __init__(self, filename=""):
		self.filename = filename


	def get_file(self, filename_sql):

		"""
		:param filename_sql: A file name SQL to open
		:type filename_sql: string

		:return: A list with sql commands.
		:rtype: list
		"""

		fd = open("./utils/"+filename_sql, 'r')
		sqlfile = fd.read()
		fd.close()
		commands = sqlfile.split(';')

		return commands


	def time_stamp(self, dates):
		"""
		:param dates:
		:type dates:

		:return:
		:rtype:
		"""


		item = dates.split('-')

		if len(item) < 3:
			# item is a list with year, month and day, if there no one of these, then timestamp = current day
			timestamp = datetime.timestamp(datetime.now())

		else:
			# year has four digits, otherwise timestamp := current day
			if len(item[0]) != 4:
				timestamp = datetime.timestamp(datetime.now())

			# month has two digits, otherwise timestamp := current day
			elif len(item[1]) != 2:
				timestamp = datetime.timestamp(datetime.now())
			
			# day has two digits, otherwise timestamp := current day
			elif len(item[2]) != 2:
				timestamp = datetime.timestamp(datetime.now())

			else:

				timestamp = datetime.timestamp(datetime(int(item[0]),
											int(item[1]),
											int(item[2])))
		
		dt_object = str(datetime.fromtimestamp(timestamp))

		return dt_object


	# extract data
	def extract(self):
		"""
		:param: None
		:type: None 

		:return: A file 
		:rtype: Object Pandas
		"""

		try:
			file = "./utils/" + self.filename
			
			data = pd.read_csv(file,
						 header = None,
						 engine='python')
		
			return data
		except Exception as err:
			print("Error, file '{}' does not exist!!!: {}".format(self.filename, err))


	def get_view(self):

		"""
		Get data from a view loaded in the DB

		:param: None
		:type: None

		:return: None
		:rtype: None
		"""

		query_view = "SELECT * FROM money_paid_by_company"

		try:
			connection = Database().get_conn()
			with connection.cursor() as cursor:
				cursor.execute(query_view)

			values = cursor.fetchall()

			return values

		except Exception as err:
			print("An error has ocurred...{}".format(err))

		finally:
			connection.close()


	# transform data
	def transform(self, content):

		"""
		:param content: Data to transform
		:type content: DataFrame

		:return: 
		:rtype: DataFrame
		"""

		# ignore the first row
		content = content.iloc[1:]
		content = content.rename(columns={
			0: "id",
			1: "company_name",
			2: "company_id",
			3: "amount",
			4: "status",
			5: "created_at",
			6: "updated_at"
			})


		#print(content.info())

		# drop NaN values
		content.dropna(inplace=True)
		content = content.reset_index(drop=True)
		val = content.isnull().any()
		#print("NaN values? \n",val)

		
		# set format with two decimal digits
		content['amount'] = list(map(lambda dec: round(float(dec), 2),
							   content['amount']))

		#print(content['amount'])

		#string to timestamp
		content['created_at'] = list(map(self.time_stamp, content['created_at']))

		#print(content['created_at'])

		
		#string to timestamp
		content['updated_at'] = list(map(self.time_stamp, content['updated_at']))
		#print(content['updated_at'])


		# short id to len(24)
		content['id'] = list(map(lambda dec: dec[:24], content['id']))

		# short company_id to len(24)
		content['company_id'] = list(map(lambda dec: dec[:24], content['company_id']))
		
		return content


	# load data
	def load(self, data):

		"""
		:param data: A DataFrame to load into a DataBase
		:type data: DataFrame

		:return: None
		:rtype: None
		"""
	
		company_name = pd.unique(data['company_name'])
		company_id = pd.unique(data['company_id'])

		# table 'companies'
		companies = [(co_id, co_name) for co_id, co_name in zip(company_id, company_name)]

		

		# table 'charges'
		charges = data[['id', 'company_id', 'amount', 'status', 'created_at', 'updated_at']]
		
		charges = charges.values.tolist()


		try:
			connection = Database().get_conn()
			
			insert_query_companies = "INSERT INTO companies(company_id, company_name)"\
				" VALUES (%s, %s)"

			insert_query_charges = "INSERT INTO charges(id, company_id, amount, status,"\
				" created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s)"
			
			create_view = "CREATE VIEW pruebas.money_paid_by_company AS"\
				" SELECT pruebas.companies.company_name, pruebas.charges.updated_at, SUM(amount) total_paid"\
				" FROM pruebas.charges"\
				" INNER JOIN pruebas.companies"\
				" ON pruebas.charges.company_id = pruebas.companies.company_id"\
				" GROUP BY DATE(updated_at), company_name"\
				" ORDER BY updated_at"

			with connection.cursor() as cursor:

				
				print("Executing 'MODELO.sql'...")
				for command in self.get_file("MODELO.sql"):
					try:
						if command.strip() != '':
							cursor.execute(command)
					except Exception as msg:
						print("Command skipped: {}".format(msg))


				# inserting companies into table "companies"
				print("Loading companies...")
				for company in companies:
					cursor.execute(insert_query_companies, company)

				# inserting charges into table 'charges'
				print("Loading charges...")
				for charge in charges:
					cursor.execute(insert_query_charges, tuple(charge))


				print("Generating a view...")
				cursor.execute(create_view)

			# insert
			connection.commit()
			print("Finished...")

		except Exception as err:
			print("An error has ocurred...{}".format(err))

		finally:
			connection.close()



	def run_etl(self):

		"""
		:param: None
		:rtype: None

		:return: None
		:rtype: None
		"""


		# extract
		content = self.extract()

		# transform
		content = self.transform(content)
		

		# load
		self.load(content)


		# calling view
		print("\n\n")
		charges = self.get_view()
		for index, item in enumerate(charges):
			print("{}, {}\t{}\t{}".format(index,
								 item['company_name'],
								 item['updated_at'],
								 item['total_paid']))

