
from utils.ETL import ETLPipeLine


if __name__=='__main__':
	

	pipe = ETLPipeLine(filename="data.csv")
	
	pipe.run_etl()







