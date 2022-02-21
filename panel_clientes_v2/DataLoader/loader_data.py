import pandas as pd
import numpy as np

class loader:
    def __init__(self, connection):
        self.con = connection

    def read_query(self, query):
        df_result = pd.read_sql(query, con=self.con)
        return df_result

    def close_connection(self):
        self.con.close()
        print('connection close')

