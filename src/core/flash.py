import duckdb
import numpy as np
from datetime import datetime
from typing import Tuple
class Series:
    def __init__(self,data: list[int | str]=None,col: list[str]=None) -> None:
        if data==None:
            raise Exception("Error:","Need series data")
        if not isinstance(data,list):
            raise Exception("Error:","Series should be in list type")
        
        self.frame,self.column = self.__transform(data,col)
        self.__name = "tbl_"+datetime.now().strftime("%Y%m%d%H%M%S")
        self.__conn = self.__create_in_memory_frame()
        result = self.__conn.sql(f"select * from {self.__name}")        
        print(result.fetchall())
    
    def __create_in_memory_frame(self) -> duckdb.connect:
        conn = duckdb.connect()
        try:
            conn.execute(f"create table {self.__name} (series_key varchar primary key not null,series_value varchar)")
            query_value  = [(key,value) for (key,value) in self.frame.items()]
            conn.executemany(f"insert into {self.__name} values(?,?)",query_value)
            conn.commit()
        except Exception as e:
            print("Exception:","Failed to store frames coz => ",e)
        return conn
    def __transform(self,data: list[int | str],col:list[str]) -> Tuple[dict,list[str]]:
        transform_data = {}
        if col==None:
            for (index,value) in enumerate(data):
                transform_data[str(index)]=str(value)
            col=transform_data.keys()
        if len(col) != len(set(col)):
            raise Exception("Error:","Duplicate columns found")
        else:
            if not isinstance(col,list):
                raise Exception("Error:","Columns has to be in list type")
            if len(data) != len(col):
                raise Exception("Error:","Number of column name provided does not match with the data")
            index=0
            for value in np.array(data,dtype=object):
                transform_data[col[index]]=str(value)
                index+=1
        return (transform_data,col)

    def __exit__(self) -> None:
        self.__conn.close()

