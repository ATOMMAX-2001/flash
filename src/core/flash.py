import duckdb
import numpy as np
from datetime import datetime
from typing import Tuple

class Dataframe:
    def __init__(self,data: list[any]=None,name=None,cache=False,col=False) -> None:
        if data==None:
            raise Exception("Error:","Need Dataframe")
        if not isinstance(data,list):
            raise Exception("Error","Dataframe should be in list type")
        if cache==True and name==None:
            raise Exception("Error:","Need [name] argument to cache")
        if len(data)==0:
            raise Exception("Error:","Empty Dataset")
        if not isinstance(col,list):
            raise Exception("Error:","Columns should be in list type")
        self.__conn = self.__create_memory_frame(data,name,cache,col)



        result = self.__conn.sql(f"select * from {self.__name}")        
        print(result.fetchall())



    def __create_memory_frame(self,data: list[any],name:str,cache: bool,col: list[str]) -> duckdb.connect:
        if cache:
            import os 
            path = os.getenv("tmp","") + "\\"
            conn = duckdb.connect(path+name)
            if col==None:
                col = [f"{i} varchar default null" for i in data[0].keys()]
            else:
                col = [f"{str(i)} varchar default null" for i in col]
            conn.execute("drop table "+name)
            conn.execute(f"create table {name} (index bigint primary key not null,{",".join(col)})")
            self.__name= name
            for (index,record) in enumerate(np.array(data)):
                    conn.execute(f"insert into {self.__name}(index,{','.join(record.keys())}) values ({('?,'*(len(record.keys())+1))[:-1]})",(index,*record.values()))
            conn.commit()
            return conn
        else:
            conn = duckdb.connect()
            try:
                if col==None:
                    col = [f"{i} varchar default null" for i in data[0].keys()]
                else:
                    col = [f"{str(i)} varchar default null" for i in col]
                self.__name = "tbl_"+datetime.now().strftime("%Y%m%d%H%M%S")
                conn.execute(f"create table {self.__name} (index varchar primary key not null,{",".join(col)})")
                for (index,record) in enumerate(np.array(data)):
                    conn.execute(f"insert into {self.__name}(index,{','.join(record.keys())}) values ({('?,'*(len(record.keys())+1))[:-1]})",(index,*record.values()))

                conn.commit()
            except duckdb.BinderException as e:
                raise Exception("EXCEPTION:",e)
            except duckdb.InvalidInputException as e:
                raise Exception("EXCEPTION",e)
            return conn



class Series:
    def __init__(self,data: list[any]=None,col: list[str]=None) -> None:
        if data==None:
            raise Exception("Error:","Need series data")
        if not isinstance(data,list):
            raise Exception("Error:","Series should be in list type")
        
        self.frame,self.column = self.__transform(data,col)
        self.__name = "tbl_"+datetime.now().strftime("%Y%m%d%H%M%S")
        self.__conn = self.__create_memory_frame()
        
        
        result = self.__conn.sql(f"select * from {self.__name}")        
        print(result.fetchall())
    
    def __create_memory_frame(self) -> duckdb.connect:
        conn = duckdb.connect()
        try:
            conn.execute(f"create table {self.__name} (series_key varchar primary key not null,series_value varchar)")
            query_value  = [(key,value) for (key,value) in self.frame.items()]
            conn.executemany(f"insert into {self.__name} values(?,?)",query_value)
            conn.commit()
        except Exception as e:
            print("Exception:","Failed to store frames coz => ",e)
        return conn
    def __transform(self,data: list[any],col:list[str]) -> Tuple[dict,list[str]]:
        transform_data = {}
        if col==None:
            for (index,value) in enumerate(np.array(data,dtype=object)):
                transform_data[str(index)]=str(value)
            col=transform_data.keys()
        else:
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

