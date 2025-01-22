import duckdb
import numpy as np
from datetime import datetime
from typing import Tuple
import pretty_tables as pt
import os
import multiprocessing as mp    
from threading import Thread

class Dataframe:
    def __init__(self,data: list[any]=None,name=None,cache=None,col=None,recache=False) -> None:
        if data==None:
            raise Exception("Error:","Need Dataframe")
        if not isinstance(data,list):
            raise Exception("Error","Dataframe should be in list type")
        if cache!= None and not cache.isalpha():
            raise Exception("Error:","Need [name] argument to cache")
        if cache and recache:
            raise Exception("Error:","No Need to pass [cache] argument")
        if len(data)==0:
            raise Exception("Error:","Empty Dataset")
        if not isinstance(col,list):
            raise Exception("Error:","Columns should be in list type")
        self.__conn = self.__create_memory_frame(data,name,cache,col,recache)
    
    def __transform(self,data: np.array) -> np.array:
        for record in data:
                    missing_key = set(self.columns[1:])-set(record.keys())
                    if len(self.columns[1:]) != len(record.keys()):
                        error_message = f"Number of columns provided [{len(self.columns[1:])}] does not match with the data [{len(record.keys())}] for {record}"
                        raise Exception("Error:",error_message)
                    if len(missing_key)!=0:
                        record[missing_key]=None
        return data
    def __create_memory_frame(self,data: list[any],name:str,cache: bool,col: list[str],recache:bool) -> duckdb.connect:
        data= np.array(data)
        if cache:
            path = os.getenv("temp","") + "\\"
            conn = duckdb.connect(path+name)
            result = conn.execute(f"select exists(select 1 from information_schema.tables where table_name='{name}')")
            if result.fetchone()[0]:
                self.__name=name
                if col==None:
                    self.columns = data[0].keys()       
                    self.columns.insert(0,"index")        
                else:
                    self.columns = col
                    self.columns.insert(0,"index")
                return conn
            else:
                if recache:
                    conn.execute("drop table ",self.__name)
            if col==None:
                self.columns = data[0].keys()
                self.columns.insert(0,"index")        
                col = [f"{i} varchar default null" for i in data[0].keys()]
            else:
                self.columns = col
                self.columns.insert(0,"index")
                col = [f"{str(i)} varchar default null" for i in col]
            try:
                conn.execute("create sequence index_id start 1")
                conn.execute(f"create table {self.__name} (id bigint primary key default nextval('index_id') not null,{','.join(col[1:])})")
                self.__name= name
                for record in data:
                        conn.execute(f"insert into {self.__name}({','.join(record.keys())}) values ({('?,'*len(record.keys()))[:-1]})",*record.values())
                conn.commit()
            except duckdb.CatalogException  as e:
                raise Exception("Exception:",e)
            except duckdb.BinderException as e:
                raise Exception("EXCEPTION:",e)
            except duckdb.InvalidInputException as e:
                raise Exception("EXCEPTION",e)
            return conn
        else: #not cache
            conn = duckdb.connect()
            try:
                if col==None:
                    self.columns = data[0].keys()
                    self.columns.insert(0,"index") 
                    col = [f"{i} varchar default null" for i in data[0].keys()]
                else:
                    self.columns = col
                    col = [f"{str(i)} varchar default null" for i in col]
                    self.columns.insert(0,"index")
                    
                self.__name = "tbl_"+datetime.now().strftime("%Y%m%d%H%M%S")
                conn.execute("create sequence index_id start 1")
                conn.execute(f"create table {self.__name} (id bigint primary key default nextval('index_id') not null,{','.join(col)})")
                data = self.__transform(data)
                data_size= len(data)
                chunk_size = int(len(data)/mp.cpu_count())

                def insert_record(db_name: str,data: list[dict],columns: list[str],new_conn: duckdb.connect) -> None:            
                    new_conn.executemany(f"insert into {db_name} ({','.join(columns)}) values ({('?,'*len(columns))[:-1]})",data)
                    new_conn.commit()
                start=0
                end=chunk_size
                my_worker=[]
                while start<=data_size:
                    result = [record.values() for record in data[start:end]]
                    if len(result)==0:
                        break
                    work = Thread(target=insert_record,args=(self.__name,result,self.columns[1:],conn))
                    my_worker.append(work)
                    work.start()
                    start=end
                    end+=chunk_size
                for work in my_worker:
                    work.join()
                    conn.execute(f"insert into {self.__name}({','.join(record.keys())}) values ({('?,'*len(record.keys()))[:-1]})",[*record.values()])
                conn.commit()
            except duckdb.BinderException as e:
                raise Exception("EXCEPTION:",e)
            except duckdb.InvalidInputException as e:
                raise Exception("EXCEPTION",e)
            return conn
    def delcache(self):
        self.__conn.execute("drop table ",self.__name)
        self.__conn.commit()
    
    def display(self) -> None:
        result = [list(i)for i in self.__conn.sql(f"select * from {self.__name} limit 10").fetchall()] 
        if len(result)==0:
            print("*"*5,"Flash Frame","*"*5)
            print("<EMPTY>")
            return
        table =pt.create(
            headers=self.columns,
            rows=result,
        )
        print("*"*5,"Flash Frame","*"*5)
        print(table)
    def __exit__(self) -> None:
        self.__conn.close()
        


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

