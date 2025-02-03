import pyarrow.csv as csv_engine
from python_calamine import CalamineWorkbook
from .flash import Dataframe
from  .flash_error import *
from typing import Dict
import numpy as np
import xlsxwriter
import multiprocessing as mp

def read_csv(filename) -> Dataframe:
        try:
            records = csv_engine.read_csv(filename)
        except Exception as e:
             error =str(e)
             error = error.replace("pyarrow","flash")
             raise ReadCsvEngineFailed(error)
             
        result = {col:None for col in records.column_names}
        for col in result.keys():
            result[col] = records.column(col).to_numpy()
        del records
        new_obj = Dataframe(data=result)
        del result
        return new_obj

def write_csv(df: np.ndarray,filename: str,delimit=",") -> None:
     if not  isinstance(df,Dataframe):
        raise InvalidFlashDataframe()
     try: 
        result = df.records()
        header = delimit.join(df.columns())
        np.savetxt(filename,result,delimiter=delimit,fmt="%s",header=header,comments="")
     except Exception as e:
        error =str(e)
        error = error.replace("pyarrow","flash")
        raise WriteCsvEngineFailed(error)
    

def excel_read_task(df:List[Any],id:int,storage:Dict[Any,Any]):
   header = df[0]
   rows = np.transpose(df[1:])
   result = {col:None for col in header}
   for index in range(len(result.keys())):
         result[header[index]] = rows[index]
   new_obj = Dataframe(result)
   storage[id]=new_obj
   del new_obj


def read_excel(filename,sheet_id=None,is_header=True) -> Dict[int,Dataframe]|Dataframe:
   if sheet_id is not None and not isinstance(sheet_id,int):
       error ="Sheet_id should be in type: <Int>"
       raise ReadXlEngineFailed(error)
   if is_header not in [True,False]:   
       error ="header should be in type: <Bool>"
       raise ReadXlEngineFailed(error)
   try:
      workbook = CalamineWorkbook.from_path(filename)
      if sheet_id ==None:
         manager = mp.Manager()
         process_storage = manager.dict()
         my_worker = []
         for id in range(len(workbook.sheet_names)):
            df = workbook.get_sheet_by_index(id).to_python()
            worker = mp.Process(target=excel_read_task,args=(df,id,process_storage))
            worker.start()
            my_worker.append(worker)
         for worker in my_worker:
            worker.join()
         return process_storage
      else:
         df = workbook.get_sheet_by_index(sheet_id).to_python()
         header = df[0]
         rows = np.transpose(df[1:])
         result = {col:None for col in header}
         for index in range(len(result.keys())):
               result[header[index]] = rows[index]
         new_obj = Dataframe(result)
         del result,rows
         return new_obj
   except Exception as e:
      error =str(e)
      error = error.replace("calamine","flash")
      raise ReadXlEngineFailed(error)


def write_excel(df: np.ndarray,filename: str,sheet_name: str):
    if not  isinstance(df,Dataframe):
        raise InvalidFlashDataframe()
    try: 
        result = df.records().astype(str)
        header = df.columns()
        workbook = xlsxwriter.Workbook(filename)
        worksheet = workbook.add_worksheet(sheet_name)
        worksheet.write_row(0,col=0,data=header)
        for (index,rows) in enumerate(result):
            worksheet.write_row(index+1,col=0,data=rows)
        workbook.close()
    except Exception as e:
      error =str(e)
      error = error.replace("calamine","flash")
      raise ReadXlEngineFailed(error)
        
    