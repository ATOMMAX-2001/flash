import pyarrow.csv as csv_engine
from python_calamine import CalamineWorkbook
from .flash import Dataframe
from  .flash_error import *
import numpy as np


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
    