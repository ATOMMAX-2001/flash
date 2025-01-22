from enum import Enum
from  .flash_error import *
from typing import Any,Tuple,Dict,List
import numpy as np


class FrameKind(Enum):
    SINGLECOL=1
    MULTICOL=2



def parse_multiple_column_data(index: None|str,data: Dict[Any,Any]) -> Tuple[str|int,Dict[str,List[Any]]]:
    if index ==None:
        index = list(data.keys())
    new_data={}
    index_pos=0
    row_size=[]
    for value in data.values():
        if isinstance(value,list) or isinstance(value,np.ndarray):
            row_size.append(len(value))
            new_data[index[index_pos]]=np.array(value)
            index_pos+=1
        else:
            raise InvalidDictDataframeData(value)
    row_size=np.array(row_size)
    if not (row_size == row_size[0]).all():
        raise InvalidRowSize(row_size)
    return index,new_data,row_size[0]
        