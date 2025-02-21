from enum import Enum
from  .flash_error import *
from typing import Any,Tuple,Dict,List
import numpy as np

class FrameKind(Enum):
    SINGLECOL=1
    MULTICOL=2


class StringUtils:
    def __init__(self,data:Any) -> np.ndarray:
        self.frame_index,self.frame_data,self.frame_kind= (
            data.frame_index,data.frame_data,data.frame_kind)
    def split(self,delimit) -> np.ndarray:
        if not isinstance(delimit,str) or delimit=="":
            print(invalid_string_arugments(delimit))
            return
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.split(self.frame_data,delimit)
    def trim(self) -> np.ndarray:
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.strip(self.frame_data)
    def ltrim(self) -> np.ndarray:
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.lstrip(self.frame_data)
    def rtrim(self) -> np.ndarray:
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.rstrip(self.frame_data)
    def contains(self,find_key: str) -> np.ndarray:
        if not isinstance(find_key,str) or find_key=="":
            print(invalid_string_arugments(find_key))
            return
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.find(self.frame_data,find_key)
    def replace(self,source:str,dest:str,count=1) -> np.ndarray:
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.replace(self.frame_data,source,dest,count)
    def count(self,key:str):
        if not isinstance(key,str) or key=="":
            print(invalid_string_arugments(key))
            return
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.count(self.frame_data,key)
    def upper(self):
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.upper(self.frame_data)
    def lower(self):
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.lower(self.frame_data)
    def capitalize(self):
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.capitalize(self.frame_data)
    def endswith(self,key):
        if not isinstance(key,str) or key=="":
            print(invalid_string_arugments(key))
            return
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.endswith(self.frame_data,key)
    def gt(self,key):
        if not isinstance(key,str) or key=="":
            print(invalid_string_arugments(key))
            return
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.greater(self.frame_data,key)
    def gte(self,key):
        if not isinstance(key,str) or key=="":
            print(invalid_string_arugments(key))
            return
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.greater_equal(self.frame_data,key)
    def lt(self,key):
        if not isinstance(key,str) or key=="":
            print(invalid_string_arugments(key))
            return
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.less(self.frame_data,key)
    def lte(self,key):
        if not isinstance(key,str) or key=="":
            print(invalid_string_arugments(key))
            return
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.less_equal(self.frame_data,key)
    def eq(self,key):
        if not isinstance(key,str) or key=="":
            print(invalid_string_arugments(key))
            return
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.equal(self.frame_data,key)
    def neq(self,key):
        if not isinstance(key,str) or key=="":
            print(invalid_string_arugments(key))
            return
        if self.frame_kind!=FrameKind.SINGLECOL:
            raise InvalidStringFrames()
        return np.char.not_equal(self.frame_data,key)
    

    
    
    
    
    
    


def parse_multiple_column_data(index: None|str,data: Dict[Any,Any]|np.ndarray) -> Tuple[str|int,Dict[str,List[Any]]]:
    if index ==None:
        index = list(data.keys())
    new_data={}
    index_pos=0
    row_size=[]
    for value in data.values():
        if isinstance(value,list):
            row_size.append(len(value))
            new_data[index[index_pos]]=np.array(value)
            index_pos+=1
        elif isinstance(value,np.ndarray):
            row_size.append(len(value))
            new_data[index[index_pos]]=value
            index_pos+=1
        else:
            raise InvalidDictDataframeData(value)
    row_size=np.array(row_size)
    if not (row_size == row_size[0]).all():
        raise InvalidRowSize(row_size)
    return index,new_data,row_size[0]
        


