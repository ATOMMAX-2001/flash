import numpy as np
import duckdb as duck
from  .flash_error import *
from .frame import *
from copy import deepcopy



class Dataframe: 
    """
    if data object type is 
    1 -> List [Single column]
    2 -> Dict [Multiple column]
    eg:
    >>> df = Dataframe(data=[1,3.3,9.3,"hello world",{"name":"abi"},[4,5,6]])
    # here each element represent as row
    >>> df = Dataframe(data={
        "name": ["flash"],
        "version": ["0.0.1"],
        "in_dict_format": [{"name":"abi"}]
    })
    # here value should be in list type represent each row for a column
    """ 
    def __init__(self,data=None,index=None) -> None:
        if data==None:
            raise EmptyInitilization()
        if index !=None:
            passing_index_in_series()
        if isinstance(data,list):
            self.frame_kind = FrameKind.SINGLECOL
            self.frame_data=np.array(data)
            self.index=np.arange(len(self.frame_data))
        elif isinstance(data,dict):
            self.frame_kind=FrameKind.MULTICOL
            if index!=None:
                if isinstance(index,list):
                    for col in index:
                        if not isinstance(col,str):
                            raise InvalidIndexDataValues(col)
                else:
                    raise InvalidDataframeData()
            self.frame_index,self.frame_data,self.frame_size = parse_multiple_column_data(index,data)
        else:
            data_type = type(data)
            raise InvalidDataframeData(data_type)
    def memspace(self) -> int:
        return self.frame_data.__sizeof__()
    def dim(self) -> int:
        return 1 if self.frame_kind==FrameKind.SINGLECOL else len(self.frame_index)
    def size(self) -> Tuple[int,int]:
        if self.frame_kind == FrameKind.SINGLECOL:
            return {"row":1,"col":len(self.frame_data)}
        else:
            return{
                "row": len(self.frame_index),
                "col": self.frame_size
            }
    def key(self) -> List[str|int]:
        return self.frame_index
    def values(self) -> List[Any]:
        return list(self.frame_data.values())
    def isEmpty(self,col) -> bool|InvalidFrameKey:
        if self.frame_data.get(col,None) is None:
            raise InvalidFrameKey(col)
        return self.frame_data[col].size==0

    def tail(self,row_length=5) -> List[Any]:
        col = "\t".join(list(map(lambda x: f"[{x.strip()}]",self.frame_index)))
        output=f"[Index]\t{col}\n"
        index=0
        result =[]
        overall_value_result=[]
        for i in list(self.frame_data.values()):
            overall_value_result.append(i[::-1])
        row =[]
        while index < len(overall_value_result[0]):
            if index == row_length:
                break
            for i in overall_value_result:
                row.append(str(i[index]))
            result.append(row)
            row=[]
            index+=1
        del row,col
        index=0
        for row in result:
            output+=str(index)+"\t"
            for col in row:
                output+=f"{str(col)}\t"
            output+="\n"
            index+=1
        return output
    def head(self,row_length=5) -> List[Any]:
        col = "\t".join(list(map(lambda x: f"[{x.strip()}]",self.frame_index)))
        output=f"[Index]\t{col}\n"
        index=0
        result =[]
        overall_value_result=[]
        for i in list(self.frame_data.values()):
            overall_value_result.append(i)
        row =[]
        while index < len(overall_value_result[0]):
            if index == row_length:
                break
            for i in overall_value_result:
                row.append(str(i[index]))
            result.append(row)
            row=[]
            index+=1
        del row,col
        index=0
        for row in result:
            output+=str(index)+"\t"
            for col in row:
                output+=f"{str(col)}\t"
            output+="\n"
            index+=1
        return output
    def remove(self,key,copy=False) -> Dict[str|int,List[Any]]|None:
        if self.frame_data.get(key,None) is None:
            raise InvalidFrameKey(key)
        if copy:
            copy_data = deepcopy(self.frame_data)
            del copy_data[key]
            new_df = Dataframe(data=copy_data)
            return new_df
        else:
            self.frame_index.remove(key)
            del self.frame_data[key]
    def update(self,key=None,value=None,copy=False) -> None:
        if self.frame_data.get(key,None) is None:
            raise InvalidFrameKey(key)
        if value ==None:
            value= [None] * len(self.frame_data[next(iter(self.frame_data))])
        if copy:
            copy_data = deepcopy(self.frame_data)
            copy_data[key] = value
            new_df = Dataframe(data=copy_data)
            return new_df
        else:
            self.frame_data[key]=value

    def __iter__(self) -> Dict[str|int,List[Any]]:
        return iter(self.frame_data.items())
    def __getitem__(self,key) -> List[Any]:
        if self.frame_data.get(key,None) is None:
            print(invalid_frame_key_error(key))
            return
        return self.frame_data[key]
    def __setitem__(self, key,value) -> Dict[str|int,List[Any]]:
        if self.frame_data.get(key,None) is None:
            self.frame_index.append(key)
        if len(value) != len(self.frame_data[next(iter(self.frame_data))]):
            raise InvalidRowSize(len(value))
        self.frame_data[key]=value
    def __str__(self) -> str:
        output="\n"
        if self.frame_kind==FrameKind.SINGLECOL:
            output+="[Index]\t [Values]\n"
            for index,elem in enumerate(self.frame_data[:10]):
                output+=f"{index}\t {str(elem)}\n"
            if len(self.frame_data)>10:
                output+="----\t -------\n"
                output+=f"{len(self.frame_data)-10} more records\n"
                output+="----\t -------\n"
        else:
            col = "\t".join(list(map(lambda x: f"[{x.strip()}]",self.frame_index)))
            output+=f"[Index]\t{col}\n"
            index=0
            result =[]
            overall_value_result=[]
            for i in list(self.frame_data.values()):
                overall_value_result.append(i)
            row =[]
            while index < len(overall_value_result[0]):
                if index>10:
                    break
                for i in overall_value_result:
                    row.append(str(i[index]))
                result.append(row)
                row=[]
                index+=1
            del row,col
            index=0
            for row in result:
                output+=str(index)+"\t"
                for col in row:
                    output+=f"{str(col)}\t"
                output+="\n"
                index+=1
            if len(overall_value_result[0])>10:
                output+="----\t -------\n"
                output+=f"{len(overall_value_result[0])-11} more records\n"
                output+="----\t -------\n"
            
        return output
        