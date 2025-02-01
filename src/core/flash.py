import numpy as np
from  .flash_error import *
from .frame import *
from copy import deepcopy
from typing import Self,Any


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
        "version": ["0.1.0"],
        "in_dict_format": [{"name":"abi"}]
    })
    # here value should be in list type represent each row for a column
    """ 
    def __init__(self,data=None,index=None) -> None:
        if data is None:
            raise EmptyInitilization()
        if index !=None:
            passing_index_in_series()
        if isinstance(data,list):
            self.frame_kind = FrameKind.SINGLECOL
            self.frame_data=np.array(data)
            self.frame_index=np.arange(len(self.frame_data))
        elif isinstance(data,np.ndarray):
            self.frame_kind = FrameKind.SINGLECOL
            self.frame_data=data
            self.frame_index=np.arange(len(self.frame_data))
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
    def keys(self) -> List[str|int]:
        return self.frame_index

    def values(self) -> np.ndarray|List[Any]:
        if self.frame_kind==FrameKind.SINGLECOL:
            return self.frame_data
        result = []
        for col in self.frame_data.keys():
            result.append(self.frame_data[col])
        return result
    def isEmpty(self,col=None) -> bool|InvalidFrameKey:
        if self.frame_kind == FrameKind.SINGLECOL:
            return len(self.frame_data)==0
        if self.frame_data.get(col,None) is None:
            raise InvalidFrameKey(col)
        return self.frame_data[col].size==0
    def apply(self,function,col:np.ndarray|None=None):
        if self.frame_kind == FrameKind.SINGLECOL:
            return np.array(list(map(function,self.frame_data)))
        if  not isinstance(col,np.ndarray):
            raise InvalidFlashDataframe
        return np.array(list(map(function,col)))
    def merge(self,df:Self):
        if not isinstance(df,Dataframe):
            raise InvalidFlashDataframe()
        if self.frame_kind == FrameKind.SINGLECOL:
            result = np.append(self.frame_data,df.values())
            new_obj = Dataframe(result)
            del result
            return new_obj
        else:
            if len(self.frame_index) != len(df.columns()):
                first_size= len(self.frame_index)
                second_size = len(df.columns())
                raise InvalidMergeColumn(first_size,second_size)
            result = {}
            second_frame = df.values()
            for (index,col) in enumerate(self.frame_index):
                result[col] = np.append(self.frame_data[col],second_frame[index])
            new_obj = Dataframe(result)
            del result,second_frame
            return new_obj
            
    def distinct(self,col:str|None=None):
        if self.frame_kind == FrameKind.SINGLECOL:
            new_obj = Dataframe(data=np.unique(self.frame_data))
            return new_obj
        if col is None:
            distinct_result = np.transpose(np.unique(self.records(),axis=0))
            result ={}
            for index,col in enumerate(self.frame_index):
                result[col] =  distinct_result[index]
            new_obj = Dataframe(data=result)
            return new_obj
        if col not in self.frame_index:
            raise InvalidFrameKey(col)
        new_obj = Dataframe(data=np.unique(self.frame_data[col]))
        return new_obj
        

    def copy(self):
        return deepcopy(self)
    def filter(self,cond,copy=False):
        result,new_result=None,[]
        if self.frame_kind == FrameKind.SINGLECOL:
            new_result = self.frame_data[~cond]
            print("DEBUG:",new_result)
        else:
            result = np.transpose(list(self.frame_data.values()))
            new_result= np.transpose(result[~cond])
        if copy:
            if self.frame_kind==FrameKind.SINGLECOL:
                new_obj = Dataframe(data=new_result)
                del new_result,cond
                return new_obj
            else:
                new_copy= deepcopy(self.frame_data)
                for index,key in enumerate(new_copy.keys()):
                    new_copy[key] = new_result[index]
                new_obj = Dataframe(data=new_copy)
                del new_result,cond
                return new_obj
        else:
            if self.frame_kind==FrameKind.SINGLECOL:
                self.frame_data=new_result
                return None
            for index,key in enumerate(self.frame_data.keys()):
                self.frame_data[key]=new_result[index]
            del new_result
            return None
    def where(self,cond) -> List[Any]:
        if self.frame_kind == FrameKind.SINGLECOL:
            new_result = np.transpose(self.frame_data[cond])
            new_obj= Dataframe(new_result)
            del new_result
            return new_obj
        result = np.transpose(list(self.frame_data.values()))
        try:
            new_result =np.transpose(result[cond])
        except Exception as e:
            raise ValueError("Invalid Condition")
        new_data={}
        for index,key in enumerate(self.frame_data.keys()):
                new_data[key]=new_result[index]
        new_obj= Dataframe(new_data)
        del result,new_data,new_result
        return new_obj
        
    def dropna(self,copy=False) -> Any:
        result,mask,new_result=None,None,[]
        if self.frame_kind == FrameKind.SINGLECOL:
            if np.any(self.frame_data==None):
                new_result=[]
            else:
                new_result=self.frame_data
        else:
            result = np.transpose(list(self.frame_data.values()))
            mask = np.array([np.any((row == None)| (row == "None")|(row == "null") | (str(np.nan) in str(row))) for row in result])
            new_result= np.transpose(result[~mask])
        if copy:
            if self.frame_kind==FrameKind.SINGLECOL:
                new_obj = Dataframe(data=new_result)
                del new_result,mask
                return new_obj
            else:
                new_copy= deepcopy(self.frame_data)
                for index,key in enumerate(new_copy.keys()):
                    new_copy[key] = new_result[index]
                new_obj = Dataframe(data=new_copy)
                del new_result,mask
                return new_obj
        else:
            if self.frame_kind==FrameKind.SINGLECOL:
                self.frame_data=new_result
                return None
            for index,key in enumerate(self.frame_data.keys()):
                self.frame_data[key]=new_result[index]
            del new_result,mask
            return None
            
        
    def tail(self,row_length=5) -> List[Any]|None:
        if not isinstance(row_length,int):
            print(invalid_record_size(row_length))
            return
        col = "\t".join(list(map(lambda x: f"[{str(x).strip()}]",self.frame_index)))
        output=f"[Index]\t{col}\n"
        result=None
        if self.frame_kind == FrameKind.SINGLECOL:
            output=f"[Index]\t[Value]\n"
            for col in range(1,row_length+1):
                output +=str(self.frame_index[-col])+"\t"+str(self.frame_data[-col])+"\n"
            del result
            return output
        else:
            result = np.transpose(list(self.frame_data.values()))[-row_length:]
        index=0
        for row in result:
            output+=str(index)+"\t"
            for i in range(len(self.frame_index)):
                output+=row[i]+"\t"
            output+="\n"
            index+=1
        del result
        return output

    def head(self,row_length=5) -> List[Any]:
        if not isinstance(row_length,int):
            print(invalid_record_size(row_length))
            return
        col = "\t".join(list(map(lambda x: f"[{str(x).strip()}]",self.frame_index)))
        output=f"[Index]\t{col}\n"
        result=None
        if self.frame_kind == FrameKind.SINGLECOL:
            output=f"[Index]\t[Value]\n"
            for col in range(row_length):
                output +=str(self.frame_index[col])+"\t"+str(self.frame_data[col])+"\n"
            del result
            return output
        else:
            result = np.transpose(list(self.frame_data.values()))[:row_length]
            index=0
            for row in result:
                output+=str(index)+"\t"
                for i in range(len(self.frame_index)):
                    output+=str(row[i])+"\t"
                output+="\n"
                index+=1
            del result
            return output
    def records(self) -> np.ndarray:
        if self.frame_kind == FrameKind.SINGLECOL:
            return list(self.frame_data)
        return np.transpose(list(self.frame_data.values()))
    def remove(self,key,copy=False) -> Self|None:
        if self.frame_kind ==FrameKind.SINGLECOL:
            if key not in self. frame_index:
                raise InvalidFrameKey(key)
            pos = list(self.frame_index).index(key)
            if copy:
                new_data = np.delete(self.frame_data,pos)
                new_obj = Dataframe(data=new_data)
                return new_obj
            else:
                self.frame_data = np.delete(self.frame_data,pos)
                list(self.frame_index).remove(key)
            return None
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
    def setCol(self,col=[]):
        if not isinstance(col,list):
            raise InvalidColData(col)
        if len(col) != len(self.frame_index):
            raise InvalidFrameCol(len(col),len(self.frame_index))
        self.frame_index = col
    def columns(self) -> List[str]:
        return self.frame_index
    def update(self,key=None,value=None,copy=False) -> None:
        if self.frame_kind==FrameKind.SINGLECOL:
            if key not in self.frame_index:
                raise InvalidFrameKey(key)
        else:
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
    def __version__(self) -> None:
        print("Flash: V0.1.0")
    def __author__(self) -> None:
        print("Flash is developed by S.Abilash")
    def __iter__(self) -> Dict[str|int,List[Any]]:
        return iter(self.frame_data.items())
    def __getitem__(self,key) -> List[Any]:
        if self.frame_kind == FrameKind.SINGLECOL:
            if key not in self.frame_index:
                print(invalid_frame_key_error(key))
                return    
        else:
            if self.frame_data.get(key,None) is None:
                print(invalid_frame_key_error(key))
                return
        return self.frame_data[key]
    def __setitem__(self, key,value) -> Dict[str|int,List[Any]]:
        if self.frame_kind == FrameKind.SINGLECOL:
            if key not in self.frame_index:
                print(invalid_frame_key_error(key))
                return    
        else:
            if self.frame_data.get(key,None) is None:
                self.frame_index.append(key)
            if value is None:
                value= [None] * len(self.frame_data[next(iter(self.frame_data))])
            else:
                if len(value) != len(self.frame_data[next(iter(self.frame_data))]):
                    raise InvalidRowSize(len(value))
        self.frame_data[key]=value
    def __str__(self) -> str:
        output="\n"
        if self.frame_kind==FrameKind.SINGLECOL:
            output+="[Index]\t [Values]\n"
            for index,elem in enumerate(self.frame_data[:10]):
                output+=f"{self.frame_index[index]}\t {str(elem)}\n"
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
        

