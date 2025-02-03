from colorama import Fore, Style
from typing import Any,List

def red_color(message: str) -> str:
    return Fore.RED+message+Style.RESET_ALL

def yellow_color(message: str) -> str:
    return Fore.YELLOW+message+Style.RESET_ALL

def white_color(message: str) -> str:
    return Fore.WHITE+message+Style.RESET_ALL


class EmptyInitilization(Exception):
    def __init__(self) -> None:
        super().__init__(red_color("\n\nEmpty Initilization"))


class InvalidDataframeData(Exception):
    def __init__(self, got_type:Any) -> None:
        got_type=str(type(got_type)).replace("class '","").replace("'","")
        super().__init__(red_color(f"\n\nDataframe should be in {white_color('<List>')} or {white_color('<Dict>')} type.Got [{yellow_color(got_type)}]"))


class InvalidDictDataframeData(Exception):
    def __init__(self,got_type:Any) -> None:
        got_type=str(type(got_type)).replace("class '","").replace("'","")
        super().__init__(red_color(f"\n\nMulti column dataframe value should be in {white_color('<List>')}. Got [{got_type}]"))

class InvalidColData(Exception):
    def __init__(self,got_type: Any) -> None:
        got_type=str(type(got_type)).replace("class '","").replace("'","")
        super().__init__(red_color(f"\n\nColumns should be in {white_color('<List>')} {red_color('type. Got')} [{yellow_color(got_type)}]"))


class InvalidIndexDataValues(Exception):
    def __init__(self,got_type: Any) -> None:
        got_type=str(type(got_type)).replace("class '","").replace("'","")
        super().__init__(red_color(f"\n\nIndex content should be in {white_color('<Str>')} type.Got [{got_type}]"))


class InvalidIndexSize(Exception):
    def __init__(self,index_size:int ,data_size: int) -> None:
        super().__init__(red_color(f"\n\nIndex size is not same as dataframe size. Got [{yellow_color(str(index_size))}], need [{yellow_color(str(data_size))}]"))


class InvalidRowSize(Exception):
    def __init__(self,row_size: List[Any]):
        super().__init__(red_color(f"\n\n All the row must be of same size. Got {yellow_color(str(row_size))} {yellow_color('row' if row_size==1 else 'rows')}"))


class InvalidFrameKey(Exception):
    def __init__(self, col):
        super().__init__(red_color(f"\n\nColumn [{col}] does not exist in the dataframe"))

class InvalidFrameCol(Exception):
    def __init__(self,got_size:int,old_size:int):
        super().__init__(red_color(f"\n\n Invalid column replacement, Dataframe have {yellow_color(str(old_size))} {red_color('columns')}, {red_color('But got')} {yellow_color(str(got_size))}"))

class ReadCsvEngineFailed(Exception):
    def __init__(self,message:str):
        super().__init__(red_color(f"Failed to parse the csv file (reason) => {message}"))

class WriteCsvEngineFailed(Exception):
    def __init__(self,message:str):
        super().__init__(red_color(f"Failed to write the csv file (reason) => {message}"))


class ReadXlEngineFailed(Exception):
    def __init__(self,message:str):
        super().__init__(red_color(f"Failed to parse the XL file (reason) => {message}"))

class WriteXlEngineFailed(Exception):
    def __init__(self, message):
        super().__init__(red_color(f"Failed to write the XL file (reason) => {message}"))


class InvalidFlashDataframe(Exception):
    def __init__(self,):
        super().__init__(red_color(f"Need valid flash data_frame"))


class InvalidMergeColumn(Exception):
    def __init__(self,first_size,second_size):
        super().__init__(red_color(f"Both the dataframe should have same number of column,GOT: {yellow_color(f'{first_size,second_size}')}"))




def invalid_frame_key_error(col:str ) -> str: 
    return red_color(f"\n\n[Error]: Column [{col}] does not exist in the dataframe")

def passing_index_in_series():
    print(yellow_color("\n\n[Note]: You cant index a series or a single column frame\n"))


def invalid_record_size(size:int) -> str:
    return yellow_color("\n\n[Error]: Record size should be in type:<Int>")
