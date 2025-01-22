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
        super().__init__(red_color(f"\n\nDataframe should be in {white_color("<List>")} or {white_color("<Dict>")} type.Got [{yellow_color(got_type)}]"))

class InvalidDictDataframeData(Exception):
    def __init__(self,got_type:Any) -> None:
        got_type=str(type(got_type)).replace("class '","").replace("'","")
        super().__init__(red_color(f"\n\nMulti column dataframe value should be in {white_color("<List>")}. Got [{got_type}]"))
class InvalidIndexData(Exception):
    def __init__(self,got_type: Any) -> None:
        got_type=str(type(got_type)).replace("class '","").replace("'","")
        super().__init__(red_color(f"\n\nIndex should be in {white_color("<List>")} type. Got [{got_type}]"))


class InvalidIndexDataValues(Exception):
    def __init__(self,got_type: Any) -> None:
        got_type=str(type(got_type)).replace("class '","").replace("'","")
        super().__init__(red_color(f"\n\nIndex content should be in {white_color("<Str>")} type.Got [{got_type}]"))


class InvalidIndexSize(Exception):
    def __init__(self,index_size:int ,data_size: int) -> None:
        super().__init__(red_color(f"\n\nIndex size is not same as dataframe size. Got [{yellow_color(str(index_size))}], need [{yellow_color(str(data_size))}]"))


class InvalidRowSize(Exception):
    def __init__(self,row_size: List[Any]):
        super().__init__(red_color(f"\n\n All the row must be of same size. Got {yellow_color(str(row_size))} {yellow_color('row' if row_size==1 else 'rows')}"))


class InvalidFrameKey(Exception):
    def __init__(self, col):
        super().__init__(red_color(f"\n\nColumn [{col}] does not exist in the dataframe"))

def invalid_frame_key_error(col) -> str: 
    return red_color(f"\n\nColumn [{col}] does not exist in the dataframe")

def passing_index_in_series():
    print(yellow_color("\n\n[Note]: You cant index a series or a single column frame\n"))


