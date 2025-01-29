import pandas as pd 
import time 
start_time = time.perf_counter()


df = pd.read_excel("./../../../deepak_postpaid_line.xlsx",sheet_name=0,engine='calamine')
df.to_excel("pandas_test.xlsx")

end_time = time.perf_counter()
print("Elasped Time:",end_time-start_time,"Seconds")


