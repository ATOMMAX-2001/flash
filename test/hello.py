import pandas as pd 
import time 
start_time = time.perf_counter()


df = pd.read_csv("./../../../deepak_postpaid_line.csv",engine="pyarrow")

end_time = time.perf_counter()
print("Elasped Time:",end_time-start_time,"Seconds")


