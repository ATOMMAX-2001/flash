import pandas as pd 
import time 
start_time = time.perf_counter()


df = pd.DataFrame(data={
    "name":["abi","abilash","shadow"],
    "score":[100,100,"100"]
})
print(df)

end_time = time.perf_counter()
print("Elasped Time:",end_time-start_time,"Seconds")


