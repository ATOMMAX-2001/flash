from src.core.flash import Dataframe
import time 

start_time = time.perf_counter()

df = Dataframe(data={
    "name": ["Abi","Abilash","Akira"],
    "score":[100,None,300]
})

new_df= df.dropna(copy=True)
print(new_df.records())

end_time=time.perf_counter()
print("Elasped time:",end_time-start_time,"Seconds")




