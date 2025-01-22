from src.core.flash import Dataframe
import time 

start_time = time.perf_counter()
df = Dataframe(data={
    "name":["ABI","ABILASH","Akira"],
    "score":[100,100,100]
})

print(df["name"])
end_time=time.perf_counter()
print("Elasped time:",end_time-start_time,"Seconds")




