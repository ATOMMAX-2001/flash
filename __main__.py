from src.core.flash import Dataframe
import src.core.flashio as flash
import time 

start_time = time.perf_counter()

df = flash.read_csv("./test/sample.csv")
df.dropna()

flash.write_csv(df,"./test/test.csv")
end_time=time.perf_counter()
print("Elasped time:",end_time-start_time,"Seconds")




