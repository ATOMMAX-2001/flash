from src.core.flash import Series,Dataframe

# df = Series([10,20,30,40,[50,50],{"name":"hello"}])
df = Dataframe([{
        "name":"abilash",
        # "age":21
    },{
        "name":"atom",
        "age":23
    }
],col=["name","age"])
