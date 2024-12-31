# Scenarios
Scenario: 1 Query to get who are getting equal salary
Input: ![image](https://github.com/user-attachments/assets/448ddf3c-4d3f-45ec-9a2b-8838b9839f2e)



Output:  ![image](https://github.com/user-attachments/assets/947c2565-d480-4262-bcd4-ed590d6490da)




Solution: # Create the data
data = [
    ("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", "HR"),
    ("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),
    ("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),
    ("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),
    ("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")
]

# Define the column names
columns = ["workerid", "firstname", "lastname", "salary", "joiningdate", "depart"]

# Create the DataFrame
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

filterdf = df.groupBy("salary").count()
filterdf.show()


df1= df.join(filterdf,["salary"], "left").orderBy("workerid").filter("count>1").drop("count")
df1.show()


finaldf = df1.select("workerid","firstname","lastname","salary","joiningdate","depart")
finaldf.show()
