import pandas as pd

def etl_process():
  # Extract
  # In case I dont have a database, I wil create a sample data
  data = {
      'Name': ['John', 'Anna', 'Peter', 'Linda', 'David'],
      'Age': [25, 30, 35, 28, 40],
      'City': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Miami'],
      'Salary': [50000, 60000, 70000, 55000, 80000],
      'Experience': [3, 5, 7, 4, 9]
  }
  df = pd.DataFrame(data)
  
  # Transform
  df['Bonus'] = df['Salary'] * df['Experience']
  
  # Load
  print(df)
  print("Load query executed")