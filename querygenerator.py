import pandas as pd

excel_file_path = 'C:\\Users\\janid\\OneDrive\\Documents\\Uni\\CCCU 23 24\\Advanced OS\\Assignment 2\\Dataset\\Movies_Data2.xlsx'
excel_data = pd.ExcelFile(excel_file_path)
sheet_names = excel_data.sheet_names

for sheet_name in sheet_names:
    df = excel_data.parse(sheet_name)
    
    insert_queries = []
    for _, row in df.iterrows():
        values = ', '.join([f"""'{value.replace("'", "''")}'""" if isinstance(value, str) and value != 'nan' else 'NULL' if pd.isna(value) else str(value) for value in row.values])
        insert_queries.append(f"({values})")
    
    columns = ', '.join(df.columns)
    values_bulk = ', '.join(insert_queries)
    bulk_insert_query = f"INSERT INTO {sheet_name.replace('Table','')} ({columns}) VALUES {values_bulk}"
    
    file_name = f"{sheet_name}_insert_query.sql"
    with open(file_name, 'w') as file:
        file.write(bulk_insert_query)

    print(f"Generated INSERT query for {sheet_name} written to {file_name}")