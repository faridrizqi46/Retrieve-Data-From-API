import os
import pandas as pd
import psycopg2 as ps
import tokenkey as tky

crdir = os.getcwd()+'\WeatherAPI'

def read_csv(file):
    try:
        df= pd.read_csv(crdir+ "\\" +file)
    except UnicodeDecodeError:
        df= pd.read_csv(crdir+ "\\" +file, encoding="ISO-8859-1") #if utf-8 encoding error

    return df

def clean_colname(dataframe):
  
    #force column names to be lower case, no spaces, no dashes
    dataframe.columns = [x.lower().replace(".","_") for x in dataframe.columns]
    
    #processing data
    replacements = {
        'timedelta64[ns]': 'varchar',
        'object': 'varchar',
        'float64': 'float',
        'int64': 'int',
        'datetime64': 'timestamp'
    }

    col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(dataframe.columns, dataframe.dtypes.replace(replacements)))
    
    return col_str, dataframe.columns

def upload_to_db(host_name, dbname, username, password, port, df, col_str, file='test.csv', tbl_name='weather'):
    conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
    cursor = conn.cursor()
    print('opened database successfully')
    
    #create table
    cursor.execute("create table %s (%s);" % (tbl_name, col_str))
    print('table was created successfully') 
    
    #insert values to table

    #save df to csv
    df_columns=df.columns
    df.to_csv(crdir+"\\"+file, header=df_columns, index=False, encoding='utf-8')
    
    #open the csv file, save it as an object
    my_file = open(crdir+"\\"+file)
    print('file opened in memory')
    
    #upload to db
    SQL_STATEMENT = """
    COPY %s FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    """

    cursor.copy_expert(sql=SQL_STATEMENT % tbl_name, file=my_file)
    print('file copied to db')

    cursor.execute("grant select on table %s to public" % tbl_name)
    conn.commit()
    cursor.close()
    print('imported table to db completed')

df = read_csv('TangerangWeatherHistory.csv')
col_str, df.columns = clean_colname(df)
upload_to_db(tky.HOST_NAME, tky.DBNAME, tky.USERNAME, tky.PASSWORD, tky.PORT, df, col_str)




