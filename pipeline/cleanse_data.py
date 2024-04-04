from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import numpy as np
from pyspark.sql.types import *
import ast

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(filename="/Users/pongk/project_data_pipeline/file/cleanse_bd.log",
                    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    filemode='w',
                    level=logging.DEBUG)
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/postgres-jdbc/postgresql-42.7.3.jar") \
    .config('spark.app.name', 'learning_spark_sql')\
    .getOrCreate()


class dataframe():
    def __init__(self,table_name):
        self.table_name = table_name
        """self.table =spark.read \
                    .format("jdbc") \
                    .option("url", "jdbc:postgresql://localhost:5432/data_pipeline") \
                    .option("dbtable", self.table_name) \
                    .option("user", "postgres") \
                    .option("password", "pong1234") \
                    .option("driver", "org.postgresql.Driver") \
                    .load()"""
    def get_table_name(self):
        return self.table_name
    def show(self):
        return self.table.show()
    def query_toshow(self,order):
        self.table.createOrReplaceTempView(self.table_name)
        spark.sql(order).show()
        return spark.sql(order)
    def query_andsave(self,order):
        self.table.createOrReplaceTempView(self.table_name)
        self.table = spark.sql(order)
        """spark.sql(order).show()"""
        return self.table.count()
    def null_check(self):
        null_counts_per_column = []
        for col_name in self.table.columns:
            null_count = self.table.where(col(col_name).isNull()).count()

            null_counts_per_column.append((col_name, null_count))
            
        for col_name, null_count in null_counts_per_column:
            print(f"Total number of null values in column '{col_name}': {null_count}")
    def describe(self):
        self.table.describe().show()
        return self.table.describe()
    def count_len(self):
        return self.table.count()
    def info(self):
        self.table.toPandas().info()
    def isnull(self):
        pan=self.table.toPandas()
        pan= pan[pan.isnull().any(axis=1)]
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        if pan.empty:
            return "0"
        else:
            info_ = spark.createDataFrame(pan)
            info_.show()
            return info_


    def dropna(self):
        self.table.dropna().show()
        return self.table.dropna()
    def topandas(self):
        return self.table.toPandas()
    def query_topandas(self,order):
        self.table.createOrReplaceTempView(self.table_name)
        return spark.sql(order).toPandas()
    def save_database(self,location,name,user,password):
        self.table.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql:"+location) \
            .option("dbtable",name) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
    def save_toparquet(self,locate_name):
        spark.conf.set("mapreduce.fileoutputcommitter.marksuccesfuljobs","false")
        self.table = self.table.repartition(1)
        self.table.coalesce(1).write\
            .format("parquet") \
            .mode("overwrite") \
            .save(f'{locate_name}')
    def dropDuplicates(self):
        self.table = self.table.dropDuplicates()
        return self.table
    def drop_column(self,col):
        self.table= self.table.drop(*col)
    def merge(self,df2,column_1,column_2,HOW):
        merged_df = self.table.join(df2.table,self.table[column_1]==df2.table[column_2], how=HOW)
        merged_df = merged_df.orderBy("uuid")
        self.table = merged_df
        return merged_df
    def Schema(self):
        self.table.printSchema()
    def col(self,col_name):
        self.table.where(col(col_name))
    def change_dtype(self,col1,type):
        self.table = self.table.withColumn(col1, self.table[col1].cast(type))
    def orderby(self,column):
        self.table=self.table.orderBy(column)
    def save_tocsv(self,locate):
        spark.conf.set("mapreduce.fileoutputcommitter.marksuccesfuljobs","false")
        self.table = self.table.repartition(1)
        self.table.write.csv(locate, mode="overwrite")


class dataframe_connect(dataframe):
    def __init__(self,table_name,database):
        super().__init__(table_name)
        self.table =spark.read \
                    .format("jdbc") \
                    .option("url", f"jdbc:postgresql://localhost:5432/{database}") \
                    .option("dbtable", self.table_name) \
                    .option("user", "postgres") \
                    .option("password", "pong1234") \
                    .option("driver", "org.postgresql.Driver") \
                    .load()
        
class dataframe_pandas(dataframe):
    def __init__(self,pandas,name):
        super().__init__(name)
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        self.table = spark.createDataFrame(pandas)
        


def check_info(df):
    print("----------schema----------")
    df.Schema()
    print("number of row :"+str(df.count_len()))
    print("----------info----------")
    df.info()
    print("----------null check----------")
    df.null_check()
    if df.isnull() == "0":
        print("null count:0")
    else:
        print("null count:"+str(df.isnull().count()))


def cleanse_student_table_to_pandas(df):
    import matplotlib as mpl
    mpl.rcParams['figure.facecolor'] = 'white'
    import matplotlib.pyplot as plt
    df.query_andsave(f"SELECT uuid,name,dob,sex,contact_info,job_id,num_course_taken,current_career_path_id,time_spent_hrs,DATEDIFF(year, dob, CURRENT_DATE) AS age, FLOOR(age / 10) * 10 AS age_group FROM {df.get_table_name()}")
    df = df.topandas()
    df["contact_info"] =df["contact_info"].apply(lambda x:ast.literal_eval(x))
    explode_contact = pd.json_normalize(df["contact_info"])
    df = pd.concat([df.drop('contact_info',axis=1),explode_contact],axis=1)
    mail = df['mailing_address'].str.split(',',expand = True)
    df['street'] = None
    df['city'] = None
    df['state'] = None
    df['zip_code'] = None
    df['street'] = mail[0]
    df['city'] = mail[1]
    df['state']= mail[2]
    df['zip_code']= mail[3]
    df = df.drop('mailing_address',axis=1)
    df['current_career_path_id'] = pd.to_numeric(df['current_career_path_id'], errors='coerce')
    df['time_spent_hrs'] = pd.to_numeric(df['time_spent_hrs'], errors='coerce')
    df['job_id'] = pd.to_numeric(df['job_id'], errors='coerce')
    miss_data =  df[df[['num_course_taken']].isnull().any(axis=1)]

    fig, axs = plt.subplots(1, 3, figsize=(15, 7))

    sg = (df.groupby('sex').count()['uuid']/len(df)).rename('complete')
    mg = (miss_data.groupby('sex').count()['uuid']/len(miss_data)).rename('incomplete')
    vs_pandas_1 = pd.concat([sg,mg],axis=1)
    print(vs_pandas_1)
    vs_pandas_1.plot.bar(ax=axs[0])

    sg = (df.groupby('job_id').count()['uuid']/len(df)).rename('complete')
    mg = (miss_data.groupby('job_id').count()['uuid']/len(miss_data)).rename('incomplete')
    vs_pandas_2 = pd.concat([sg,mg],axis=1)
    print(vs_pandas_2)
    vs_pandas_2.plot.bar(ax=axs[1])

    sg = (df.groupby('age_group').count()['uuid']/len(df)).rename('complete')
    mg = (miss_data.groupby('age_group').count()['uuid']/len(miss_data)).rename('incomplete')
    vs_pandas_3 = pd.concat([sg,mg],axis=1)
    print(vs_pandas_3)
    vs_pandas_3.plot.bar(ax=axs[2])

    plt.tight_layout()
    plt.show()
    
    df = df.dropna(subset=['num_course_taken'])
    miss_data_job_id = df[df[['job_id']].isnull().any(axis=1)]
    miss_data = pd.concat([miss_data,miss_data_job_id])
    df = df.dropna(subset=['job_id'])
    

    df['current_career_path_id'] = np.where(df['current_career_path_id'].isnull(),0,df['current_career_path_id'])
    df['time_spent_hrs'] = np.where(df['time_spent_hrs'].isnull(),0,df['time_spent_hrs'])
    df = df.drop_duplicates()
    df = df.sort_values(by='uuid', ascending=True)
    miss_data = miss_data.drop_duplicates()
    miss_data = miss_data.sort_values(by='uuid', ascending=True)

    return (df,miss_data)

def merge(_df_student,_df_path,_df_job):
    try:
        _df_student = _df_student.rename(columns={"job_id": "JOB_ID_1"})
        if _df_student.empty:
            raise ValueError("Input dataframe clean_new_students is empty.")
        if _df_path.empty:
            raise ValueError("Input dataframe clean_career_path is empty.")
        if _df_job.empty:
            raise ValueError("Input dataframe clean_student_jobs is empty.")
        df_student=dataframe_pandas(_df_student,'df1')
        df_career_path = dataframe_pandas(_df_path,'df2')
        df_student_jobs = dataframe_pandas(_df_job,'df3')
        df_student.merge(df_career_path,'current_career_path_id','career_path_id','left')
        df_student.merge(df_student_jobs,'JOB_ID_1','job_id','left')
        try:
            df_student.drop_column(["JOB_ID_1",'career_path_id'])
        except KeyError as e:
            print(f"Column drop error: {e}")    
    except Exception as e:  # Catch general exceptions (broader handling)
        print(f"Unexpected error: {e}")   
    else: 
        return df_student

def cleanse_career_path_topandas(df):
    df = df.topandas()
    not_applicable = {'career_path_id':0,'career_path_name':'not applicable','hours_to_complete':0}
    df.loc[len(df)] = not_applicable
    df =df.drop_duplicates()
    return df

def cleanse_job_to_pandas(df):
    df=df.topandas()
    df =df.drop_duplicates()
    return df

def test_nulls(df):
    df_missing = df[df.isnull().any(axis=1)]
    cnt_missing = len(df_missing)
    try:
        assert cnt_missing == 0 , "there are " + str(cnt_missing)+ "null in the table"
    except AssertionError as ae:
        logger.exception(ae)
    else:
        print("No null row found")

def test_schema(local_df,db_df):
    errors =0
    mismatch_columns = []
    for col in db_df:
        try:
            if local_df[col].dtype != db_df[col].dtypes:
                errors +=1
                mismatch_columns.append(col)
                logger.error(f"Data type mismatch for column '{col}': Local dtype - {local_df[col].dtype}, DB dtype - {db_df[col].dtype}")
        except NameError as ne:
            logger.exception(ne)
            raise ne 
    errors = 1
    if errors > 0 :
        assert_err_msg = str(errors) + " column(s) dtype aren't the same"
        logger.exception(assert_err_msg)
    assert errors == 0, assert_err_msg

def test_num_cols(local_df,db_df):
    try:
        assert len(local_df.columns) == len(db_df.columns)
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print("Number of columns are same")

def test_for_path_id(student,career_paths):
    studebt_table = student.current_career_path_id.unique()
    is_subset = np.isin(studebt_table,career_paths.career_path_id.unique())
    missing_id = studebt_table[~is_subset]
    try:
        assert len(missing_id) == 0 , "Missing career_path_id(s)"+str(list(missing_id))+"in 'career_paths' table" 
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print("all career_path_ids are present.")

def test_for_job_id(student,student_jobs):
    student_table = student.job_id.unique()
    is_subset = np.isin(student_table, student_jobs.job_id.unique())
    missing_id = student_table[~is_subset]
    try:
        assert len(missing_id) == 0, "Missing job_id(s):" +str(list(missing_id))+" in 'student_jobs' table"
    except AssertionError as ae:
        logger.exception (ae)
        raise ae
    else:
        print("All job_ids are present.")

def main(locate_update,locate_clean):
    logger.info("Start Log")
    with open('C:\\Users\\pongk\\project_data_pipeline\\file\\changelog.md','r') as f:
        lines = f.readlines()
    if len(lines) == 0:
        next_ver = 0
    else:
        next_ver = int(lines[0].replace("## 0.0.", ""))+1
    table_student = dataframe_connect("cademycode_students",locate_update)
    table_career_path = dataframe_connect("cademycode_courses",locate_update)
    table_student_jobs = dataframe_connect("cademycode_student_jobs",locate_update)
    pd_student = table_student.topandas()
    
    try:
        clean_db = dataframe_connect("cleanse_data",locate_clean)
        pd_clean_db = clean_db.topandas()
        missing_db = dataframe_connect("missing_data",locate_clean)
        pd_missing = missing_db.topandas()
        new_student = pd_student[~np.isin(pd_student.uuid.unique(),pd_clean_db.uuid.unique())]
    except:
        new_student = pd_student
        clean_db =[]
        
    print(new_student)
    clean_new_students, missing_data = cleanse_student_table_to_pandas(dataframe_pandas(new_student,'new_student'))
    
    try:
        new_missing_data = missing_data[~np.isin(missing_data.uuid.unique(),pd_missing.uuid.unique())]
    except:
        new_missing_data = missing_data


    if len(new_missing_data)>0:
        incomplete = dataframe_pandas(new_missing_data,'incomplete')
        incomplete.save_database(locate_update,"missing_data","postgres","pong1234")
        incomplete.save_toparquet("/Users/pongk/project_data_pipeline/file/miss_data.parquet")
        incomplete.save_tocsv("/Users/pongk/project_data_pipeline/file/miss_data.csv")
    if len(clean_new_students)>0:
        clean_career_path = cleanse_career_path_topandas(table_career_path)
        clean_student_jobs = cleanse_job_to_pandas(table_student_jobs)
        test_for_job_id(clean_new_students,clean_student_jobs)
        test_for_path_id(clean_new_students,clean_career_path)
        df_clean = merge(clean_new_students,clean_career_path,clean_student_jobs)
        if len(clean_db)>0:
            test_num_cols(df_clean.topandas(),pd_clean_db)
            test_schema(df_clean.topandas(),pd_clean_db)
        
        test_nulls(df_clean.topandas())

        df_clean.save_database(locate_update,"cleanse_data","postgres","pong1234")
        clean_db = dataframe_connect("cleanse_data",locate_update)
        clean_db.save_toparquet("/Users/pongk/project_data_pipeline/file/final_df.parquet")
        clean_db.save_tocsv("/Users/pongk/project_data_pipeline/file/final_df.csv")
        new_lines =[
            '## 0.0.'+str(next_ver)+'\n'+
            '### Added\n' +
            '_ '+ str(len(df_clean.topandas()))+'more data to database of raw data \n'+
            '_ '+ str(len(new_missing_data))+ 'new missing data to incomplete_data table\n'+
            '\n'
        ]
        w_lines =''.join(new_lines + lines)
        with open('/Users/pongk/project_data_pipeline/file/changelog.md','w') as f:
            for line in w_lines:
                f.write(line)
    else:
        print('no new data')
        logger.info('no new data')
    logger.info("END Log")
    spark.stop()

if __name__ == "__main__":
    main("data_pipe_line_update","data_pipeline")
    