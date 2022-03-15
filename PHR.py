import argparse
import json
import requests
from urllib.parse import urljoin
import pyspark
import datetime
import numpy as np
import pandas as pd
import time
from pyspark.sql.types import StructType, StructField, StringType , IntegerType, DoubleType,ByteType,ArrayType,DateType,FloatType
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql import Row
from pyspark import SparkContext, SQLContext
import scipy.stats

spark = SparkSession\
    .builder\
    .appName("QC_Scheduler_Task_Create")\
    .enableHiveSupport()\
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
spark.conf.set("spark.sql.crossJoin.enabled", "true")



def impute_QM_RESULT_ETL(df):

    '''
    This function is used to impute the values of the column QM_RESULT_ETL
    Input  : df -> Input DataFrame
    Returns : df -> Dataframe with imputed value of QM_RESULT_ETL column

    '''
    if df[(df["QM_RESULT_ETL"].isnull()) & (df["UD_DESCRIPTION"])=="Rejected"].shape[0] > 0:

        df["QM_RESULT_ETL"] =np.where(((df["QM_RESULT_ETL"].isnull()) & (df["UD_DESCRIPTION"]=="Rejected")),
                                    df["QM_RESULT"],
                                    df["QM_RESULT_ETL"])


    df["QM_TEST_DESC"].replace(["Uniformoty","[\r\n]"],["Uniformity",""],inplace=True)
    df["QM_TEST_DESC"] = df['QM_TEST_DESC'].str.lower()
    df["TEST_TYPE"]    = df['TEST_TYPE'].str.lower()

    return df

def create_df_uod(df,str1,strlist,strlist2):
    '''
    This function filters the data for tests = uniformity of dosage
    Input 1 : df -> Entire data for the plant
    Input 2 : str1 -> value to be checked in  QM_TEST_DESC
    Input 3 : strlist -> list of values to be checked in TEST_TYPE
    Input 4 : strlist2 -> list of values to be checked in  QM_TEST_DESC
    Returns : dfuod -> Dataframe for uniformity of dosage tests

    '''

    dfuod = df[df['QM_TEST_DESC'].str.contains(str1)]


    for i in strlist:

        ind = dfuod[dfuod['TEST_TYPE'].str.contains(i)].index

        if len(ind)>0:
            dfuod = dfuod.drop(ind)

    for i in strlist2:

        ind = dfuod[dfuod['QM_TEST_DESC'].str.contains(i)].index

        if len(ind)>0:

            dfuod = dfuod.drop(ind)

    return dfuod

def create_df_disso(df,str1,strlist):

    '''
    This function filters the data for tests = dissolution
    Input 1 : df -> Entire data for the plant
    Input 2 : str1 -> value to be checked in  QM_TEST_DESC
    Input 3 : strlist -> list of values to be checked in QM_TEST_DESC
    Returns : dfuod -> Dataframe for dissolution tests

    '''


    dfdisso = df[df['QM_TEST_DESC'].str.contains(str1)]


    for i in strlist:

        ind = dfdisso[dfdisso['QM_TEST_DESC'].str.contains(i)].index

        if len(ind)>0:
            dfdisso = dfdisso.drop(ind)

    return dfdisso

def create_df_cu(df,str1,strlist):

    '''
    This function filters the data for tests = content uniformity
    Input 1 : df -> Entire data for the plant
    Input 2 : str1 -> value to be checked in  QM_TEST_DESC
    Input 3 : strlist -> list of values to be checked in QM_TEST_DESC
    Returns : dfuod -> Dataframe for content uniformity tests

    '''

    dfcu = df[df['QM_TEST_DESC'].str.contains(str1)]

    for i in strlist:

        ind = dfcu[dfcu['QM_TEST_DESC'].str.contains(i)].index

        if len(ind)>0:
            dfcu = dfcu.drop(ind)

    return dfcu

def create_df_bu(df,str1,strlist):

    '''
    This function filters the data for tests = blend uniformity
    Input 1 : df -> Entire data for the plant
    Input 2 : str1 -> value to be checked in  QM_TEST_DESC
    Input 3 : strlist -> list of values to be checked in QM_TEST_DESC
    Returns : dfuod -> Dataframe for blend uniformity tests

    '''

    dfbu = df[df['QM_TEST_DESC'].str.contains(str1)]

    for i in strlist:

        ind = dfbu[dfbu['QM_TEST_DESC'].str.contains(i)].index

        if len(ind)>0:

            dfbu = dfbu.drop(ind)

    return dfbu

def create_df_stat(df,str1,strlist):

    '''
    This function filters the data for tests = statistical
    Input 1 : df -> Entire data for the plant
    Input 2 : str1 -> value to be checked in  QM_TEST_DESC
    Input 3 : strlist -> list of values to be checked in QM_TEST_DESC
    Returns : dfuod -> Dataframe for statistical tests

    '''

    dfstat = df[df['TEST_TYPE'].str.contains(str1)]


    for i in strlist:

        ind = dfstat[dfstat['QM_TEST_DESC'].str.contains(i)].index

        if len(ind)>0:

            dfstat = dfstat.drop(ind)

    return dfstat

def concat_data(dfuod,dfcu,dfbu,dfstat,dfdisso,df):

    '''
    This function concats the raw data with the filtered value of specific tests
    Input 1 : dfuod -> Dataframe for uniformity of dosage tests
    Input 2 : dfcu -> Dataframe for content uniformity tests
    Input 3 : dfbu -> Dataframe for blend uniformity tests
    Input 4 : dfstat -> Dataframe for statistical tests
    Input 5 : dfdisso -> Dataframe for dissolution tests
    Input 6 : df -> list of values to be checked in QM_TEST_DESC
    Returns : final_df -> Concatenated data of all the five tests and rest of the raw data.

    '''

    l1 = ["blend uniformity","uniformity of dosage","content uniformity","statistical","disso"]

    for i in l1:
        if i == "statistical":
            #print(df[df['TEST_TYPE'].str.contains(i)].index)
            ind = df[df['TEST_TYPE'].str.contains(i)].index
        else:
            ind = df[df['QM_TEST_DESC'].str.contains(i)].index
            #print(df[df['QM_TEST_DESC'].str.contains(i)].index)

        df = df.drop(ind)

    final_df = pd.concat([df,dfuod,dfdisso,dfcu,dfbu,dfstat])
    return final_df

def final_data_clean(final_df):

    '''
    This function is used to filter out tests based on the keywords and strings
    Input 1 : final_df -> Concatenated data of all the five tests and rest of the raw data.
    Returns : final_df -> Returns the cleaned dataframe based on the filters

    '''

    final_df = final_df[~final_df.QM_TEST_DESC.str.contains("unit")]
    final_df = final_df[~final_df.QM_TEST_DESC.str.contains("part")]
    final_df = final_df[~final_df.QM_TEST_DESC.str.contains("average assay")]
    final_df = final_df[~final_df.QM_TEST_DESC.str.contains("average asaay")]
    final_df = final_df[~final_df.QM_TEST_DESC.str.contains("determination-")]
    final_df = final_df[~final_df.QM_TEST_DESC.str.contains("= x =")]
    final_df = final_df[~final_df.QM_TEST_DESC.str.contains("x > 101.5")]
    final_df = final_df[~final_df.QM_TEST_DESC.str.contains("x < 98.5")]
    final_df = final_df[~final_df.QM_TEST_DESC.str.contains("trial 1")]
    final_df = final_df[~final_df.QM_TEST_DESC.str.contains("trial 2")]
    final_df = final_df[~final_df.QM_TEST_DESC.str.contains("maximum")]
    final_df = final_df[~final_df.QM_TEST_DESC.str.contains("minimum")]
    final_df = final_df[~final_df.QM_TEST_DESC.str.contains("prep")]

    final_df["TEST_DATE"] = pd.to_datetime(final_df["TEST_DATE"])

    return final_df

def remove_result_etl(final_df):

    '''
    This function is used to drop certain records based on QM_RESULT_ETL
    Input 1 : final_df -> Cleand and processed Data
    Returns : final_df -> Returns the cleaned dataframe based on the filters

    '''

    final_df['result_etl'] = final_df["QM_RESULT_ETL"].str.lower()

    ind_rmv = final_df[final_df.result_etl.str.contains("complies") |
             final_df.result_etl.str.contains("avg") |
             final_df.result_etl.str.contains("bpql") |
             final_df.result_etl.str.contains("less")].index.values.tolist()

    final_df.drop(ind_rmv,inplace = True)

    final_df.reset_index(drop=True)

    return final_df

def result_etl_sub(final_df):

    '''
    This function is used to substitute the non numerics values in QM_RESULT_ETL column by "LOQ" column
    Input 1 : final_df -> Cleand and processed Data
    Returns : final_df -> Data with numeric values in QM_RESULT_ETL column

    '''

    #collecting the index where QM_RESULT_ETL has these values
    ind = final_df[final_df.result_etl.str.contains("bdl") |
             final_df.result_etl.str.contains("lod") |
             final_df.result_etl.str.contains("loq") |
             final_df.result_etl.str.contains("<") |
             final_df.result_etl.str.contains("not detected") |
             final_df.result_etl.str.contains("below") |
             final_df.result_etl.str.contains("min")].index.values.tolist()

    #collecting the values from the LOQ columns
    val = final_df.loc[ind].LOQ.values.tolist()

    # replacing QM_RESULT_ETL with collected values from LOD
    final_df.loc[ind,'QM_RESULT_ETL'] = val

    return final_df

def create_master_table(final_df):

    '''
    This function is used to create a master data at PLANT,PRODUCT,SPEC,TEST Level.
    Input 1 : final_df -> Cleand and processed Data
    Returns : joined_df -> Aggregated data with metrics to calculate  ppk values at PLANT,PRODUCT,SPEC,TEST Level.

    '''


    #identifying the groups with multiple NMT_ETL and NLT_ETL
    grps = final_df.groupby(["PRODUCT_CODE","SPEC_NO","QM_TEST_DESC"]).agg({"NMT_ETL":["nunique","first",],
                                                                            "NLT_ETL":["nunique","first"],
                                                                            })

    grps = grps.reset_index()
    grps = grps[(grps["NMT_ETL"]["nunique"]>1) | (grps["NLT_ETL"]["nunique"]>1)]
    grps["multiple_entry_flag"] = 1
    cols = [i[0] for i in grps.columns]
    grps.columns = cols

    grps = grps[['PRODUCT_CODE', 'SPEC_NO', 'QM_TEST_DESC','multiple_entry_flag']]


    # Joining it back to the main data
    joined_df = pd.merge(final_df, grps, on=['PRODUCT_CODE', 'SPEC_NO','QM_TEST_DESC'],how='left')


    #Modifiying the test description as per the NLT_NMT combination
    joined_df["NLT_ETL"] = joined_df["NLT_ETL"].fillna("NULL").astype('str')
    joined_df["NMT_ETL"] = joined_df["NMT_ETL"].fillna("NULL").astype('str')

    #joined_df.dtypes
    joined_df["NLT_NMT"] = joined_df["NLT_ETL"].str.cat(joined_df["NMT_ETL"],sep="_")
    joined_df["NLT_NMT"] = joined_df["NLT_NMT"].fillna("NULL").astype('str')

    joined_df["QM_TEST_DESC_NEW"] = joined_df["QM_TEST_DESC"].str.cat(joined_df["NLT_NMT"],sep=" - ")
    joined_df.loc[joined_df[joined_df["multiple_entry_flag"]!=1].index,'QM_TEST_DESC_NEW'] = joined_df[joined_df["multiple_entry_flag"]!=1]['QM_TEST_DESC']



    print("----------------------------------------------------")
    #print(joined_df.dtypes)

    #restoring NULL to NaN
    # try:
    joined_df["NLT_ETL"] = joined_df["NLT_ETL"].replace("NULL",np.nan).astype('float')
    joined_df["NMT_ETL"] = joined_df["NMT_ETL"].replace("NULL",np.nan).astype('float')

    # except:

    #     print(joined_df["NLT_ETL"])
    #     print(joined_df["NMT_ETL"])



    joined_df = joined_df[['PLANT_CODE','PLANT_NAME', 'PRODUCT_CODE','PRODUCT_NAME','BATCH_NO',
                          'SPEC_NO','QM_TEST_DESC','QM_TEST_DESC_NEW','QM_RESULT_ETL_NEW',
                          'Date_Period','TEST_DATE', 'QM_RESULT_ETL', 'LOD','LOQ', 'NLT_ETL', 'NMT_ETL']]

    return joined_df

def filter_data(df):
    '''
    This function is used to drop values where NMT_ETL and NLT_ETL is null
    Input 1 : df -> Cleand and processed Data
    Returns : df -> Data with notnull combination of NMT_ETL and NLT_ETL

    '''


    ind = df[(df["NMT_ETL"].isnull()) & (df["NLT_ETL"].isnull())].index

    if len(ind)>0:
        print("Dropping rows where both NMT_ETL and NLT_ETL are null")
        df = df.drop(ind)

    return df

def qtl(x):

    '''
    This function is used as helper function to groupby for calculating percentiles

    '''

    return x.quantile(0.00135)

def qtu(x):
    '''
    This function is used as helper function to groupby for calculating percentiles

    '''
    return x.quantile(0.99865)

def create_summary_df(comb_df):
    '''
    This function is used to prepare the master data with some metrics which be used to calculate the ppk values
    Input 1 : comb_df -> Master Data
    Returns : mdf -> Final data to be used as detailed table for ppk calculation

    '''

    mdf = comb_df.groupby(['PLANT_CODE',
                           'PRODUCT_CODE',
                           'SPEC_NO',
                           'QM_TEST_DESC_NEW',
                           'Date_Period']).agg({'NMT_ETL':'first',
                                                'NLT_ETL':'first',
                                                "BATCH_NO":['nunique','count'],
                                                'QM_RESULT_ETL_NEW': ['median',qtl, qtu]}).reset_index()

    mdf.columns = ["PLANT_CODE","PRODUCT_CODE","SPEC_NO","QM_TEST_DESC",'Date_Period',
                   "USL","LSL","BATCH_COUNT","Total Rows","median","qtl","qtu"]

    mdf["dmedtolow"] = mdf["median"] - mdf["qtl"]

    mdf["dmedtoupp"] = mdf["qtu"] - mdf["median"]

    return mdf

def create_ppk(mdf):

    '''
    This function is used to calculate the ppk values
    Input 1 : mdf -> Final data to be used as detailed table for ppk calculation
    Returns : ppk_df -> Detailed Table for PPK

    '''


    #CASE 1. Both NULL - Already filtered out in filter_data()


    #CASE 2. Both NOT NULL
    df2 = mdf[(mdf["USL"].notnull()) & (mdf["LSL"].notnull())]

    df2["PPK_UP"] = (df2["USL"]-df2["median"]).divide(df2["dmedtoupp"])

    df2["PPK_DOWN"] = (df2["median"]-df2["LSL"]).divide(df2["dmedtolow"])

    df2['PPK'] = df2[['PPK_UP','PPK_DOWN']].min(axis=1)


    #CASE 3 When USL is not null
    df3 = mdf[(mdf["USL"].notnull()) & (mdf["LSL"].isnull())]

    df3["PPK"] = (df3["USL"]-df3["median"]).divide(df3["dmedtoupp"])

    #CASE 4 When LSL is not null
    df4 = mdf[(mdf["USL"].isnull()) & (mdf["LSL"].notnull())]

    df4["PPK"] = (df4["median"]-df4["LSL"]).divide(df4["dmedtolow"])


    ppk_df = pd.concat([df2,df3,df4])

    ppk_df["LB"] = scipy.stats.chi2.ppf(0.05, (ppk_df["Total Rows"]-1))/(ppk_df["Total Rows"]-1)#Total no. of batches

    ppk_df["ppk_final"] = np.sqrt(ppk_df["LB"])*ppk_df["PPK"]


    return ppk_df

def create_date_cols(df):
    '''
    This function is used to create time related cols
    Input 1 : df -> Raw data
    Returns : df -> Raw data with new cols

    '''
    df["TEST_DATE"] = pd.to_datetime(df["TEST_DATE"])
    df["Year"] = df["TEST_DATE"].dt.year
    df["Month"] = df["TEST_DATE"].dt.month
    df["Day"] = df["TEST_DATE"].dt.day

    return df

def create_date_cuts():
    '''
    This function is used to create various date cuts
    Returns : date_cut -> Dictionary containing various key value pairs
    '''
    FYs = [2019,2020,2021,2022]
    #FYs = [2021]
    date_cut = {}

    for FY in FYs:

        #print(FY)

        date_cut["FY"+str(FY)] = [pd.Timestamp(FY-1, 4, 1),pd.Timestamp(FY, 4, 1)]
        date_cut["Q1"+str(FY)] = [pd.Timestamp(FY-1, 4, 1),pd.Timestamp(FY-1, 7, 1)]
        date_cut["Q2"+str(FY)] = [pd.Timestamp(FY-1, 7, 1),pd.Timestamp(FY-1, 10, 1)]
        date_cut["Q3"+str(FY)] = [pd.Timestamp(FY-1, 10, 1),pd.Timestamp(FY, 1, 1)]
        date_cut["Q4"+str(FY)] = [pd.Timestamp(FY, 1, 1),pd.Timestamp(FY, 4, 1)]
        date_cut["HY1"+str(FY)] = [pd.Timestamp(FY-1, 4, 1),pd.Timestamp(FY-1, 10, 1)]
        date_cut["HY2"+str(FY)] = [pd.Timestamp(FY-1, 10, 1),pd.Timestamp(FY, 4, 1)]
        date_cut["Last 3 months"] = [pd.Timestamp.now() - pd.DateOffset(months=3),pd.Timestamp.now()]


    return date_cut

def slice_date(data,date_cut,start,end,cat):
    '''
    This function is used to slice the data based on dates
    Input 1 : data -> Raw data
    Input 2 : date_cut -> Time Period
    Input 3 : start -> Start Date of the Time Period
    Input 4 : end -> End Date of the Time Period
    Returns : df -> Sliced data based on date

    '''
    print("Date Cut for {} ".format(cat))
    df = data[(data["TEST_DATE"] >= start) & (data["TEST_DATE"] < end)].sort_values(by="TEST_DATE")
    df["Date_Period"] = date_cut
    print("Total rows ",df.shape)

    return df

def generate_table(df):
    '''
    This function is used to trigger all the functions to calculate PPK
    Input 1 : df -> Raw Data
    Returns : ppk_df -> Detail table for ppk
    '''
    df = impute_QM_RESULT_ETL(df)

    dfuod = create_df_uod(df,"uniformity of dosage",["qual","absorbance"],["max","min"])
    dfdisso = create_df_disso(df,"disso",["absorbance"])
    dfcu = create_df_cu(df,"content uniformity",["mum"])
    dfbu = create_df_bu(df,"blend uniformity",["unit"])
    dfstat = create_df_stat(df,"statistical",["relative"])

    final_df = concat_data(dfuod,dfcu,dfbu,dfstat,dfdisso,df)

    final_df = final_data_clean(final_df)
    final_df = remove_result_etl(final_df)
    final_df = result_etl_sub(final_df)

    #Dropping rows where QM_RESULT_ETL does not contain numeric value
    final_df["QM_RESULT_ETL_NEW"] = pd.to_numeric(final_df["QM_RESULT_ETL"], errors='coerce')
    final_df = final_df[final_df.QM_RESULT_ETL_NEW.notnull()]

    final_df = filter_data(final_df)

    #dropping all the duplicates
    final_df = final_df.drop_duplicates()

    joined_df = create_master_table(final_df)


    mdf = create_summary_df(joined_df)
    ppk_df = create_ppk(mdf)

    ppk_df["VALUE2"] = np.clip(ppk_df["ppk_final"], 0, 2)
    ppk_df.loc[ppk_df["VALUE2"].isnull(),"COLOR"] = "RED"
    ppk_df["COLOR"] =  np.where(ppk_df["ppk_final"]>1.33,"GREEN",np.where(ppk_df["ppk_final"]<1,"RED","YELLOW"))

    ppk_df["METRIC"] = "PPK"

    ppk_df = ppk_df[['PLANT_CODE','PRODUCT_CODE','SPEC_NO','Date_Period','BATCH_COUNT','METRIC','QM_TEST_DESC',"VALUE2","COLOR"]]

    ppk_df.columns = ["PLANT_CODE","PRODUCT_CODE","STAGING_CODE_DESC/SPEC_NO",
                        "DATE_CUT","BATCH_ID/BATCH_COUNT","METRIC","NOTIFICATION/TEST",
                        "VALUE2","COLOR"]

    return ppk_df
    #return final_df

def prepare_data(df):
    '''
    This function is used to create some new columns and impute some columns which will be used in calcucalting QMS Metric
    Input 1 : df -> Raw Data
    Returns : df -> Imputed data with new columns to be used for QMS
    '''

    #df.columns = cols = [i[9:] for i in df.columns]
    print(df.columns)
    df['CAUSE_CODE_DESC'].fillna("NA",inplace=True)
    df['DEFECT_TYPE_CODE_DESC'].fillna("NA",inplace=True)
    df['CAUSE_GROUP_DESC'].fillna("NA",inplace=True)
    df['DEFECT_LOCATION_TEXT'].fillna("NA",inplace=True)


    df["Metric"] =  df["NOTIFICATION_TYPE"] + "_" + df["NOTIFICATION_SUB_CATEGORY"] + "_" + df["STAGING_CODE_DESC"]
    df["Value2"] = df.CAUSE_CODE_DESC + "_" + df.DEFECT_TYPE_CODE_DESC + "_" + df.CAUSE_GROUP_DESC + "_" + df.DEFECT_LOCATION_TEXT
    df["STAGING_CODE_DESC/SPEC_NO"] = np.nan

    return df

def f1(df):
    '''
    This function is used to find data for products having market complaints
    Input 1 : df -> Raw Data
    Returns : df_f1 -> Data for products where we have Market Complaints
    '''
    df_f1 = df[(df.NOTIFICATION_TYPE=="F1") & (df.NOTIFICATION_CATEGORY=="Market Complaints") & (df.NOTIFICATION_SUB_CATEGORY=="Substantiated")]

    df_f1 = df_f1[['PLANT_CODE', 'MATERIAL_CODE', 'STAGING_CODE_DESC/SPEC_NO', 'Date_Period',
                   'BATCH_ID','Metric','NOTIFICATION_NUMBER','Value2',]]

    df_f1['Color'] = "RED"

    return df_f1

def OOS(df):
    '''
    This function is used to find data for products having notification as OOS
    Input 1 : df -> Raw Data
    Returns : df_OOS -> Data for products having notification as OOS
    '''
    df_OOS = pd.concat([df[(df.NOTIFICATION_TYPE=="F2") &
                           (df.NOTIFICATION_CATEGORY.str.contains("OOS"))],df[df.NOTIFICATION_TYPE=="OOS"]])

    print(df_OOS.NOTIFICATION_SUB_CATEGORY.nunique())



    df_OOS = df_OOS[df_OOS.NOTIFICATION_SUB_CATEGORY.str.contains("Valid",na=False)]
    df_OOS = df_OOS[['PLANT_CODE', 'MATERIAL_CODE', 'STAGING_CODE_DESC/SPEC_NO', 'Date_Period',
                   'BATCH_ID','Metric','NOTIFICATION_NUMBER','Value2',]]

    df_OOS['Color'] = "RED"

    return df_OOS

def OOT(df):
    '''
    This function is used to find data for products having notification as OOT
    Input 1 : df -> Raw Data
    Returns : df_OOT -> Data for products having notification as OOT
    '''

    df_OOT = pd.concat([df[(df.NOTIFICATION_TYPE=="OT") & (df.NOTIFICATION_CATEGORY.str.contains("OOT"))],df[df.NOTIFICATION_TYPE=="OOT"]])

    df_OOT = df_OOT[['PLANT_CODE', 'MATERIAL_CODE', 'STAGING_CODE_DESC/SPEC_NO', 'Date_Period',
                   'BATCH_ID','Metric','NOTIFICATION_NUMBER','Value2',]]

    df_OOT['Color'] = "YELLOW"

    return df_OOT

def generate_qms(df):
    '''
    This function is used to trigger all the functions to calculate QMS Metrics
    Input 1 : df -> Raw Data
    Returns : qms_df -> Detail table for QMS Metrics
    '''
    df_f1 = f1(df)
    df_OOS = OOS(df)
    df_OOT = OOT(df)

    qms_df = pd.concat([df_f1,df_OOS,df_OOT])
    qms_df.columns = ["PLANT_CODE","PRODUCT_CODE","STAGING_CODE_DESC/SPEC_NO",
                    "DATE_CUT","BATCH_ID/BATCH_COUNT","METRIC","NOTIFICATION/TEST",
                    "VALUE2","COLOR"]

    return qms_df

#Gettiing the data where batch counts is 30+
def batch_cut(data,n):

    if n!="All":

        ppk_batch = data.groupby(["PLANT_CODE","PRODUCT_CODE"])["BATCH_NO"].nunique().reset_index()
        ppk_batch = ppk_batch[ppk_batch["BATCH_NO"]>=n]
        ppk_batch = pd.merge(ppk_batch[["PLANT_CODE","PRODUCT_CODE"]],data, on = ["PLANT_CODE","PRODUCT_CODE"], how = 'inner')

        groups = pd.DataFrame(ppk_batch.groupby(["PLANT_CODE",
                                                   "PRODUCT_CODE",
                                                   "BATCH_NO"])["TEST_DATE"].max()).reset_index()

        grps = groups.groupby(["PLANT_CODE","PRODUCT_CODE"])

        grouped = pd.DataFrame()
        for name,grp in grps:

            grouped = pd.concat([grouped,grp.sort_values(by = "TEST_DATE",ascending=False).head(n)])


        ppk_batch = pd.merge(grouped[["PLANT_CODE",
                                         "PRODUCT_CODE",
                                         "BATCH_NO"]],ppk_batch, on = ["PLANT_CODE","PRODUCT_CODE","BATCH_NO"], how = 'inner')


        ppk_batch["Date_Period"] = "TOP "+str(n)
    else:
        ppk_batch = data
        ppk_batch["Date_Period"] = "ALL"



    return ppk_batch

def batch_cut_qms(data,n):
    print(n)
    if n!="All":


        print(data.columns)

        qms_batch = data.groupby(['PLANT_CODE', 'MATERIAL_CODE'])["BATCH_ID"].nunique().reset_index()
        qms_batch = qms_batch[qms_batch["BATCH_ID"]>=n]
        qms_batch = pd.merge(qms_batch[['PLANT_CODE', 'MATERIAL_CODE']],data,
                             on = ['PLANT_CODE', 'MATERIAL_CODE'],
                             how = 'inner')

        groups = pd.DataFrame(qms_batch.groupby(['PLANT_CODE', 'MATERIAL_CODE',"BATCH_ID"])["TEST_DATE"].max()).reset_index()

        grps = groups.groupby(['PLANT_CODE', 'MATERIAL_CODE',])

        grouped = pd.DataFrame()
        for name,grp in grps:

            grouped = pd.concat([grouped,grp.sort_values(by = "TEST_DATE",ascending=False).head(n)])


        qms_batch = pd.merge(grouped[['PLANT_CODE', 'MATERIAL_CODE',
                                         "BATCH_ID"]],qms_batch, on = ['PLANT_CODE', 'MATERIAL_CODE',
                                                                       "BATCH_ID"], how = 'inner')

        qms_batch["Date_Period"] = "TOP "+str(n)

    else:
        print("ALL")
        qms_batch = data
        qms_batch["Date_Period"] = "ALL"


    return qms_batch

def date_wise_data(date_cut,data,data_qms):

    ppk_df = pd.DataFrame()
    qms_df = pd.DataFrame()


    for key in date_cut.keys():

        start = date_cut[key][0]
        end = date_cut[key][1]
        print(key," Start ---> ",start," End ---> ",end)

        df_ppk = slice_date(data,key,start,end,"PPK")
        df_qms = slice_date(data_qms,key,start,end,"QMS")

        if df_ppk.shape[0]>0:

            ppk_df_ = generate_table(df_ppk)
            ppk_df = pd.concat([ppk_df,ppk_df_])

        if df_qms.shape[0]>0:

            qms_df_ = generate_qms(df_qms)
            qms_df = pd.concat([qms_df,qms_df_])

    return ppk_df,qms_df

def batch_wise_data(data,data_qms):

    batch_cut_list = [30,10,"All"]

    ppk_df_batch_cut = pd.DataFrame()

    for cuts in batch_cut_list:

        df = batch_cut(data,cuts)

        ppk_df_ = generate_table(df)
        ppk_df_batch_cut = pd.concat([ppk_df_,ppk_df_batch_cut])


    qms_df_batch_cut = pd.DataFrame()

    for cuts in batch_cut_list:

        df = batch_cut_qms(data_qms,cuts)
        break

        qms_df_ = generate_qms(df)
        qms_df_batch_cut = pd.concat([qms_df_,qms_df_batch_cut])

    return ppk_df_batch_cut,qms_df_batch_cut

def prepare_detail_table(ppk_df,qms_df,ppk_df_batch_cut,qms_df_batch_cut):

    detail_table = pd.concat([qms_df,
                              ppk_df,
                              ppk_df_batch_cut,
                              qms_df_batch_cut]).sort_values(by=["DATE_CUT","PLANT_CODE","PRODUCT_CODE","STAGING_CODE_DESC/SPEC_NO"])


    map_dict = {}
    for i in detail_table.DATE_CUT.value_counts().index:
        if "Last 3 months" in i:
            map_dict[i] = "Last 3 months"
        elif "Q" in i:
            map_dict[i] = "Quarterly"
        elif "FY" in i:
            map_dict[i] = "Yearly"
        elif "HY" in i:
            map_dict[i] = "Half Yearly"
        else:
            map_dict[i] = "Batchwise"

    Plant_Name = data.PLANT_NAME[0]
    detail_table["Plant Name"] = Plant_Name
    detail_table["Category Type"] = detail_table["DATE_CUT"].map(map_dict)
    detail_table.rename(columns = {'VALUE2':'Description',
                                   'PRODUCT_CODE':'Material Code',
                                   "BATCH_ID/BATCH_COUNT":"Batch No.",
                                   'METRIC':'Metric',
                                   "COLOR":"Metric PQI",
                                   "DATE_CUT":"Category",
                                   "PLANT_CODE":"Plant Code",
                                  'STAGING_CODE_DESC/SPEC_NO':"Spec No",
                                   "NOTIFICATION/TEST":"Notification/Test"},inplace=True)


    #dropping records where material code is not available
    indexlist = detail_table[detail_table["Material Code"].isnull()].index.tolist()
    detail_table.drop(indexlist,inplace=True)

    return detail_table

def summarize_ppk(x):

    color = ''

    if len(np.unique(x)) == 1:
        color = np.unique(x)[0]

    else:
        #print(x.value_counts().index)
        colors = x.value_counts().index
        if "RED" in colors:
            color = "RED"
        else:
            color = x.value_counts().index[0]

    return color

def summarize_qms(x):
    color = ''

    if "RED" in x:
        color = "RED"
    else:
        color = x.value_counts().index[0]

    return color

def summarize_color(x):

    color = ''

    if len(np.unique(x)) == 1:
        color = np.unique(x)[0]

    else:
        colors = x.value_counts().index
        if "RED" in colors:
            color = "RED"
        elif "YELLOW" in colors:
            color = "YELLOW"
        else:
            color = "GREEN"

    return color

def extarct_ppk(df):

    ppk = df[df["Metric"]=="PPK"]
    ppk_df =  ppk.groupby(['Category',
                           'Plant Code',
                           'Material Code',
                           'Spec No']).agg({"Metric PQI":[summarize_ppk]}).reset_index()

    ppk_df.columns = [i[0] for i in ppk_df.columns]
    ppk_df.rename(columns={"Metric PQI":"METRIC_PPK"},inplace=True)
    return ppk_df

def extract_f1(df):

    f1 = df[(df["Metric"].str.contains("F1_Non Medical")) | (df["Metric"].str.contains("F1_Medical"))]


    f1_df =  f1.groupby(['Category',
                           'Plant Code',
                           'Material Code',
                           ]).agg({"Metric PQI":[summarize_qms]}).reset_index()

    f1_df.columns = [i[0] for i in f1_df.columns]
    f1_df.rename(columns={"Metric PQI":"METRIC_F1"},inplace=True)

    return f1_df

def extract_OOS(df):

    OOS = df[df["Metric"].str.contains("F2_Valid OOS",na=False)]


    OOS_df =  OOS.groupby(['Category',
                           'Plant Code',
                           'Material Code',
                           ]).agg({"Metric PQI":[summarize_qms]}).reset_index()

    OOS_df.columns = [i[0] for i in OOS_df.columns]
    OOS_df.rename(columns={"Metric PQI":"METRIC_OOS"},inplace=True)

    return OOS_df

def extract_OOT(df):

    OOT = df[df["Metric"].str.contains("OT_OOT",na=False)]


    OOT_df =  OOT.groupby(['Category',
                           'Plant Code',
                           'Material Code']).agg({"Metric PQI":[summarize_qms]}).reset_index()

    OOT_df.columns = [i[0] for i in OOT_df.columns]
    OOT_df.rename(columns={"Metric PQI":"METRIC_OOT"},inplace=True)

    return OOT

def summarize_ppk(x):

    color = ''

    if len(np.unique(x)) == 1:
        color = np.unique(x)[0]

    else:
        #print(x.value_counts().index)
        colors = x.value_counts().index
        if "RED" in colors:
            color = "RED"
        else:
            color = x.value_counts().index[0]

    return color

def summarize_qms(x):
    color = ''

    if "RED" in x:
        color = "RED"
    else:
        color = x.value_counts().index[0]

    return color

def summarize_color(x):

    color = ''

    if len(np.unique(x)) == 1:
        color = np.unique(x)[0]

    else:
        colors = x.value_counts().index
        if "RED" in colors:
            color = "RED"
        elif "YELLOW" in colors:
            color = "YELLOW"
        else:
            color = "GREEN"

    return color

def extarct_ppk(df):

    ppk = df[df["Metric"]=="PPK"]
    ppk_df =  ppk.groupby(['Category',
                           'Plant Code',
                           'Material Code',
                           'Spec No']).agg({"Metric PQI":[summarize_ppk]}).reset_index()

    ppk_df.columns = [i[0] for i in ppk_df.columns]
    ppk_df.rename(columns={"Metric PQI":"METRIC_PPK"},inplace=True)
    return ppk_df

def extract_f1(df):

    f1 = df[(df["Metric"].str.contains("F1_Non Medical")) | (df["Metric"].str.contains("F1_Medical"))]


    f1_df =  f1.groupby(['Category',
                           'Plant Code',
                           'Material Code',
                           ]).agg({"Metric PQI":[summarize_qms]}).reset_index()

    f1_df.columns = [i[0] for i in f1_df.columns]
    f1_df.rename(columns={"Metric PQI":"METRIC_F1"},inplace=True)

    return f1_df

def extract_OOS(df):

    OOS = df[df["Metric"].str.contains("F2_Valid OOS",na=False)]


    OOS_df =  OOS.groupby(['Category',
                           'Plant Code',
                           'Material Code',
                           ]).agg({"Metric PQI":[summarize_qms]}).reset_index()

    OOS_df.columns = [i[0] for i in OOS_df.columns]
    OOS_df.rename(columns={"Metric PQI":"METRIC_OOS"},inplace=True)

    return OOS_df

def extract_OOT(df):

    OOT = df[df["Metric"].str.contains("OT_OOT",na=False)]


    OOT_df =  OOT.groupby(['Category',
                           'Plant Code',
                           'Material Code']).agg({"Metric PQI":[summarize_qms]}).reset_index()

    OOT_df.columns = [i[0] for i in OOT_df.columns]
    OOT_df.rename(columns={"Metric PQI":"METRIC_OOT"},inplace=True)

    return OOT_df

def generate_summary_table(detail_table):

    ppk_df = extarct_ppk(detail_table)

    f1_df = extract_f1(detail_table)

    OOS_df = extract_OOS(detail_table)

    OOT_df = extract_OOT(detail_table)

    FTO3_Summary_df_qms = pd.concat([OOT_df,OOS_df,f1_df]).reset_index(drop=True)

    FTO3_Summary_df_qms.fillna("GREEN",inplace=True)

    t = FTO3_Summary_df_qms.groupby(['Category',
                                'Plant Code',
                                'Material Code',
                                ]).agg({
                                         "METRIC_OOT":[summarize_color],
                                         "METRIC_OOS":[summarize_color],
                                         "METRIC_F1": [summarize_color]}).reset_index()

    cols = [col[0] for col in t.columns]
    t.columns = cols

    summary_table = pd.merge(ppk_df,t,on=['Category','Plant Code','Material Code'],how='outer')
    summary_table.fillna("GREEN",inplace = True)

    return summary_table

def generate_reporting_tables(phr_summary_table,phr_detail_table):

    phr_detail_table['last_mod_date'] = datetime.datetime.now()
    phr_detail_table['source_detail'] = "Detail_Table_Product_Healthcare_Report"
    phr_detail_table['last_mod_by'] = "T00003222"

    phr_detail_table.columns = ['Plant_Code', 'Material_Code', 'Spec_No', 'Category', 'Batch_No',
           'Metric', 'Notification_Test', 'Description', 'Metric_PQI',
           'Plant_Name', 'Category_Type', 'last_mod_date', 'source_detail',
           'last_mod_by']

    phr_summary_table['last_mod_date'] = datetime.datetime.now()
    phr_summary_table['source_detail'] = "Summary_Table_Product_Healthcare_Report"
    phr_summary_table['last_mod_by'] = "T00003222"

    phr_summary_table.columns = ['Category', 'Plant_Code', 'Material_Code', 'Spec_No', 'METRIC_PPK',
           'METRIC_OOT', 'METRIC_OOS', 'METRIC_F1', 'last_mod_date',
           'source_detail', 'last_mod_by']

    summary_table_schema = StructType([
                 StructField('Category', StringType(), True),
                 StructField('Plant_Code', StringType(), True),
                 StructField('Material_Code', StringType(), True),
                 StructField('Spec_No', StringType(), True),
                 StructField('METRIC_PPK', StringType(), True),
                 StructField('METRIC_OOT', StringType(), True),
                 StructField('METRIC_OOS', StringType(), True),
                 StructField('METRIC_F1', StringType(), True),
                 StructField('last_mod_date', DateType(), True),
                 StructField('source_detail', StringType(), True),
                 StructField('last_mod_by', StringType(), True)])

    detail_table_schema = StructType([
                 StructField('Plant_Code', StringType(), True),
                 StructField('Material_Code', StringType(), True),
                 StructField('Spec_No', StringType(), True),
                 StructField('Category', StringType(), True),
                 StructField('Batch_No', StringType(), True),
                 StructField('Metric', StringType(), True),
                 StructField('Notification_Test', StringType(), True),
                 StructField('Description', StringType(), True),
                 StructField('Metric_PQI', StringType(), True),
                 StructField('Plant_Name', StringType(), True),
                 StructField('Category_Type', StringType(), True),
                 StructField('last_mod_date', DateType(), True),
                 StructField('source_detail', StringType(), True),
                 StructField('last_mod_by', StringType(), True),])

    detail_table_df = spark.createDataFrame(phr_detail_table, schema=detail_table_schema)
    summary_table_df = spark.createDataFrame(phr_summary_table, schema=summary_table_schema)

    detail_table_df.write.format("orc").mode('overwrite').saveAsTable('raw.trpt_detail_table')
    summary_table_df.write.format("orc").mode('overwrite').saveAsTable('raw.trpt_summary_table')

def run(data,data_qms,plant_code,batch_initials):


    print("Running the script for plant ",plant_code)

    data = data[data.PLANT_CODE == plant_code].copy()
    data_qms = data_qms[data_qms.PLANT_CODE == plant_code].copy()



    data["batch_initials"] = data.BATCH_NO.str.slice(0,1)
    data = data[data["batch_initials"]==batch_initials]
    del data["batch_initials"]


    data_qms = prepare_data(data_qms)
    data_qms = create_date_cols(data_qms)
    date_cut = create_date_cuts()

    ppk_df,qms_df = date_wise_data(date_cut,data,data_qms)
    ppk_df_batch_cut,qms_df_batch_cut = batch_wise_data(data,data_qms)
    detail_table = prepare_detail_table(ppk_df,qms_df,ppk_df_batch_cut,qms_df_batch_cut)
    summary_table = generate_summary_table(detail_table)

    return summary_table, detail_table



###MAIN CODE######
if __name__ == '__main__':

    with open("/home/appadmin/phr_report/parameters.json",'r') as f:
    #with open("parameters.json",'r') as f:
        params = json.load(f)

    print("******")
    print("Loading parameters/credentials")
    ppk_table = params["ppk_table"]
    qms_table = params["qms_table"]
    # plant_code = params["plant_details"]["FTO_3"]["plant_code"]
    # batch_initials = params["plant_details"]["FTO_3"]["batch_initials"]
    print("******")

    print("ppk_table : ",ppk_table)
    print("qms_table : ",qms_table)


    data = spark.table(ppk_table).toPandas()
    data_qms = spark.table(qms_table).toPandas()

    print("Tables Loaded")

    data.columns = [cols.upper() for cols in data.columns]
    data_qms.columns = [cols.upper() for cols in data_qms.columns]
    data_qms.rename(columns = {"CREATED_DATE_TIME" : "TEST_DATE"},inplace=True)

    data["TEST_DATE"] = pd.to_datetime(data["TEST_DATE"])
    data["NLT_ETL"] = data.NLT_ETL.replace('',np.nan).astype('float')
    data["NMT_ETL"] = data.NMT_ETL.replace('',np.nan).astype('float')

    phr_summary_table = pd.DataFrame()
    phr_detail_table = pd.DataFrame()

    for plant in params['plant_details']:

        plant_code = params['plant_details'][plant]['plant_code']
        batch_initials = params['plant_details'][plant]['batch_initials']

        print("PLANT CODE ----- ",plant_code)
        print("BATCH INITIALS -----",batch_initials)

        summary_table_plant, detail_table_plant = run(data,data_qms,plant_code,batch_initials))

        phr_summary_table = pd.concat([phr_summary_table,summary_table_plant])
        phr_detail_table = pd.concat([phr_detail_table,detail_table_plant])

    generate_reporting_tables(phr_summary_table,phr_detail_table)
    print("Reporting Tables Generated")
