import pandas as pd
import numpy as np
import datetime
import os
import re
from time import sleep
from ftplib import FTP

#记得修改路径
path = "C:\\Users\\xieminchao\\Desktop\\benefit_cal"
os.chdir(path)

start_date = "2020-10-01"
end_date = "2020-10-08"
maoyan_date = str(datetime.date(int(end_date[0:4]),int(end_date[5:7]),int(end_date[8:])))
df_film = pd.read_excel("影片日期.xlsx")

#嘉影厅列表
df_jiaying_hall = pd.read_excel("嘉影厅列表.xlsx")
jiaying_hall_list = np.array(df_jiaying_hall["影院"]).tolist()

#如果更改时间的话，记得改文件名
writer = pd.ExcelWriter("%s-%s影片排座汇总 计算.xlsx" % (start_date[5:].replace("-","."),end_date[5:].replace("-",".")))

def ftp_run(date_list):
    ftp = FTP()
    ftp.connect(host = "172.20.240.195",port = 21 ,timeout = 30)
    ftp.login(user = "sjzx",passwd = "jy123456@")
    list = ftp.nlst()
    for each_date in date_list:
        each_date = each_date.replace("-","")
        filename = "SessionRevenue_"+each_date+".csv"
        for each_file in list:
            judge = re.match(filename,each_file)
            if judge:
                file_handle = open(filename,"wb+")
                ftp.retrbinary("RETR "+filename,file_handle.write)
                file_handle.close()
                print("%s file download success" % filename)
                sleep(1)
    ftp.quit()

df_total = pd.DataFrame()
date_list =[str(x)[0:10] for x in pd.date_range(start = start_date,end = end_date,freq = "D")]
ftp_run(date_list)
listdir = os.listdir(path)
for each_date in date_list:
    filename = "SessionRevenue_"+ each_date.replace("-","") +".csv"
    if filename in listdir:
        each_df = pd.read_csv(filename,encoding = "utf-8")
        showcount_time = each_df["场次时间"]
        time_list = []
        t1 = datetime.datetime.strptime(each_date + " 06:00:00","%Y-%m-%d %H:%M:%S")
        t2 = datetime.datetime.strptime(str(datetime.date(int(each_date[0:4]),int(each_date[5:7]),int(each_date[8:10])) + datetime.timedelta(days =1)) + " 05:59:59","%Y-%m-%d %H:%M:%S")
        for each_time in showcount_time:
            tmp_time = datetime.datetime.strptime(each_time,"%Y-%m-%d %H:%M:%S")
            delta1 = tmp_time - t1
            delta2 = t2 - tmp_time
            if delta1.days == 0 and delta2.days == 0:
                time_list.append(each_time)
        each_df = each_df[each_df["场次时间"].isin(time_list)]
        film = np.array(each_df["影片"])
        pat = "（数字）|（数字3D）|（数字IMAX）|（数字IMAX3D）|（中国巨幕）|（中国巨幕立体）|（IMAX3D）|（IMAX 3D）|（IMAX）|\s*"
        for i in range(len(film)):
            film[i] = re.sub(pat,"",film[i])
        each_df["影片"] = film
        each_df.insert(4,"场次时间2",each_df["场次时间"])
        film_time = np.array(each_df["场次时间"])
        pat_time = "\d+-\d+-\d+"
        for i in range(len(film_time)):
            film_time[i] = re.findall(pat_time,film_time[i])[0]
        each_df["场次时间"] = film_time
        each_df = each_df[each_df["场次状态"].isin(["开启"])]
        df_total = pd.concat([df_total,each_df],ignore_index = True)
        print("%s data process complete" % each_date)
        
#第一遍先找出冷门片的包场数据，并剔除
columns_dict = {"票房":"总影院票房","人数":"总影院人数","总座位数":"总影院排座数"}
#计算影院影片的票房、人数、座位数
table = pd.pivot_table(df_total, index = ["影院","影片"],values = ["票房","人数","总座位数","场次时间"],aggfunc = {"票房":np.sum,"人数":np.sum,"总座位数":np.sum,"场次时间":len},fill_value = 0,margins = False)
df_table = pd.DataFrame(table)
df_table.sort_values(by = "票房",ascending = False,inplace = True)
df_table.reset_index(inplace = True)
#计算影院影片场次
df_table_session = pd.pivot_table(df_table,index = ["影院"],values = ["场次时间"],aggfunc = {"场次时间":np.sum},fill_value = 0,margins = False)
df_table_session.rename(columns = {"场次时间":"总影院场次数"},inplace = True)
df_table_session.reset_index(inplace = True)
df_table = pd.merge(df_table,df_table_session,how = "left",on = "影院")
df_table_total = pd.pivot_table(df_table,index = ["影院"],values = ["票房","人数","总座位数"],aggfunc = {"票房":np.sum,"人数":np.sum,"总座位数":np.sum},fill_value = 0,margins = False)
df_table_total.rename(columns = columns_dict,inplace = True)
df_table_total.reset_index(inplace = True)
df_table = pd.merge(df_table,df_table_total,how = "left",on = "影院")
# df_table["排片占比"] = np.round(np.divide(df_table["场次时间"],df_table["总影院场次数"]),6)
df_table["排座占比"] = np.round(np.divide(df_table["总座位数"],df_table["总影院排座数"]),6)
df_table["人次占比"] = np.round(np.divide(df_table["人数"],df_table["总影院人数"]),6)
df_table["票房占比"] = np.round(np.divide(df_table["票房"],df_table["总影院票房"]),6)
df_table["上座率"] = np.round(np.divide(df_table["人数"],df_table["总座位数"]),4)
df_table["排座效益"] = np.round(np.divide(df_table["票房占比"],df_table["排座占比"]),6)
df_one_session_film = df_table[df_table["场次时间"].isin([1,2])].loc[:,["影院","影片"]]
df_one_session_film.sort_values(by = "影院",ascending = True,inplace = True)
df_one_session_film.reset_index(drop = True,inplace = True)
print(df_one_session_film)
#筛选出冷门片但上座率高、排座效益偏高的影院影片数据
df_table_cold_film = df_table[(df_table["上座率"] >= 0.25) & (df_table["排座占比"] <= 0.08) & (df_table["排座效益"] >= 2.3)]
print(df_table_cold_film)
#去到原始的合并数据里划分成两部分
#重新计算上座率，不去用文本转数字了
df_total["上座率2"] = np.round(np.divide(df_total["人数"].astype(int),df_total["总座位数"].astype(int)),4)
print(len(df_total))
df_total_part1 = df_total[df_total["上座率2"] < 0.7]
df_total_part2 = df_total[df_total["上座率2"] >= 0.7]
# #用各自列的tolist方法换为列表，然后就可以取值了，否则df的loc方法返回的object对象无法转为str
# t = 0
# for i in df_total_part2.index:
#     for j in df_table_cold_film.index:
#         if df_total_part2.loc[i:i,"影院"].tolist()[0] == df_table_cold_film.loc[j:j,"影院"].tolist()[0] and df_total_part2.loc[i:i,"影片"].tolist()[0] == df_table_cold_film.loc[j:j,"影片"].tolist()[0]:
#             df_total_part2.drop(index = i,axis = 0,inplace = True)
#             t += 1
#             print("已剔除:%s" % t)
#             #在源数据剔除了所以跳到下一个循环
#             break

#得出剔除包场数据后合并
df_total2 = pd.concat([df_total_part1,df_total_part2],ignore_index = True)
df_total2.sort_values(by = "影院",ascending = True,inplace = True)
df_total2.reset_index(drop = True,inplace = True)
print(len(df_total2))
#建立过滤函数
#df_total和df_filter必须都先reset_index，即index重置
def df_filter_data(df_total,df_filter,main_field,sub_field,midnight_session = False):
    df_res = pd.DataFrame()
    for each_df in [df_total,df_filter]:
        each_df.sort_values(by = [main_field,sub_field],ascending = [True,True],inplace = True)
        each_df.reset_index(drop = True,inplace = True)
    
    def df_data_index_list(df,field):
        df_data_list,df_index_list = [],[]
        for i in df.index:
            data = df.loc[i:i,field].tolist()[0]
            if data not in df_data_list:
                df_data_list.append(data)
                df_index_list.append(i)
        return df_data_list,df_index_list
    
    df_total_main_list,df_total_index_list = df_data_index_list(df_total,main_field)
    df_filter_main_list,df_filter_index_list = df_data_index_list(df_filter,main_field)
    #用于标记连接位置
    flag = 0
    s = 0
    for i in range(len(df_total_index_list)):
        total_begin = df_total_index_list[i]
        total_end = 0
        if i != len(df_total_main_list) - 1:
            total_end = df_total_index_list[i+1]
        else:
            total_end = len(df_total)
        for j in range(len(df_filter_index_list)):
            filter_begin = df_filter_index_list[j]
            filter_end = 0
            if j != len(df_filter_main_list) - 1:
                filter_end = df_filter_index_list[j+1]
            else:
                filter_end = len(df_filter)
            if df_total_main_list[i] == df_filter_main_list[j]:
                for p in range(total_begin,total_end):
                    for q in range(filter_begin,filter_end):
                        #跳过匹配的那一行进行拼接
                        flag_status = 0
                        if df_total.loc[p:p,sub_field].tolist()[0] == df_filter.loc[q:q,sub_field].tolist()[0]:
                            if midnight_session == False:
                                flag_status = 1
                            #若是要剔除新片的午夜场
                            else:
                                tmp_time = datetime.datetime.strptime(df_total.loc[p:p,"场次时间2"].tolist()[0],"%Y-%m-%d %H:%M:%S")
                                t1 = datetime.datetime.strptime(df_filter.loc[q:q,sub_field].tolist()[0] + " 00:00:00","%Y-%m-%d %H:%M:%S")
                                t2 = datetime.datetime.strptime(df_filter.loc[q:q,sub_field].tolist()[0] + " 05:59:59","%Y-%m-%d %H:%M:%S")
                                delta1 = tmp_time - t1
                                delta2 = t2 - tmp_time
                                if delta1.days == 0  and delta2.days == 0:
                                    flag_status = 1
                        if flag_status == 1:
                            df_res = pd.concat([df_res,df_total.loc[flag:p-1,:]],ignore_index = True)
                            flag = p + 1
                            s += 1
                            print("%s名:%s,%s名:%s" % (main_field,df_total.loc[p:p,main_field].tolist()[0],sub_field,df_filter.loc[q:q,sub_field].tolist()[0]))
                            print("已剔除：%s" % s)
            #补上最后未匹配到的部分                    
            if j == len(df_filter_main_list) - 1 and i == len(df_total_main_list) - 1:
                df_res = pd.concat([df_res,df_total.loc[flag:len(df_total) - 1,:]],ignore_index = True)
    
    return df_res

df_total2 = df_filter_data(df_total2,df_one_session_film,"影院","影片")
print(len(df_total2))
# df_total2 = df_filter_data(df_total2,df_jiaying_hall,"影院","影厅")
# print(len(df_total2))
# df_total2 = df_filter_data(df_total2,df_film,"影片","场次时间",midnight_session = True)
# print(len(df_total2))

table2 = pd.pivot_table(df_total2, index = ["影院","影片"],values = ["票房","人数","总座位数","场次时间"],aggfunc = {"票房":np.sum,"人数":np.sum,"总座位数":np.sum,"场次时间":len},fill_value = 0,margins = False)
df_table2 = pd.DataFrame(table2)
df_table2.sort_values(by = "票房",ascending = False,inplace = True)
df_table2.reset_index(inplace = True)
df_table2_session = pd.pivot_table(df_table2,index = ["影院"],values = ["场次时间"],aggfunc = {"场次时间":np.sum},fill_value = 0,margins = False)
df_table2_session.rename(columns = {"场次时间":"总影院场次数"},inplace = True)
df_table2_session.reset_index(inplace = True)
df_table2 = pd.merge(df_table2,df_table2_session,how = "left",on = "影院")
df_table2_total = pd.pivot_table(df_table2,index = ["影院"],values = ["票房","人数","总座位数"],aggfunc = {"票房":np.sum,"人数":np.sum,"总座位数":np.sum},fill_value = 0,margins = False)
df_table2_total.rename(columns = columns_dict,inplace = True)
df_table2_total.reset_index(inplace = True)
df_table2 = pd.merge(df_table2,df_table2_total,how = "left",on = "影院")
df_table2["上座率"] = np.round(np.divide(df_table2["人数"],df_table2["总座位数"]),6)
df_table2["排座占比"] = np.round(np.divide(df_table2["总座位数"],df_table2["总影院排座数"]),6)
df_table2["人次占比"] = np.round(np.divide(df_table2["人数"],df_table2["总影院人数"]),6)
df_table2["票房占比"] = np.round(np.divide(df_table2["票房"],df_table2["总影院票房"]),6)
df_table2["排座效益"] = np.round(np.divide(df_table2["票房占比"],df_table2["排座占比"]),6)
df_table2["效益误差平方和"] = np.round(np.square(np.subtract(df_table2["排座效益"],np.full(len(df_table2),1))),6)
df_table2["排座效益加权误差平方和"] = np.round(np.multiply(df_table2["效益误差平方和"],df_table2["排座占比"]),8)
df_table2["排座效率"] = np.round(np.divide(df_table2["人次占比"],df_table2["排座占比"]),6)
df_table2["效率误差平方和"] = np.round(np.square(np.subtract(df_table2["排座效率"],np.full(len(df_table2),1))),6)
df_table2["排座效率加权误差平方和"] = np.round(np.multiply(df_table2["效率误差平方和"],df_table2["排座占比"]),8)
df_table2.to_excel(writer,sheet_name = "数据源及计算",header = True,index = False)
df_res = pd.pivot_table(df_table2,index = ["影院"],values = ["排座效益加权误差平方和","排座效率加权误差平方和"],aggfunc = {"排座效益加权误差平方和":np.sum,"排座效率加权误差平方和":np.sum},fill_value = 0,margins = False)
df_res.reset_index(inplace = True)
df_res.sort_values(by = "排座效率加权误差平方和",ascending = True,inplace = True)
df_res["排座效率排名"] = df_res["排座效率加权误差平方和"].rank(ascending = True,method = "min")
df_res["标准化分数"] = np.round(np.abs(np.divide(df_res["排座效率加权误差平方和"] - df_res["排座效率加权误差平方和"].min(),df_res["排座效率加权误差平方和"].max() - df_res["排座效率加权误差平方和"].min()) * 10 - 10),3)
df_res.to_excel(writer,sheet_name = "影院排名",header = True,index = False)
df_total2.to_excel(writer,sheet_name = "数据源",header = True,index= False)
writer.save()