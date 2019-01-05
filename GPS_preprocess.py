# -*- coding: utf-8 -*-
"""
Created on Wed Jan 17 18:44:12 2018

@author: Administrator
"""
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import copy
class square:
    string=['RECDATETIME','ISARRLFT','PRODUCTID','STATIONSEQNUM','PACKCODE','GPSSPEED','ROUTEID','LONGITUDE','LATITUDE','STATIONNUM']
    string2=['RECDATETIME','ISARRLFT','PRODUCTID','STATIONSEQNUM','PACKCODE','GPSSPEED','ROUTEID','LONGITUDE','LATITUDE','STATIONNUM','STAORDER']
    string3=['RECDATETIME','ISARRLFT','TIME','ROUTEID','PRODUCTID']
    string4=['ISARRLFT','RECDATETIME1','ROUTEID1','TIME1','RECDATETIME2','ROUTEID2','TIME2','RECDATETIME','ROUTEID','TIME']
    
    def oringin(data,sta_num,num):
        from pandas.tseries.offsets import Second
        su=square.find_info(num,sta_num)
        orin_dict={'RECDATETIME':(pd.to_datetime(data['RECDATETIME'])+120*Second()).values,'ISARRLFT':100000,'PRODUCTID':np.nan,'STATIONSEQNUM':np.nan,'PACKCODE':np.nan,'GPSSPEED':np.nan,'ROUTEID':np.nan,'LONGITUDE':su[0],'LATITUDE':su[1],'STATIONNUM':su[3],'STAORDER':su[2]}
        orin=pd.DataFrame(orin_dict,index=['100000'])
        return orin

    def oringin1(data,sta_num,num):
        from pandas.tseries.offsets import Second
        su=square.find_info1(num,sta_num)
        orin_dict={'RECDATETIME':(pd.to_datetime(data['RECDATETIME'])-120*Second()).values,'ISARRLFT':100000,'PRODUCTID':data['PRODUCTID'],'STATIONSEQNUM':data['STATIONSEQNUM'],'PACKCODE':data['PACKCODE'],'GPSSPEED':data['GPSSPEED'],'ROUTEID':data['ROUTEID'],'LONGITUDE':su[0],'LATITUDE':su[1],'STATIONNUM':su[3],'STAORDER':su[2]}
        orin=pd.DataFrame(orin_dict,index=['100000'])
        return orin

    def oringin3(data,sta_num,delta_sec,num):
        print(delta_sec)
        from pandas.tseries.offsets import Second
        su=square.find_info1(num,sta_num)
        orin_dict={'RECDATETIME':(pd.to_datetime(data['RECDATETIME'])-delta_sec*Second()).values,'ISARRLFT':100000,'PRODUCTID':data['PRODUCTID'],'STATIONSEQNUM':data['STATIONSEQNUM'],'PACKCODE':data['PACKCODE'],'GPSSPEED':data['GPSSPEED'],'ROUTEID':data['ROUTEID'],'LONGITUDE':su[0],'LATITUDE':su[1],'STATIONNUM':su[3],'STAORDER':su[2]}
        orin=pd.DataFrame(orin_dict,index=['100000'])
        return orin
       
    def __init__(self,up=30.710745,down=30.6888,left=104.036693,right=104.04945):
        '''确定实例范围'''
        self._up=up
        self._down=down
        self._right=right
        self._left=left

    def enpend(self,sigma=60):
        '''确定容差范围'''
        sigmax=sigma*0.000008990363
        sigmay=sigma*0.000009020117
        self._left=self._left-sigmax
        self._right=self._right+sigmax
        self._down=self._down-sigmay
        self._up=self._up+sigmay
        
    def judge(self,data):
        '''输入Dataframe和线路，返回某一经纬度范围内的数据'''
        return data[(data['LONGITUDE']<self._right)&(\
                    data['LONGITUDE']>self._left)&(data['LATITUDE']>self._down)&(data['LATITUDE']<self._up)]
    
    def choose_route(data,num):
        '''输入Dataframe和线路，返回某一线路的数据'''
        return data[data['ROUTEID']==num]
    
    def choose_bus(data,num):  
        '''输入Dataframe和线路，返回某一车载机编号的数据'''
        b=data[data['PRODUCTID']==num]
        b=square.sort_time(b) 
        return b
        
    def sort_time(data):
        '''输入Dataframe和线路，返回按时间排好序的线路'''
        data['RECDATETIME']=pd.to_datetime(data['RECDATETIME'])
        data=data.sort_values(by='RECDATETIME')
        return data
    
    def manuver_data(self,filename,output,chunksize=10000):
        '''输入文件名，返回所有的指定范围内的数据，支持大文本'''
        loop=True
        reader=pd.read_csv(filename,names=['RECDATETIME','ISARRLFT','PRODUCTID','STATIONSEQNUM',\
                                           'PACKCODE','GPSSPEED',\
                                           'ROUTEID','LONGITUDE','LATITUDE','STATIONNUM'],iterator=True)
        while loop:
            try:
                a=reader.get_chunk(chunksize)
                b=self.judge(a)
                b.to_csv(output,mode='a',header=None,)
            except StopIteration:
                print('stop')
                loop=False
                
    def split_bus(data,num):
        '''输入Dataframe和线路，返回所有的车载机编号'''
        b=square.sort_time(square.choose_route(data,num))        
        a=b['PRODUCTID'].unique()
        l1=[]
        for i in a:
            m=square.choose_bus(b,i)
            i=str(i)
            a='bus'+str(num)+'_'+i+'.csv'
            m.to_csv(a)
            l1.append(a)
        return l1
    
    def split_count(data,num,second_sigma=600,code_sigma=10):
        '''找出一片区域内的固定班次数据'''
        from pandas.tseries.offsets import Second
        b=square.sort_time(square.choose_route(data,num))        
        a=b['PRODUCTID'].unique()
        for i in a:
            m=square.choose_bus(b,i)
            m1=copy.copy(m.iloc[0:1,:])
            flag=1
            m1.to_csv('bus'+str(num)+'_'+str(i)+'_'+str(flag)+'.csv',mode='a',header=False)
            length=m.iloc[:,0].size
            for j in range(length-1):
                b1=copy.copy(pd.to_datetime(m.iloc[j,0]))
                b2=copy.copy(pd.to_datetime(m.iloc[j+1,0]))
                if (b1+second_sigma*Second()>=b2): 
                    m1=copy.copy(m.iloc[j+1:j+2,:])
                    m1.to_csv('bus'+str(num)+'_'+str(i)+'_'+str(flag)+'.csv',mode='a',header=False)
                else:
                    
                    flag+=1
                    m1=copy.copy(m.iloc[j+1:j+2,:])
                    m1.to_csv('bus'+str(num)+'_'+str(i)+'_'+str(flag)+'.csv',mode='a',header=False)
    
    def plot_gps(data):
        '''输入Dateframe，画出行车轨迹'''
        x=[float(i) for i in data['LONGITUDE']]
        y=[float(i) for i in data['LATITUDE']]
        plt.scatter(x,y)
        plt.show()
        
    def plot_gps1(filename):
        '''输入数据名，画出行车轨迹'''
        data=pd.read_csv(filename,names=square.string)
        x=[float(i) for i in data['LONGITUDE']]
        y=[float(i) for i in data['LATITUDE']]
        plt.scatter(x,y)
        plt.show()
    
    def stop_gps(num):
        '''选择num的所有站台编号，返回列表'''
        k=pd.read_csv('stopGIS.csv',encoding='gbk')
        k=k[k['LINENO']==str(num)]
        k=[i for i in k['STATIONA']]
        return k
    
    def stop_order(num):
        '''选择num所需的站点GIS数据，以便后期数据融合'''
        k=pd.read_csv('stopGIS.csv',encoding='gbk')
        k=k[k['LINENO']==str(num)]
        k=copy.copy(k.loc[:,['STATIONA','STAORDER']])
        return k
    
    def drop_duplicates(filename,outputname,chunk):
        '''删除重复值函数'''
        chunksize=chunk
        loop=True
        reader=pd.read_csv(filename,usecols=square.string2,iterator=True)
        b=pd.DataFrame(columns=square.string)
        while loop:
            try:
                a=reader.get_chunk(chunksize)
                a=a.drop_duplicates('STATIONNUM',keep='first')
                b=b.append(a)
            except StopIteration:
                loop=False
        b.to_csv(outputname)
        
    def is_come_go(data1,data2):
        '''班次结束判定函数'''
        if (int(data1['STAORDER'].values)-10)>int(data2['STAORDER'].values):
            return True
        else:
            return False
    
    def is_lack_rec(data1,data2):
        '''缺失值判定函数'''
        if ((int(data1['STAORDER'].values)+1)<(int(data2['STAORDER'].values)))&(~(square.is_come_go(data1,data2))):
            return True
        else:
            return False
    
    def is_abnormal(data1,data2,data3):
        '''异常值判定函数'''
        if (int(data1['STAORDER'].values)>=int(data2['STAORDER'].values))&(int(data1['STAORDER'].values)<=int(data3['STAORDER'].values))|((int(data1['STAORDER'].values)<=int(data2['STAORDER'].values))&(int(data3['STAORDER'].values)<=int(data2['STAORDER'].values))&(int(data3['STAORDER'].values)+10>=int(data2['STAORDER'].values)))|(int(data1['STAORDER'].values)<=int(data2['STAORDER'].values)-20)&(int(data3['STAORDER'].values)<=int(data2['STAORDER'].values)-20):
            return True
        else:
            return False
        
    def is_end(data1,data2):
        '''开头数据缺失判定函数'''
        if data2['STAORDER'].values==1:
            return True
        else:
            return False
    
    def is_end_lack(data1,data2,num):
        '''班次结束数据缺失函数'''
        b=square.oringin1(data2,data2['STATIONNUM'].values,num)
        if square.is_end(data1,data2)&b['STAORDER'].values!=data1['STAORDER'].values:
            return int(b['STAORDER'].values)-int(data1['STAORDER'].values)
        else:
            return 0
    
    def find_info(num,st):
        '''找到下一站的经纬度，服务于非结尾数据'''
        k=pd.read_csv('stopGIS.csv',encoding='gbk')
        k=k[k['LINENO']==str(num)]
        k=copy.copy(k.loc[:,['STATIONA','STAORDER','LOT','LAT']])
        lot=[i for i in k['LOT']]
        lat=[i for i in k['LAT']]
        order=[i for i in k['STAORDER']]
        nam=[i for i in k['STATIONA']]
        a=nam.index(st)
        if a<len(nam)-1:
            lot1=lot[a+1]
            lat1=lat[a+1]
            order1=order[a+1]
            nam1=nam[a+1]
        else:
            lot1=lot[-1]
            lat1=lat[-1]
            order1=order[-1]
            nam1=nam[-1]
        su=[lot1,lat1,order1,nam1]
        return su

    def find_info1(num,st):
        '''找到下一站的经纬度，服务于结尾数据'''
        k=pd.read_csv('stopGIS.csv',encoding='gbk')
        k=k[k['LINENO']==str(num)]
        k=copy.copy(k.loc[:,['STATIONA','STAORDER','LOT','LAT']])
        lot=[i for i in k['LOT']]
        lat=[i for i in k['LAT']]
        order=[i for i in k['STAORDER']]
        nam=[i for i in k['STATIONA']]
        a=nam.index(st)
        lot1=lot[a-1]
        lat1=lat[a-1]
        order1=order[a-1]
        nam1=nam[a-1]
        su=[lot1,lat1,order1,nam1]
        return su
   
    def del_abnor(filename,outputname):
        '''删除异常数据'''
        df1=pd.read_csv(filename,usecols=square.string2)
        length=df1.iloc[:,0].size
        b=pd.DataFrame(columns=square.string2)
        for j in range(length-2):
            b1=copy.copy(df1.iloc[j:j+1,:])
            b2=copy.copy(df1.iloc[j+1:j+2,:])
            b3=copy.copy(df1.iloc[j+2:j+3,:])
            if j==0:
                if (int(b1['STAORDER'].values)<int(b2['STAORDER'].values)):
                    b=b.append(b1)
                    b=b.append(b2)
                elif (int(b1['STAORDER'].values)>int(b2['STAORDER'].values))&(int(b1['STAORDER'].values)<int(b3['STAORDER'].values)):
                    b2['STAORDER']=100
                    b=b.append(b1)
                    b=b.append(b2)
                elif (int(b1['STAORDER'].values)>int(b2['STAORDER'].values))&(int(b1['STAORDER'].values)>int(b3['STAORDER'].values)):
                    b1['STAORDER']=100
                    b=b.append(b1)
                    b=b.append(b2)
                
            elif j==length-3:
                b=b.append(b2)
                b=b.append(b3)
            else:
                if square.is_abnormal(b1,b2,b3):
                    b2['STAORDER']=100
                    b=b.append(b2)
                else:
                    b=b.append(b2)
        b=b[b['STAORDER']!=100]
        b.to_csv(outputname)
    
    def insert_items(filename,outputname,num):
        '''插入中间站点'''
        df1=pd.read_csv(filename,usecols=square.string2)
        length=df1.iloc[:,0].size
        #b0=square.oringin()
        b=pd.DataFrame(columns=square.string2)
        for j in range(length-1):
            b1=copy.copy(df1.iloc[j:j+1,:])
            b2=copy.copy(df1.iloc[j+1:j+2,:])
            if j==0:
                b=b.append(b1)
                if square.is_lack_rec(b1,b2):                    
                    b0=square.oringin(b1,int(b1['STATIONNUM'].values),num)
                    b=b.append(b0)
                b=b.append(b2)
            elif j==length-2:
                b=b.append(b2)
            else:
                if square.is_lack_rec(b1,b2):
                    li=[i for i in range(int(b1['STAORDER'].values),int(b2['STAORDER'].values))]
                    #计算间隔总秒数
                    bdate=b1.loc[:,'RECDATETIME'].values[0]
                    edate=b2.loc[:,'RECDATETIME'].values[0]
                    dt=np.datetime64(edate)-np.datetime64(bdate)
                    all_dt=dt.item().total_seconds()
                    each_dt=all_dt/len(li)
                    bo=b2['STATIONNUM'].values
                    li2=[]
                    for i in range(len(li)-1):
                        if i==0:
                            b0=square.oringin3(b2,bo,each_dt,num)
                        else:
                            b0=square.oringin3(b0,bo,each_dt,num)
                        li2.append(b0)
                        bo=b0['STATIONNUM'].values
                    for i in range(len(li2)-1,-1,-1):
                        b=b.append(li2[i])
                    b=b.append(b2)
                else:
                    b=b.append(b2)
        b[['PRODUCTID','STATIONSEQNUM','PACKCODE','GPSSPEED','ROUTEID','LONGITUDE','LATITUDE','STATIONNUM']]=b[['PRODUCTID','STATIONSEQNUM','PACKCODE','GPSSPEED','ROUTEID','LONGITUDE','LATITUDE','STATIONNUM']].fillna(method='ffill')
        b.to_csv(outputname,columns=square.string2)
    
    def insert_head(filename,outputname,num):
        '''插入开始缺失的站点'''
        df1=pd.read_csv(filename,usecols=square.string2)
        length=df1.iloc[:,0].size
        #b0=square.oringin()
        b=pd.DataFrame(columns=square.string2)
        for j in range(length-1):
            b1=copy.copy(df1.iloc[j:j+1,:])
            b2=copy.copy(df1.iloc[j+1:j+2,:])
            if j==0:
                if int(b1['STAORDER'].values)!=1:
                    li=[i for i in range(1,int(b2['STAORDER'].values))]
                    bo=b2['STATIONNUM'].values
                    li2=[]
                    for i in range(len(li)):
                        if i==0:
                            b0=square.oringin1(b2,bo,num)
                        else:
                            b0=square.oringin1(b0,bo,num)
                        li2.append(b0)
                        bo=b0['STATIONNUM'].values
                    for i in range(len(li2)-1,-1,-1):
                        b=b.append(li2[i])
                    b=b.append(b2)
                else:
                    b=b.append(b1)
                    b=b.append(b2)
                b[['PRODUCTID','STATIONSEQNUM','PACKCODE','GPSSPEED','ROUTEID','LONGITUDE','LATITUDE','STATIONNUM']]=b[['PRODUCTID','STATIONSEQNUM','PACKCODE','GPSSPEED','ROUTEID','LONGITUDE','LATITUDE','STATIONNUM']].fillna(method='bfill')
            elif j==length-2:
                b=b.append(b2)
            else:
                if square.is_come_go(b1,b2):
                    if int(b2['STAORDER'].values)!=1:
                        li=[i for i in range(1,int(b2['STAORDER'].values))]
                        bo=b2['STATIONNUM'].values
                        li2=[]
                        for i in range(len(li)):
                            if i==0:
                                b0=square.oringin1(b2,bo,num)
                            else:
                                b0=square.oringin1(b0,bo,num)
                            li2.append(b0)
                            bo=b0['STATIONNUM'].values
                        for i in range(len(li2)-1,-1,-1):
                            b=b.append(li2[i])
                        b=b.append(b2)
                    else:
                        b=b.append(b2)
                else:
                    b=b.append(b2)
        b[['PRODUCTID','STATIONSEQNUM','PACKCODE','GPSSPEED','ROUTEID','LONGITUDE','LATITUDE','STATIONNUM']]=b[['PRODUCTID','STATIONSEQNUM','PACKCODE','GPSSPEED','ROUTEID','LONGITUDE','LATITUDE','STATIONNUM']].fillna(method='ffill')
        b.to_csv(outputname,columns=square.string2)
        
    def insert_tail(filename,outputname,num):
        '''插入末尾的站点'''
        df1=pd.read_csv(filename,usecols=square.string2)
        length=df1.iloc[:,1].size
        b=pd.DataFrame(columns=square.string2)
        for i in range(length-1):
            b1=copy.copy(df1.iloc[i:i+1,:])
            b2=copy.copy(df1.iloc[i+1:i+2,:])
            a=square.is_end_lack(b1,b2,num)
            if i==0:
                b=b.append(b1)
                b=b.append(b2)
            else:
                if a:
                    for j in range(a):
                        if j==0:
                            b0=square.oringin(b1,int(b1['STATIONNUM'].values),num)
                            b=b.append(b0)
                        else:
                            b0=square.oringin(b0,int(b0['STATIONNUM'].values),num)
                            b=b.append(b0)
                b=b.append(b2)
            b[['PRODUCTID','STATIONSEQNUM','PACKCODE','GPSSPEED','ROUTEID','LONGITUDE','LATITUDE','STATIONNUM']]=b[['PRODUCTID','STATIONSEQNUM','PACKCODE','GPSSPEED','ROUTEID','LONGITUDE','LATITUDE','STATIONNUM']].fillna(method='ffill')
            b.to_csv(outputname,columns=square.string2)
                    
    def clear_data(filename,num):
        '''输入的是某一天一条线路一个班次的所有的gps点，
        输出的是清洁的数据'''
        list1=square.stop_gps(num)
        df=pd.read_csv(filename)
        df=df[df['ISARRLFT']==1]
        o=pd.DataFrame(columns=square.string)
        for i in list1:
            df1=df[df['STATIONNUM']==i]
            o=o.append(df1)
        df1=o.drop_duplicates('RECDATETIME',keep='first')
        df1=square.sort_time(df1)
        df3=square.stop_order(num)
        df4=pd.merge(df1,df3,how='left',left_on='STATIONNUM',right_on='STATIONA')
        df4.to_csv('D:/anaconda/gpsdataraw/'+filename+'raw.csv')
        df4.to_csv('buses.csv')
        square.drop_duplicates('buses.csv','buses.csv',6)
        square.drop_duplicates('buses.csv','buses.csv',7)
        square.drop_duplicates('buses.csv','buses.csv',8)
        square.drop_duplicates('buses.csv','buses.csv',9)
        square.drop_duplicates('buses.csv','buses.csv',10)
        square.drop_duplicates('buses.csv','buses.csv',11)
        square.drop_duplicates('buses.csv','buses.csv',12)
        df2=pd.read_csv('buses.csv',usecols=square.string)
        df3=square.stop_order(num)
        df4=pd.merge(df2,df3,how='left',left_on='STATIONNUM',right_on='STATIONA')
        df4=df4.drop(['STATIONA'],axis=1)
        df4.to_csv('buses.csv')
        square.del_abnor('buses.csv','buses.csv')
        square.del_abnor('buses.csv','buses.csv')
        square.del_abnor('buses.csv','buses.csv')
        square.del_abnor('buses.csv','buses.csv')
        df2=pd.read_csv('buses.csv',usecols=square.string2)
        square.drop_duplicates('buses.csv','buses.csv',6)
        square.drop_duplicates('buses.csv','buses.csv',7)
        square.insert_items('buses.csv','buses.csv',num)
        square.insert_head('buses.csv','buses.csv',num)
        square.insert_tail('buses.csv','buses.csv',num)
        square.drop_duplicates('buses.csv','buses.csv',6)
        square.drop_duplicates('buses.csv','buses.csv',7)
        square.insert_items('buses.csv','buses.csv',num)
        square.del_abnor('buses.csv','buses.csv')
        df=pd.read_csv('buses.csv',usecols=square.string2)
        df.to_csv('buses.csv')
        return df
    
    def csv_reader(filename,route):
        '''输入原始数据，输出某一个班次的数据，支持大文本'''
        chunksize=10000
        df1=pd.read_csv(filename,names=square.string,iterator=True)
        loop=True
        b=pd.DataFrame(columns=square.string)
        while loop:
            try:
                a=df1.get_chunk(chunksize)
                a=a[a['ROUTEID']==route]
                b=b.append(a)
            except StopIteration:
                loop=False
        return b
            
    def main(datelist,linelist):
        '''输入每一天的原始数据文件名列表和线路列表，
        输出所有的到站时间清洁数据'''
        temp1=[]
        for i in datelist:
            for j in linelist:
                df1=square.csv_reader(i,j)
                productlist=square.split_bus(df1,j)
                for k in productlist:
                    df2=square.clear_data(k,j)
                    temp='D:/anaconda/gpsdata/'+i+str(j)+str(k)+'.csv'
                    df2.to_csv(temp)
                    temp1.append(temp)
        return temp1
                    
    
    def select_stop_file(filename,li):
        '''输入dataframe，输出选好的数据，
        该函数最好利用于li为两个输入的方式，分别为起终点'''
        data=pd.read_csv(filename,usecols=square.string2)
        b=pd.DataFrame(columns=square.string2)
        for i in li:
            df=data[data['STATIONNUM']==i]
            b=b.append(df)
        b=square.sort_time(b)
        return b
    
    def select_stop_file1(filename,num1,num2):
        '''输入dataframe，输出选好的数据，
        该函数最好利用于为两个输入的方式，分别为起终点'''
        data=pd.read_csv(filename,usecols=square.string2)
        df=data[(data['STATIONNUM']==num1)|(data['STATIONNUM']==num2)]
        return df

    def select_stop_data1(data,num1,num2):
        '''输入dataframe，输出选好的数据，
        该函数最好利用于li为两个输入的方式，分别为起终点'''
        df=data[(data['STATIONNUM']==num1)|(data['STATIONNUM']==num2)]
        return df
    
    def select_stop_data(data,li):
        '''输入dataframe，输出选好的数据，
        该函数最好利用于li为两个输入的方式，分别为起终点'''
        b=pd.DataFrame(columns=square.string2)
        for i in li:
            df=data[data['STATIONNUM']==i]
            b=b.append(df)
        b=square.sort_time(b)
        return b
        
    def minus_date(data):
        bdate=data.iloc[0,0]
        edate=data.iloc[-1,0]
        dt=np.datetime64(edate)-np.datetime64(bdate)
        dt=dt.item().total_seconds()
        dic={'STARTIME':[bdate],'TIME':[dt]}
        df=pd.DataFrame(dic)
        return df
    
    def minus_date1(data,num1,num2):
        b=pd.DataFrame(columns=square.string3)
        for i in range(0,data.iloc[:,0].size,2):
            b1=data.iloc[i:i+1,:][['RECDATETIME','ISARRLFT','STATIONNUM','ROUTEID','PRODUCTID']]
            b2=data.iloc[i+1:i+2,:][['RECDATETIME','ISARRLFT','STATIONNUM']]
            if (b1.empty)|(b2.empty):
                break
            else:
                bdate=b1.iloc[0,0]
                edate=b2.iloc[0,0]
                dt=np.datetime64(edate)-np.datetime64(bdate)
                dt=dt.item().total_seconds()
                if not((b1.iloc[0,1]==1)&(b2.iloc[0,1]==1)):
                    isa=10000
                else:
                    isa=1
                dic={'RECDATETIME':[bdate],'ISARRLFT':[isa],'TIME':[dt],'ROUTEID':[int(b1['ROUTEID'].values)],'PRODUCTID':[int(b1['PRODUCTID'].values)]}
                df=pd.DataFrame(dic)
                b=b.append(df)
        return b
    
    def main2(datelist,linelist,num1,num2):
        '''输入每一天的原始数据文件名列表和线路列表，
        输出所有的到站时间清洁数据'''
        temp1=[]
        for i in datelist:
            b=pd.DataFrame(columns=square.string3)
            for j in linelist:
                df1=square.csv_reader(i,j)
                productlist=square.split_bus(df1,j)
                for k in productlist:
                    temp='D:/anaconda/gpsdata1/'+i+str(j)+str(k)+'.csv'
                    print(temp)
                    df2=square.clear_data(k,j)
                    df3=square.select_stop_data1(df2,num1,num2)
                    if df3.empty:
                        continue
                    else:
                        df4=square.minus_date1(df3,num1,num2)                    
                        b=b.append(df4)
                        df2.to_csv(temp)
                        temp1.append(temp)
            b=square.sort_time(b)
            b.to_csv(i+'_the_outcome.csv')   
        return temp1  
       
    def select_neardate_file(filename,ignor_abnor=True,dat=0):
        data=pd.read_csv(filename,usecols=square.string3)
        b=pd.DataFrame(columns=square.string4)
        length=data.iloc[:,0].size
        if (ignor_abnor)&(dat==0):
            for i in range(length):
                if (i==0) or (i==1):
                    continue
                else:
                    b1=copy.copy(data.iloc[i-2:i-1,:])
                    b2=copy.copy(data.iloc[i-1:i,:])
                    b3=copy.copy(data.iloc[i:i+1,:])
                    time1=b1['TIME'].values[0]
                    recdatatime1=b1['RECDATETIME'].values[0]
                    routeid1=b1['ROUTEID'].values[0]
                    time2=b2['TIME'].values[0]
                    recdatatime2=b2['RECDATETIME'].values[0]
                    routeid2=b2['ROUTEID'].values[0]
                    time=b3['TIME'].values[0]
                    recdatatime=b3['RECDATETIME'].values[0]
                    routeid=b3['ROUTEID'].values[0]
                    if b1['ISARRLFT'].values[0]==1&b2['ISARRLFT'].values[0]==1&b3['ISARRLFT'].values[0]==1:
                        isarrlft=1
                    else:
                        isarrlft=10000
                    b0={'ISARRLFT':[isarrlft],'RECDATETIME1':[recdatatime1],'ROUTEID1':[routeid1],'TIME1':[time1],\
                        'RECDATETIME2':[recdatatime2],'ROUTEID2':[routeid2],'TIME2':[time2],\
                        'RECDATETIME':[recdatatime],'ROUTEID':[routeid],'TIME':[time]}
                    b0=pd.DataFrame(b0)
                    b=b.append(b0)
            return b
        if not(ignor_abnor)&(dat==0):
            data=data[data['ISARRLFT']==1]
            length=data.iloc[:,0].size
            for i in range(length):
                if (i==0) or (i==1):
                    continue
                else:
                    b1=copy.copy(data.iloc[i-2:i-1,:])
                    b2=copy.copy(data.iloc[i-1:i,:])
                    b3=copy.copy(data.iloc[i:i+1,:])
                    time1=b1['TIME'].values[0]
                    recdatatime1=b1['RECDATETIME'].values[0]
                    routeid1=b1['ROUTEID'].values[0]
                    time2=b2['TIME'].values[0]
                    recdatatime2=b2['RECDATETIME'].values[0]
                    routeid2=b2['ROUTEID'].values[0]
                    time=b3['TIME'].values[0]
                    recdatatime=b3['RECDATETIME'].values[0]
                    routeid=b3['ROUTEID'].values[0]
                    if b1['ISARRLFT'].values[0]==1&b2['ISARRLFT'].values[0]==1&b3['ISARRLFT'].values[0]==1:
                        isarrlft=1
                    else:
                        isarrlft=10000
                    b0={'ISARRLFT':[isarrlft],'RECDATETIME1':[recdatatime1],'ROUTEID1':[routeid1],'TIME1':[time1],\
                        'RECDATETIME2':[recdatatime2],'ROUTEID2':[routeid2],'TIME2':[time2],\
                        'RECDATETIME':[recdatatime],'ROUTEID':[routeid],'TIME':[time]}
                    b0=pd.DataFrame(b0)
                    b=b.append(b0)
            return b
        if not(ignor_abnor)&dat:
            length=data.iloc[:,0].size
            for i in range(length):
                if (i==0) or (i==1):
                    continue
                else:
                    b1=copy.copy(data.iloc[i-2:i-1,:])
                    b2=copy.copy(data.iloc[i-1:i,:])
                    b3=copy.copy(data.iloc[i:i+1,:])
                    if b3['ROUTEID'].values[1]==dat:
                        time1=b1['TIME'].values[0]
                        recdatatime1=b1['RECDATETIME'].values[0]
                        routeid1=b1['ROUTEID'].values[0]
                        time2=b2['TIME'].values[0]
                        recdatatime2=b2['RECDATETIME'].values[0]
                        routeid2=b2['ROUTEID'].values[0]
                        time=b3['TIME'].values[0]
                        recdatatime=b3['RECDATETIME'].values[0]
                        routeid=b3['ROUTEID'].values[0]
                        if b1['ISARRLFT'].values[0]==1&b2['ISARRLFT'].values[0]==1&b3['ISARRLFT'].values[0]==1:
                            isarrlft=1
                        else:
                            isarrlft=10000
                        b0={'ISARRLFT':[isarrlft],'RECDATETIME1':[recdatatime1],'ROUTEID1':[routeid1],'TIME1':[time1],\
                            'RECDATETIME2':[recdatatime2],'ROUTEID2':[routeid2],'TIME2':[time2],\
                            'RECDATETIME':[recdatatime],'ROUTEID':[routeid],'TIME':[time]}
                        b0=pd.DataFrame(b0)
                        b=b.append(b0)
                    else:
                        continue
            return b
        if (ignor_abnor)&dat:
            data=data[data['ISARRLFT']==1]
            length=data.iloc[:,0].size
            for i in range(length):
                if (i==0) or (i==1):
                    continue
                else:
                    b1=copy.copy(data.iloc[i-2:i-1,:])
                    b2=copy.copy(data.iloc[i-1:i,:])
                    b3=copy.copy(data.iloc[i:i+1,:])
                    if b3['ROUTEID'].values[0]==dat:
                        time1=b1['TIME'].values[0]
                        recdatatime1=b1['RECDATETIME'].values[0]
                        routeid1=b1['ROUTEID'].values[0]
                        time2=b2['TIME'].values[0]
                        recdatatime2=b2['RECDATETIME'].values[0]
                        routeid2=b2['ROUTEID'].values[0]
                        time=b3['TIME'].values[0]
                        recdatatime=b3['RECDATETIME'].values[0]
                        routeid=b3['ROUTEID'].values[0]
                        if b1['ISARRLFT'].values[0]==1&b2['ISARRLFT'].values[0]==1&b3['ISARRLFT'].values[0]==1:
                            isarrlft=1
                        else:
                            isarrlft=10000
                        b0={'ISARRLFT':[isarrlft],'RECDATETIME1':[recdatatime1],'ROUTEID1':[routeid1],'TIME1':[time1],\
                            'RECDATETIME2':[recdatatime2],'ROUTEID2':[routeid2],'TIME2':[time2],\
                            'RECDATETIME':[recdatatime],'ROUTEID':[routeid],'TIME':[time]}
                        b0=pd.DataFrame(b0)
                        b=b.append(b0)
                    else:
                        continue
                return b
    
    def divide_time(filename,start_time,end_time):
        df=pd.read_csv(filename)
        
    pass