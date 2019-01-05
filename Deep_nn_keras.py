# -*- coding: utf-8 -*-
"""
Created on Sat Jan 27 16:40:59 2018

@author: Administrator
"""

import keras
import numpy as np
from keras.models import Sequential
from keras.layers import Dense, Activation,Dropout
from sklearn.model_selection import train_test_split
from keras.wrappers.scikit_learn import KerasClassifier
from sklearn import preprocessing
from sklearn.svm import SVC
from keras.optimizers import SGD,Adagrad,Adadelta,RMSprop,Adam
import pandas as pd
from sklearn.metrics import accuracy_score

def convert_1(data):   
    ro,co=data.shape
    data=data.tolist()
    for i in data:
        ma=i.index(max(i))
        for j in range(co):
            if j==ma:
                i[j]=1
            else:
                i[j]=0
    return np.array(data)

string=['Gender','age','salary','occuption','originlanduse','starttime','tripmode','destinationlanduse','arrivingtime','activityduration']
inputfile='wentout_o.xlsx'   #excel输入
x=pd.read_excel(inputfile,usecols=string)
y=pd.read_excel(inputfile,usecols=['Trippurpose']) #pandas以DataFrame的格式读入excel表
x=x.values#转化为矩阵
y=y.values
x=preprocessing.scale(x,axis=0)#对输入数据进行第二范式的转换
y=keras.utils.to_categorical(y,num_classes=6)#将输出转化成one-hot标签形式，6类，输出为1-5有一个是没用的
x_train,x_test,y_train,y_test=train_test_split(x,y,test_size=0.2,random_state=1)#训练集和测试集划分
'''
model=SVC()
model.fit(x_train,y_train)
a=model.predict(x_test)
print(accuracy_score(y_test,model.predict(x_test)))
'''
model = Sequential()  #层次模型
model.add(Dense(50,input_dim=10,activation='relu')) #输入层，Dense表示BP层
model.add(Dense(50,activation='relu'))
model.add(Dropout(0.3))#dropout防止过拟合
model.add(Dense(50,activation='relu'))
model.add(Dropout(0.3))#dropout防止过拟合
model.add(Dense(50,activation='relu'))
model.add(Dropout(0.3))#dropout防止过拟合
model.add(Dense(50,activation='relu'))
model.add(Dropout(0.3))#dropout防止过拟合
model.add(Dense(50,activation='relu'))
model.add(Dropout(0.3))#dropout防止过拟合
model.add(Dense(6,activation='softmax'))  #输出层

model.compile(loss='categorical_crossentropy', optimizer='sgd',metrics=['accuracy']) #编译模型
model.fit(x_train, y_train, nb_epoch =1, batch_size=10) #训练模型1000次
data=model.predict(x_test)
d=convert_1(data)
print(accuracy_score(y_test,d))
        
        