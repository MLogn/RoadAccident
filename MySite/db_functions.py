import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import MySQLdb
from scipy.spatial import distance
from dateutil.parser import parse
import time
import sys

user_info_structure = ['userid',
                       'veh_type',
                       'gender',
                       'area',
                       'age',
                       'vehicle_age']
accidents_structure = ['acc_index',
                       'place',
                       'time',
                       'date',
                       'road_type',
                       'accident_severity',
                       'road_class',
                       'vehicles_number',
                       'casualities_number']
casualty_structure = ['acc_index',
                      'casualty_type',
                      'casualty_class',
                      'casualty_sex',
                      'casualty_age',
                      'car_passenger']
vehicle_structure = ['acc_index',
                     'veh_type',
                     'driver_sex',
                     'driver_age',
                     'vehicle_age']

mapper_dict = {
    'veh_type': [
        'Pedal cycle',
        'Motorcycle',
        'Taxi',
        'Car',
        'Minibus',
        'Bus',
        'Tram',
        'Van',
        'Scooter',
        'Electric motorcycle'
    ],
    'gender': [
        'Male',
        'Female'
    ]
}


def get_string(field, code):
    return mapper_dict[field][code]


def get_code(field, value):
    return mapper_dict[field].index(value)


class DbFunctions():

    def allstat(self, user_id, data):

        query = 'select count(*) from user_info, vehicle, casualty where user_info.userid =\'' + user_id + '\'and ' \
                'casualty.acc_index = vehicle.acc_index and vehicle.veh_type = user_info.veh_type and ' \
                'vehicle.vehicle_age = user_info.vehicle_age and user_info.gender = casualty.casualty_sex and ' \
                'user_info.age = casualty.casualty_age;'
        self.cur.execute(query)
        data = self.cur.fetchall()


    def __init__(self):
        self.connection = MySQLdb.connect(user="DreamTeam",
                                          passwd="1234",
                                          host="localhost",
                                          db="RoadAccidentsDB", )
        self.cur = self.connection.cursor()

    def show_user_info(self, user_id):
        query = 'select * from user_info where userid=\'' + user_id + '\';'
        self.cur.execute(query)
        data = self.cur.fetchall()
        res = {}
        for i in range(len(data)):
            if data[i] is not None:
                res[user_info_structure[i]] = data[i]
        return res

    def add_user_info(self, user_id, data):
        # check if user exists

        check_user_query = 'select * from user_info where userid=\'' + user_id + '\';'
        self.cur.execute(check_user_query)
        info = self.cur.fetchall()
        if len(info) != 0:
            query = 'update user_info set '
            for field, value in data.items():
                if field not in user_info_structure or value is None:
                    continue

                query += field + '=\'' + get_code(field, value)
                query += str(value) + '\', '
            query = query[:-2] + ' where userid=\'' + user_id + '\';'
        else:
            query = 'insert into user_info '
            fields = '(userid, '
            values = 'values(\'' + user_id + '\', '
            for field, value in data.items():
                if field not in user_info_structure or value is None:
                    continue
                fields += field + ', '
                values += '\'' + str(value) + '\', '
            query += fields[:-2] + ') ' + values[:-2] + ');'
        self.cur.execute(query)
        self.connection.commit()


a = DbFunctions()
res = a.add_user_info('asd123', {'gender': 1, 'area': '123'})
res = a.add_user_info('dsa321', {'vehicle_age': 125})
print(res)
