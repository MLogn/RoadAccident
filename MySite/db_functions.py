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

    # when all stats defined for user except area
    def allstats(self, user_id):

        query = 'select count(*) from user_info, vehicle, casualty where user_info.userid =\'' + user_id + '\'and ' \
                'casualty.acc_index = vehicle.acc_index and vehicle.veh_type = user_info.veh_type and ' \
                'vehicle.vehicle_age = user_info.vehicle_age and user_info.gender = casualty.casualty_sex and ' \
                'user_info.age = casualty.casualty_age;'
        self.cur.execute(query)
        res = self.cur.fetchall()

        return (res[0][0] / self.all_casualties) * 100

    # when user age and veh_type defined

    def UsrAge(self, user_id):

        query = 'select count(*) from user_info join casualty on casualty.casualty_age = user_info.age' \
                ' where user_info.userid =\'' + user_id + '\';'

        self.cur.execute(query)
        res = self.cur.fetchall()

        return (res[0][0] / self.all_casualties) * 100

    def UsrGender(self, user_id):

        query = 'select count(*) from user_info join casualty on casualty.casualty_sex = user_info.gender' \
                ' where user_info.userid =\'' + user_id + '\';'

        self.cur.execute(query)
        res = self.cur.fetchall()

        return (res[0][0] / self.all_casualties) * 100

    def VehicleType(self, user_id):

        query = 'select count(*) from user_info join vehicle on vehicle.veh_type = user_info.veh_type' \
                ' where user_info.userid =\'' + user_id + '\';'

        self.cur.execute(query)
        res = self.cur.fetchall()

        return (res[0][0] / self.all_casualties) * 100

    def VehicleAge(self, user_id):

        query = 'select count(*) from user_info join vehicle on vehicle.vehicle_age = user_info.vehicle_age' \
                ' where user_info.userid =\'' + user_id + '\';'

        self.cur.execute(query)
        res = self.cur.fetchall()

        return (res[0][0] / self.all_casualties) * 100

    def AgeAndVehicle(self, user_id):

        query = 'select count(*) from user_info, vehicle, casualty where user_info.userid =\'' + user_id + '\'and ' \
                'casualty.acc_index = vehicle.acc_index and vehicle.veh_type = user_info.veh_type' \
                ' and casualty.casualty_age = user_info.age;'
        self.cur.execute(query)
        res = self.cur.fetchall()
        return (res[0][0] / self.all_casualties) * 100

    # when gender and vehicle_type defined
    def GenderAndVehicle(self, user_id):

        query = 'select count(*) from user_info, vehicle, casualty where user_info.userid =\'' + user_id + '\'and ' \
                'casualty.acc_index = vehicle.acc_index and vehicle.veh_type = user_info.veh_type' \
                ' and casualty.casualty_sex = user_info.gender;'
        self.cur.execute(query)
        res = self.cur.fetchall()

        return (res[0][0] / self.all_casualties) * 100

    # when user age and vechicle_age defined
    def AgeAndVechAge(self, user_id):

        query = 'select count(*) from user_info, vehicle, casualty where user_info.userid =\'' + user_id + '\'and ' \
                'casualty.acc_index = vehicle.acc_index and user_info.vehicle_age = vehicle.vehicle_age' \
                ' and casualty.casualty_age = user_info.age;'
        self.cur.execute(query)
        res = self.cur.fetchall()

        return (res[0][0] / self.all_casualties) * 100

    # when user age and user gender defined
    def AgeAndGender(self, user_id):

        query = 'select count(*) from user_info, casualty where user_info.userid =\'' + user_id + '\'and ' \
                ' casualty.casualty_sex = user_info.gender and casualty.casualty_age = user_info.age;'

        self.cur.execute(query)
        res = self.cur.fetchall()
        return (res[0][0] / self.all_casualties) * 100

    def GenderAndVehicleAge(self, user_id):

        query = 'select count(*) from user_info, vehicle, casualty where user_info.userid =\'' + user_id + '\'and ' \
                ' casualty.acc_index = vehicle.acc_index and user_info.gender = casualty.casualty_sex' \
                ' and user_info.vehicle_age = vehicle.vehicle_age;'

        self.cur.execute(query)
        res = self.cur.fetchall()
        return (res[0][0] / self.all_casualties) * 100

    def AgeAndGenderAndVehicleAge(self, user_id):

        query = 'select count(*) from user_info, vehicle, casualty where user_info.userid =\'' + user_id + '\'and ' \
                'casualty.acc_index = vehicle.acc_index and user_info.gender = casualty.casualty_sex and ' \
                'user_info.vehicle_age = vehicle.vehicle_age and casualty.casualty_age = user_info.age;'
        self.cur.execute(query)
        res = self.cur.fetchall()
        return (res[0][0] / self.all_casualties) * 100

    def AgeAndVehicleAndVehicleAge(self, user_id):

        query = 'select count(*) from user_info, vehicle, casualty where user_info.userid =\'' + user_id + '\'and ' \
                'casualty.acc_index = vehicle.acc_index and user_info.vehicle_age = vehicle.vehicle_age and ' \
                'user_info.veh_type = vehicle.veh_type and casualty.casualty_age = user_info.age;'
        self.cur.execute(query)
        res = self.cur.fetchall()

        return (res[0][0] / self.all_casualties) * 100

    def AgeAndVehTypeAndGender(self, user_id):

        query = 'select count(*) from user_info, vehicle, casualty where user_info.userid =\'' + user_id + '\'and ' \
                'casualty.acc_index = vehicle.acc_index and vehicle.veh_type = user_info.veh_type and ' \
                'casualty.casualty_age = user_info.age and casualty.casualty_sex = user_info.gender;'

        self.cur.execute(query)
        res = self.cur.fetchall()

        return (res[0][0] / self.all_casualties) * 100

    def GenderandVehTypeAndVechAge(self, user_id):

        query = 'select count(*) from user_info, vehicle, casualty where user_info.userid =\'' + user_id + '\'and ' \
                'casualty.acc_index = vehicle.acc_index and vehicle.veh_type = user_info.veh_type and ' \
                'vehicle.vehicle_age = user_info.vehicle_age and user_info.gender = casualty.casualty_sex;'

        self.cur.execute(query)
        res = self.cur.fetchall()

        return (res[0][0] / self.all_casualties) * 100

    def __init__(self):
        self.connection = MySQLdb.connect(user="DreamTeam",
                                          passwd="1234",
                                          host="localhost",
                                          db="RoadAccidentsDB", )
        self.cur = self.connection.cursor()

        query = 'select count(accidents.acc_index) from accidents'
        self.cur.execute(query)
        self.all_casualties = self.cur.fetchall()[0][0]

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
                if field not in user_info_structure:
                    continue
                query += field + '=\''
                query += str(value) + '\', '
            query = query[:-2] + ' where userid=\'' + user_id + '\';'
        else:
            query = 'insert into user_info '
            fields = '(userid, '
            values = 'values(\'' + user_id + '\', '
            for field, value in data.items():
                if field not in user_info_structure:
                    continue
                fields += field + ', '
                values += '\'' + str(value) + '\', '
            query += fields[:-2] + ') ' + values[:-2] + ');'
        self.cur.execute(query)
        self.connection.commit()


a = DbFunctions()
res = a.add_user_info('asd123', {'vehicle_age' : 7, 'gender': 1, 'area': '123', 'age' : 27, 'veh_type' : 3})
percents = []
percents.append(a.allstats('asd123'))
percents.append(a.UsrAge('asd123'))
percents.append(a.UsrGender('asd123'))
percents.append(a.VehicleType('asd123'))
percents.append(a.VehicleAge('asd123'))
percents.append(a.AgeAndVehicle('asd123'))
percents.append(a.GenderAndVehicle('asd123'))
percents.append(a.AgeAndVechAge('asd123'))
percents.append(a.AgeAndGender('asd123'))
percents.append(a.GenderAndVehicleAge('asd123'))
percents.append(a.AgeAndGenderAndVehicleAge('asd123'))
percents.append(a.AgeAndVehicleAndVehicleAge('asd123'))
percents.append(a.AgeAndVehTypeAndGender('asd123'))
percents.append(a.GenderandVehTypeAndVechAge('asd123'))

for i in percents:
    print(i, "%\n")
