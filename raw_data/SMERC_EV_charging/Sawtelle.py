#! /usr/bin/python

print "Content-type: text/html\n\n"
print "Upload EV Charging DATA"

print("<TITLE>CGI script output</TITLE>")
print("<H1>EV Charging Data_Sawtelle</H1>")


import cgitb
cgitb.enable()

import sys
import MySQLdb

from copy import deepcopy
from citadel import Citadel

import threading


#define the connection param
hostname='localhost'
username='xxxxxxxxxx'
password='xxxxxxxxxx'
dbname='xxxxxxxxx'
tbname='xxxxxxx'
station_A1="3f7ea6af-9a48-11e6-a3d4-001851942ca6"
station_A2="3f80a11a-9a48-11e6-a3d4-001851942ca6"
station_A3="3f81ece7-9a48-11e6-a3d4-001851942ca6"
station_A4="3f8333e6-9a48-11e6-a3d4-001851942ca6"
station_B1="3f847478-9a48-11e6-a3d4-001851942ca6"
station_B2="3f85b965-9a48-11e6-a3d4-001851942ca6"
station_B3="3f86fdfa-9a48-11e6-a3d4-001851942ca6"
station_B4="3f88414f-9a48-11e6-a3d4-001851942ca6"

port= 3306
charset='utf8'


base_url = 'https://citadel.ucsd.edu'

citadel = Citadel(base_url)


metadata_1 = {
        'name': 'EV_Charging_Sawtelle_A1',
        'tags': {
            'point_type':'electrical_power',
            'unit':'W',
            'source_reference': 'SMERC',

            },
        'geometry':{
            'coordinates':[-118.426232,34.021357],
            'type':'Point'
            }
        }

metadata_2 = deepcopy(metadata_1)
metadata_2['name'] = 'EV_Charging_Sawtelle_A2'

metadata_3 = deepcopy(metadata_1)
metadata_3['name'] = 'EV_Charging_Sawtelle_A3'

metadata_4 = deepcopy(metadata_1)
metadata_4['name'] = 'EV_Charging_Sawtelle_A4'

metadata_5 = deepcopy(metadata_1)
metadata_5['name'] = 'EV_Charging_Sawtelle_B1'

metadata_6 = deepcopy(metadata_1)
metadata_6['name'] = 'EV_Charging_Sawtelle_B2'

metadata_7 = deepcopy(metadata_1)
metadata_7['name'] = 'EV_Charging_Sawtelle_B3'

metadata_8 = deepcopy(metadata_1)
metadata_8['name'] = 'EV_Charging_Sawtelle_B4'


resA=None
resA2=None
resA3=None
resA4=None
resB1=None
resB2=None
resB3=None
resB4=None



def test_create_point(metadata):
    print('Init adding point test')
    res = citadel.create_point(metadata)
    if not res['success']:
        print(res['result']['reason'])
        assert(False)
    print('Done adding point test')

def _get_uuid_by_name(name):
    res = citadel.query_points(name=name)
    if res['success']:
        return res['result']['point_list'][0]['uuid']
    else:
        print(res['result']['reason'])
        assert(False)

def test_put_timeseries(metadata,ts_data):
    print('Init put timeseries test')
    uuid = _get_uuid_by_name(name=metadata['name'])
    print 'uuid:',uuid
    res = citadel.put_timeseries(uuid, ts_data)
    if not res['success']:
        print(res['result'])
        assert(False)
    print('Done put timeseries test')
    print'<br> <br/>'

def query():

    global resA,resA2,resA3,resA4,resB1,resB2,resB3,resB4

    try:
        conn = MySQLdb.connect(
            host=hostname,
            user=username,
            passwd=password,
            db=dbname,
            port=port,
            charset=charset,
        )

        cursor = conn.cursor()
        cursor.execute("""
                SELECT * FROM %s WHERE entID='%s' ORDER BY timestamp DESC LIMIT 3
            """ % (tbname, station_A1))
        resA = cursor.fetchall()

        cursor.execute("""
                    SELECT * FROM %s WHERE entID='%s' ORDER BY timestamp DESC LIMIT 3
                """ % (tbname, station_A2))
        resA2 = cursor.fetchall()

        cursor.execute("""
                        SELECT * FROM %s WHERE entID='%s' ORDER BY timestamp DESC LIMIT 3
                    """ % (tbname, station_A3))
        resA3 = cursor.fetchall()

        cursor.execute("""
                        SELECT * FROM %s WHERE entID='%s' ORDER BY timestamp DESC LIMIT 3
                    """ % (tbname, station_A4))
        resA4 = cursor.fetchall()

        cursor = conn.cursor()
        cursor.execute("""
                    SELECT * FROM %s WHERE entID='%s' ORDER BY timestamp DESC LIMIT 3
                """ % (tbname, station_B1))
        resB1 = cursor.fetchall()

        cursor.execute("""
                        SELECT * FROM %s WHERE entID='%s' ORDER BY timestamp DESC LIMIT 3
                    """ % (tbname, station_B2))
        resB2 = cursor.fetchall()

        cursor.execute("""
                            SELECT * FROM %s WHERE entID='%s' ORDER BY timestamp DESC LIMIT 3
                        """ % (tbname, station_B3))
        resB3 = cursor.fetchall()

        cursor.execute("""
                            SELECT * FROM %s WHERE entID='%s' ORDER BY timestamp DESC LIMIT 3
                        """ % (tbname, station_B4))
        resB4 = cursor.fetchall()

        # close curs
        cursor.close()
        # close connection
        conn.close()
        # Last_update_5 = datetime.datetime.fromtimestamp(int(res[0][1])).strftime('%Y-%m-%d %H:%M:%S')


    except MySQLdb.Error, e:
        print
        'mysql error %d:%s', (e.args[0], e.args[1])
        sys.exit()



def live():

    query()

    for t in range(3):

        test_ts_data = {resA[t][1]: resA[t][3]}
        test_ts_data_2 = {resA2[t][1]: resA2[t][3]}
        test_ts_data_3 = {resA3[t][1]: resA3[t][3]}
        test_ts_data_4 = {resA4[t][1]: resA4[t][3]}
        test_ts_data_5 = {resB1[t][1]: resB1[t][3]}
        test_ts_data_6 = {resB2[t][1]: resB2[t][3]}
        test_ts_data_7 = {resB3[t][1]: resB3[t][3]}
        test_ts_data_8 = {resB4[t][1]: resB4[t][3]}


        try:
            print'A1:'
            test_put_timeseries(metadata_1, test_ts_data)

        except:
            print 'A1 error'

        try:
            print'A2:'
            test_put_timeseries(metadata_2, test_ts_data_2)

        except:
            print 'A2 error'

        try:
            print 'A3:'
            test_put_timeseries(metadata_3, test_ts_data_3)

        except:
            print 'A3 error'

        try:
            print 'A4:'
            test_put_timeseries(metadata_4, test_ts_data_4)

        except:
            print 'A4 error'

        try:
            print 'B1:'
            test_put_timeseries(metadata_5, test_ts_data_5)

        except:
            print 'B1 error'

        try:
            print 'B2:'
            test_put_timeseries(metadata_6, test_ts_data_6)

        except:
            print 'B2 error'

        try:
            print'B3:'
            test_put_timeseries(metadata_7, test_ts_data_7)

        except:
            print 'B3 error'

        try:
            print'B4:'
            test_put_timeseries(metadata_8, test_ts_data_8)

        except:
            print 'B4 error'

    threading.Timer(300, live).start()

live()



