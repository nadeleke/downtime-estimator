import sys
import ast
import csv
import time
import json
import random
import datetime
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer


# --------------------------------
# Creating Fields
# --------------------------------
def readFieldData(file_name):
    # Loading field info
    fields = {}
    with open(file_name) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            fields[row['index']] = [row['field_name'], row['state'], row['size_(MM_Bbls)']]
            # print(row['index'], row['field_name'], row['state'], row['size_(MM_Bbls)'])
    # print(fields)
    return fields


# --------------------------------
# Creating unique wells
# --------------------------------
def genWellInfo(fields, numOfWells):
    well_info = {}

    while len(well_info) < numOfWells:
        wellID = 0.0 + random.randrange(100000, numOfWells+300000)  # 1E6)
        # wellID = 0.0 + random.randrange(100, numOfWells + 120)  # 1E6)

        if wellID not in well_info:
            # Assigning a random completion type
            r = random.randrange(0, len(completion_types))
            completion_type = completion_types[r]
            # Assigning completion type properties
            if completion_type == 'BP':
                pay_height = random.choice([100, 120])  # ft
                # rate_prod = random.randrange(100, 200)  # BBls/d
                pump_rate = random.randrange(50000, 500000)  # Bbls/s
            elif completion_type == 'ESP':
                pay_height = random.randrange(120, 300)  # ft
                # rate_prod = random.randrange(150, 350)  # Bbls/d
                pump_rate = random.randrange(0.7E6, 15E6)  # Bbls/s

            # Assigning random well diameter
            well_diam = random.randrange(2, 7)  # in inches
            well_diam = well_diam * 0.0833333  # ft -- converting inches to ft

            # Calculating well volume
            wellbore_vol = 3.14/4.0 * well_diam * well_diam * pay_height / 5.61458333  # Bbls

            # Assigning a random field to this wellID
            fieldID = str(random.randrange(1, len(fields) + 1))

            # Assigning field specific data to this wellID
            field_name = fields[fieldID][0]
            state = fields[fieldID][1]
            size = fields[fieldID][2]

            well_info[wellID] = [fieldID, completion_type, wellbore_vol, field_name, state, size, pump_rate]

    # print('Well info =', well_info)
    return well_info


# --------------------------------------
# Assigning initial well status randomly
# --------------------------------------
def genWellStatus(well_info):
    well_stat = {}
    for wellID in well_info:
        event = event_types[random.randrange(0, len(event_types))]
        well_stat[wellID] = event

    # print('well_stat =', well_stat)
    return well_stat


# -------------------------------------------
# This function generates the next time stamp
# -------------------------------------------
def nextTimeStamp(dt, maxStep):
    # Generating random datetime step (on second scale)
    r = random.randrange(max(1, max_step-10), maxStep)
    step = datetime.timedelta(seconds=r)
    return dt + step


# ---------------------------------------------
# Generating single event data
# ---------------------------------------------
def genEvent(dt, wellID, well_stat, well_info, last_event_data=None):

    fieldID = well_info[wellID][0]
    completion_type = well_info[wellID][1]
    wellbore_vol = well_info[wellID][2]
    field_name = well_info[wellID][3]
    state = well_info[wellID][4]
    size = well_info[wellID][5]
    pump_rate = well_info[wellID][6]

    if well_stat[wellID] is event_types[0]:
        event = event_types[1]
        well_stat[wellID] = event_types[1]
    else:
        event = event_types[0]
        well_stat[wellID] = event_types[0]

    if event == 'on':  # no production prior to event
        vol_prod = 0
    else:  # event = 'off' so determine production
        if last_event_data is not None:
            time0 = ast.literal_eval(last_event_data[1])['time']
            # time0 = json.loads(last_event_data[1])['time']
            # print(last_event_data)
            # print(last_event_data[1])
            # print(ast.literal_eval(last_event_data[1])['time'])
            # input('pause')

            vol_prod = (dt - datetime.datetime.strptime(str(time0), "%Y-%m-%d %H:%M:%S")).total_seconds() * pump_rate  # Bbls
            # minimum production
            if vol_prod < 0.7 * wellbore_vol:
                vol_prod = 0.7 * wellbore_vol
        else:
            vol_prod = 0.7 * wellbore_vol  # wellbore_vol

    # percent of wellbore volume producible
    producible_wv = 0.7 * wellbore_vol  # Bbls

    # str_fmt = "{};{};{};{};{};{};{};{};{};{}"
    # reform_event_data = str_fmt.format(str(dt), str(wellID), event, vol_prod, fieldID, completion_type, wellbore_vol, field_name, state, size)

    reform_event_data = dict(id=str(wellID), time=str(dt), status=event, vp=vol_prod, vw=producible_wv, \
                             fieldID=fieldID, field_name=field_name, comp=completion_type, rop=pump_rate, \
                             state=state, size=int(size))

    return well_stat, (wellID, str(reform_event_data))


def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d %H:%M:%S")
    d2 = datetime.strptime(d2, "%Y-%m-%d %H:%M:%S")
    return abs((d2 - d1).seconds)


if __name__ == "__main__":

    args = sys.argv
    ip_address = str(args[1])
    partition_key = str(args[2])

    print(args, '\n', 'ip address -->', ip_address)
    # input('p')

    client = SimpleClient(ip_address)
    producer = KeyedProducer(client)

    # --------------------------------
    # Defining data variables
    # --------------------------------
    dt = datetime.datetime(2017, 1, 22, 9, 53, 42)  # Defining a starting date and time
    max_step = 20  # Time step max increment size (it's a random fxn btw 0 and max_step)
    numOfWells = 100000  # Number of wells
    # n = 700  # Number of event data points

    # --------------------------------
    # Generating time series data
    # --------------------------------
    event_types = ['on', 'off']
    completion_types = ['BP', 'ESP']  # diff from 'completion_type' (no s)

    # --------------------------------
    # creating instance of producer and generating data
    # --------------------------------

    # Loading field Data
    fields = readFieldData('field.csv')

    # Generating Well and Event Data
    well_info = genWellInfo(fields, numOfWells)
    well_stat = genWellStatus(well_info)

    # Seperating wells by fields
    wells_by_field = {}
    for i in range(1, len(fields) + 1):
        wells_by_field[i] = []

    for wellID in well_info:
        for i in range(1, len(fields)+1):
            if well_info[wellID][3] == fields[str(i)][0]:
                wells_by_field[i].append(wellID)


    # Determining max number of wells in a field
    max_n_wells = 0
    for i in range(1, len(fields) + 1):
        n = len(wells_by_field[i])
        if n > max_n_wells:
            max_n_wells = n
        print(n, 'wells in', fields[str(i)][0], 'field')


    # last event data
    last_event_data = {}  # or load data from file
    for wellID in well_info:
        last_event_data[wellID] = None

    # ----------------------------------
    # Generating time series event data
    # ----------------------------------
    cycle_count = 0
    # well range for j in range (jf, jfn)
    jf = {}     # field index
    jfn = {}    # field index
    for i in range(1, len(fields) + 1):
        jfn[i], jf[i] = 0, 0

    # while dt < datetime.datetime(2017, 1, 22, 9, 53, 42):
    while True:

        # Wait for 1 second
        time.sleep(1)

        dt = nextTimeStamp(dt, max_step)  # increase max_step to extend

        for i in range(1, len(fields) + 1):  # for every new timestamp, a random number of wells per field have an event

            r = random.randrange( 1, max(2, int(int(fields[str(i)][2]) / .1) ) )
            jfn[i] = jf[i] + r

            if jfn[i] > len(wells_by_field[i]):
                jfn[i] = len(wells_by_field[i])

        wells_per_cycle = 0
        for i in range(1, len(fields) + 1):  # field loop

            for j in range(jf[i], jfn[i]):  # next set of wells in the field loop
                # time.sleep(.2)
                wells_per_cycle += 1

                if len(wells_by_field[i]) != 0 :

                    # print('here', i, j, jf[i], jfn[i])

                    wellID = wells_by_field[i][j]

                    # print('b4=', well_stat[wellID])
                    # print('lastwelldata = ', last_event_data[wellID] )

                    # Generate event
                    well_stat, event = genEvent(dt, wellID, well_stat, well_info, last_event_data[wellID])

                    # Update last event dictionary
                    last_event_data[wellID] = event

                    # Generating historical data
                    # with open('historical_data.txt', 'a') as f:
                    #     f.write(str(event)+'\n')

                    # print(str(event))
                    # print('after=',  well_stat[wellID])
                    producer.send_messages('event_data_topic', str(partition_key).encode(), str(event).encode())

                    if jfn[i] >= len(wells_by_field[i]):
                        # print('here2', i, j, jf[i], jfn[i])
                        # print(jf[i], jfn[i])
                        # input('pause')
                        jfn[i] = 0
                        jf[i] = 0

            jf[i] = jfn[i]
            # input('pause')

        # print(wells_per_cycle, ' wells in this batch', )
        # print('------------------------------------------------------------------------------------------')

        cycle_count += 1
