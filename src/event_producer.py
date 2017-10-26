import sys
import ast
import csv
import time
import random
import datetime
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

# --------------------------------
# Constants & Conversion factors
# --------------------------------
pi = 3.14
ftPerInch = 0.0833333
ft3PerBbl = 5.61458333


# --------------------------------
# Creating Fields
# --------------------------------
def readFieldData(file_name):
    # Loading field info
    fields = {}
    with open(file_name) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            fields[row['index']] = [row['field_name'], row['state'], row['size_(MM_Bbls)'], row['lat'], row['lng']]
    # print(fields)
    return fields


# --------------------------------
# Creating unique wells
# --------------------------------
def genWellInfo(fields, numOfWells):
    well_info = {}

    while len(well_info) < numOfWells:
        wellID = 0.0 + random.randrange(100000, numOfWells + 300000)  # 1E6)

        if wellID not in well_info:
            # Assigning a random completion type
            r = random.randrange(0, len(completion_types))
            completion_type = completion_types[r]
            # Assigning completion type properties
            if completion_type == 'BP':
                pay_height = random.choice([120, 160])  # ft
                pump_rate = random.randrange(.3E3, 1.7E3) / 60. / 60. / 12.  # Bbls/s
            elif completion_type == 'ESP':
                pay_height = random.randrange(150, 300)  # ft
                pump_rate = random.randrange(1.5E3, .5E4) / 60. / 60. / 12.  # Bbls/s

            # Assigning random well diameter
            well_diam = random.randrange(3, 7)  # in inches
            well_diam = well_diam * ftPerInch  # ft -- converting inches to ft

            # Calculating well volume
            wellbore_vol = pi / 4.0 * well_diam * well_diam * pay_height / ft3PerBbl  # Bbls

            # Assigning a random field to this wellID
            fieldID = str(random.randrange(1, len(fields) + 1))

            # Assigning field specific data to this wellID
            field_name = fields[fieldID][0]
            state = fields[fieldID][1]
            size = fields[fieldID][2]
            field_lat = ast.literal_eval(fields[fieldID][3])
            field_lng = ast.literal_eval(fields[fieldID][4])

            # Define well latitude and longitude
            r_lat = random.randrange(1, 10000) / 10000
            r_lng = random.randrange(1, 10000) / 10000
            r_sign_lat = random.choice([-1, 1])
            r_sign_lng = random.choice([-1, 1])
            well_lat = float(field_lat) + r_sign_lat * r_lat
            well_lng = float(field_lng) + r_sign_lng * r_lng

            well_info[wellID] = [fieldID, completion_type, wellbore_vol, field_name, state, size, pump_rate, well_lat,
                                 well_lng]

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
    r = random.randrange(max(1, max_step - 10), maxStep)
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
    lat = well_info[wellID][7]
    lng = well_info[wellID][8]

    if well_stat[wellID] is event_types[0]:
        event = event_types[1]
        well_stat[wellID] = event_types[1]
    else:
        event = event_types[0]
        well_stat[wellID] = event_types[0]

    if event == 'on':  # no production prior to event
        vol_prod = 0
    else:  # event = 'off' so determine production
        if completion_type == 'BP':
            vol_prod = random.randrange(60, 100)  # Bbls
        else:
            vol_prod = random.randrange(120, 300)  # Bbls

            # if last_event_data:
            #     time0 = ast.literal_eval(last_event_data[1])['time']
            #     # time0 = json.loads(last_event_data[1])['time']
            #     # print(last_event_data)
            #     # print(last_event_data[1])
            #     print(ast.literal_eval(last_event_data[1])['time'])
            #     delta_t = (dt - datetime.datetime.strptime(str(time0), "%Y-%m-%d %H:%M:%S")).total_seconds()
            #     delta_t = 1 if delta_t < 1 else delta_t
            #     # Produced fluid volume
            #     vol_prod = delta_t * pump_rate  # Bbls
            #     # minimum production
            #     if vol_prod < 0.7 * wellbore_vol:
            #         vol_prod = 0.7 * wellbore_vol
            # else:
            #     vol_prod = 0.7 * wellbore_vol  # wellbore_vol

    # Percent of wellbore volume producible
    producible_wv = 0.7 * wellbore_vol  # Bbls

    reform_event_data = dict(id=str(wellID), time=str(dt), status=event, vp=vol_prod, vw=producible_wv, \
                             fieldID=fieldID, field_name=field_name, comp=completion_type, rop=pump_rate, \
                             state=state, size=int(size), lat=lat, lng=lng)

    return well_stat, (wellID, str(reform_event_data))


def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d %H:%M:%S")
    d2 = datetime.strptime(d2, "%Y-%m-%d %H:%M:%S")
    return abs((d2 - d1).seconds)


if __name__ == "__main__":

    args = sys.argv
    ip_address = str(args[1])
    partition_key = str(args[2])

    client = SimpleClient(ip_address)
    producer = KeyedProducer(client)

    # --------------------------------
    # Defining data variables
    # --------------------------------
    dt = datetime.datetime(2017, 10, 9, 1, 59, 59)  # Defining a starting date and time
    max_step = 600  # max time step size
    numOfWells = 50000  # Number of wells

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

    # Separating wells by fields
    wells_by_field = {}
    for i in range(1, len(fields) + 1):
        wells_by_field[i] = []

    for wellID in well_info:
        for i in range(1, len(fields) + 1):
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
    jf = {}  # field index
    jfn = {}  # field index
    for i in range(1, len(fields) + 1):
        jfn[i], jf[i] = 0, 0

    while True:

        time.sleep(1)

        dt = nextTimeStamp(dt, max_step)  # increase max_step to extend

        for i in range(1, len(fields) + 1):  # for every new timestamp, a random number of wells per field have an event

            r = random.randrange(1, max(2, int(int(fields[str(i)][2]) / .5)))
            jfn[i] = jf[i] + r
            if jfn[i] > len(wells_by_field[i]):
                jfn[i] = len(wells_by_field[i])

        wells_per_cycle = 0
        for i in range(1, len(fields) + 1):  # field loop

            if len(wells_by_field[i]) != 0:

                for j in range(jf[i], jfn[i]):  # next set of wells in the field loop

                    wells_per_cycle += 1

                    wellID = wells_by_field[i][j]

                    # Generate event
                    well_stat, event = genEvent(dt, wellID, well_stat, well_info, last_event_data[wellID])

                    # Update last event dictionary
                    last_event_data[wellID] = event

                    print(str(event))
                    # print('after=',  well_stat[wellID])

                    # Send message to kafka queue
                    producer.send_messages('event_data_topic', str(partition_key).encode(), str(event).encode())

                    if jfn[i] >= len(wells_by_field[i]):
                        jfn[i] = 0
                        jf[i] = 0

                jf[i] = jfn[i]

        cycle_count += 1
