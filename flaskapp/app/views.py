import ast
import redis
from app import app
from flask import Flask, render_template, request, jsonify

@app.route('/')
@app.route('/index')
def index():
  return render_template("index.html")
 
@app.route('/estimator')
def estimator():
  return  render_template("estimator.html", title = 'estimator')

@app.route('/wellsearch')
def wellFinder():
	dns = 'ec2-34-234-14-219.compute-1.amazonaws.com'
	well_id = request.args.get('well_id')
	# well_id = str(well_id)
	hist_map = redis.StrictRedis(host=dns, port=6379, db=0, decode_responses=True)
	
	# Particular well
	well_data = hist_map.hgetall(well_id)
	if well_data:
		lat = well_data['lat']
		lng = well_data['lng']
		comp = well_data['comp']
		status = well_data['status']
		if ast.literal_eval(well_data['vp']): 
			vp = ast.literal_eval(well_data['vp'])[-1]
		else:
			vp = -888
		if ast.literal_eval(well_data['time']):
			time = ast.literal_eval(well_data['time'])[-1]
		time_n = well_data['time_n']
		field = well_data['field']
		if status == 'on':
			message = r'Well ID: {0}<br>Well Status: {1}<br>Completion Type: {2}<br>Last Event Timestamp: {3}<br>Oil volume produced last cycle: {4:.2f} Bbls'.format(well_id, status, comp, time, vp)
		else:
			message = r'Well ID: {0}<br>Well Status: {1} (Predicted start up time: {2})<br>Last Event Timestamp: {3}<br>Completion Type: {4}<br>Oil volume produced last cycle: {5:.2f} Bbls'.format(well_id, status, time_n, time, comp, vp)
		well_result = dict(field=field,lat=lat,lng=lng,status=status,message=message)

	# # Surrounding wells
	# count=0
	# if well_data:
	# 	for key in hist_map.keys():
	# 		if hist_map.hgetall(key)['fieldID'] == well_result['field']:
	# 			count += 1
	# 			well_data = hist_map.hgetall(key)
	# 			lat = well_data['lat']
	# 			lng = well_data['lng']
	# 			comp = well_data['comp']
	# 			status = well_data['status']
	# 			if ast.literal_eval(well_data['vp']): 
	# 				vp = ast.literal_eval(well_data['vp'])[-1]
	# 			else:
	# 				vp = -888
	# 			if ast.literal_eval(well_data['time']):
	# 				time = ast.literal_eval(well_data['time'])[-1]
	# 			time_n = well_data['time_n']
	# 			field = well_data['field']
	# 			if status == 'on':
	# 				message = r'Well ID: {0}<br>Well Status: {1}<br>Completion Type: {2}<br>Last Event Timestamp: {3}<br>Oil volume produced last cycle: {4:.2f} Bbls'.format(key, status, comp, time, vp)
	# 			else:
	# 				message = r'Well ID: {0}<br>Well Status: {1} (Predicted start up time: {2})<br>Last Event Timestamp: {3}<br>Completion Type: {4}<br>Oil volume produced last cycle: {5:.2f} Bbls'.format(key, status, time_n, time, comp, vp)
	# 			wells[key] = dict(field=field,lat=lat,lng=lng,status=status,message=message)
	# 
	# 		if count >= 5:
	# 			break
		

	if well_data:
		# return jsonify(result=well_result, other=wells)
		return jsonify(result=well_result)
	else:
		field='Not Available'
		lat=37.09024
		lng=-100.712891
		status='N/A'
		message = r'Well ID: {0}<br>Well Status: {1}'.format(well_id, 'This well does not exist')
		well_result = dict(field=field,lat=lat,lng=lng,status=status,message=message)
		return jsonify(result=well_result)

@app.route('/fieldselection')
def fieldSelector():
	fields = {}
	dns = 'ec2-34-234-14-219.compute-1.amazonaws.com'
	fields['1']=dict(fieldID='1',field='Eagleville',lat=31.968599,lng=-99.901813)
	fields['2']=dict(fieldID='2',field='Spraberry',lat=32.6828828,lng=-101.7823657)
	fields['3']=dict(fieldID='3',field='Prudhoe Bay',lat=70.325556,lng=-148.711389)
	fields['4']=dict(fieldID='4',field='Wattenberg',lat=40.15,lng=-104.45)
	fields['5']=dict(fieldID='5',field='Briscoe Ranch',lat=28.23222,lng=-99.857776)
	fields['6']=dict(fieldID='6',field='Kuparuk River',lat=70.3372,lng=-149.8504)
	fields['7']=dict(fieldID='7',field='Mississippi Canyon',lat=28.5,lng=-89.75)
	fields['8']=dict(fieldID='8',field='Wasson',lat=33.0487,lng=-102.8202)
	fields['9']=dict(fieldID='9',field='Belridge South',lat=35.4755183,lng=-119.7423528)
	fields['10']=dict(fieldID='10',field='Green Canyon',lat=27.597,lng=-90.163)

	selected_field_name = request.args.get('field')
	for i in range(1, len(fields)+1):
		if fields[str(i)]['field'] == selected_field_name:
			field_lat = fields[str(i)]['lat']
			field_lng = fields[str(i)]['lng']
			field_id = fields[str(i)]['fieldID']

	# return jsonify(result={'lat': lat, 'lng': lng})
	hist_map = redis.StrictRedis(host=dns, port=6379, db=0, decode_responses=True)
	wells = {}
	count = 0
	for key in hist_map.keys():
		if hist_map.hgetall(key)['fieldID'] == field_id:
			count += 1
			well_data = hist_map.hgetall(key)
			lat = well_data['lat']
			lng = well_data['lng']
			comp = well_data['comp']
			status = well_data['status']
			if ast.literal_eval(well_data['vp']): 
				vp = ast.literal_eval(well_data['vp'])[-1]
			else:
				vp = -888
			if ast.literal_eval(well_data['time']):
				time = ast.literal_eval(well_data['time'])[-1]
			time_n = well_data['time_n']
			field = well_data['field']
			if status == 'on':
				message = r'Well ID: {0}<br>Well Status: {1}<br>Completion Type: {2}<br>Last Event Timestamp: {3}<br>Oil volume produced last cycle: {4:.2f} Bbls'.format(key, status, comp, time, vp)
			else:
				message = r'Well ID: {0}<br>Well Status: {1} (Predicted start up time: {2})<br>Last Event Timestamp: {3}<br>Completion Type: {4}<br>Oil volume produced last cycle: {5:.2f} Bbls'.format(key, status, time_n, time, comp, vp)
			wells[key] = dict(field=field,lat=lat,lng=lng,status=status,message=message)

		if count >= 30:
			break

	return jsonify(result=wells,length=count)

# @app.route('/wellsearch')
# def wellSearch():
# 	wellid = request.arg.get('sub', 0, type=str)
# 	return jsonify( result=lookupwell(wellid) )

# @app.route('/charts')
# def charts():
#  	return render_templates("charts.html")


@app.route('/_add_numbers')
def add_numbers():
    """Add two numbers server side, ridiculous but well..."""
    a = request.args.get('a', 0, type=int)
    b = request.args.get('b', 0, type=int)
    return jsonify(result=a + b)


# @app.route('/estimator')
# def addresult():
#     return render_template('estimator.html')
