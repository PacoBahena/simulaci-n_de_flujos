from flask_api import FlaskAPI
from flask import request
from helper_functions import *
import psycopg2 as pg
import sys
from time import time
import pandas as pd


app = FlaskAPI(__name__)

###
#genera salts
salts = hash_family(k=20)
big_prime = 1042043
#genera vector	 de bits 
filtro_bloom = bloom_filter(salts,big_prime)
filtro_bloom_empleados = bloom_filter(salts,big_prime)

canasta = cubeta()
#genera hyperloglog
hloglog = hyperloglog(5)
###

unique_inserts_counter = 0

###Cońexión a postgres 
#Borra lo que tenga.
pos_connection = pg.connect(dbname='flujo', user='usuario_flujo', host="pos1.cjp3gx7nxjsk.us-east-1.rds.amazonaws.com", password='flujos', connect_timeout=8)
pos_connection.set_session(autocommit=True)
cur = pos_connection.cursor()
cur.execute("TRUNCATE checkin")
cur.execute("TRUNCATE checkin_bloom")
cur.execute("TRUNCATE window_flujo")
pos_connection.commit()
cur.close()

@app.route('/limpia_db_bloom/<int:num_hashes>/<int:big_prime>')
def clean_db(num_hashes,big_prime):

	pg_connection = pg.connect(dbname='flujo', user='usuario_flujo', host="pos1.cjp3gx7nxjsk.us-east-1.rds.amazonaws.com", password='flujos', connect_timeout=8)
	cur = pg_connection.cursor()
	cur.execute("TRUNCATE checkin")
	cur.execute("TRUNCATE checkin_bloom")
	cur.execute("TRUNCATE window_flujo")
	pg_connection.commit()
	cur.close()
	pg_connection.close()

	global filtro_bloom
	global unique_inserts_counter

	salts = hash_family(k=num_hashes)
	#genera vector	 de bits 
	filtro_bloom = bloom_filter(salts,big_prime)
	filtro_bloom_empleados = bloom_filter(salts,big_prime)
	unique_inserts_counter = 0

	results = {"mensaje":"Se borraron registros y bloom_filter y bloom_filter_empleados"}

	return results 

@app.route('/insert_elements_bloom/',methods=['POST'])
def insert_elements_bloom_filter():

	records = request.data.get('records')
	#counter
	nuevas_visitas = 0 
	visitas_existentes = 0

	global pos_connection
	global unique_inserts_counter

	try:
		cur = pos_connection.cursor()
	except:
	 	pos_connection = pg.connect(dbname='flujo', user='usuario_flujo', host="pos1.cjp3gx7nxjsk.us-east-1.rds.amazonaws.com", password='flujos',connect_timeout=8)
	 	pos_connection.set_session(autocommit=True)
	 	cur = pos_connection.cursor()

	ts0 =time()

	for visit in records:	
		#revisa si esta en el filtro, si no, insertalo.
		# Si es nuevo es igual a 0,

		es_empleado = filtro_bloom_empleados.is_in_filter(visit)
		
		if es_empleado == 0:

			es_nuevo = filtro_bloom.new_observation(visit)
			unique_inserts_counter += es_nuevo		
		
		if es_nuevo == 1:

			#Si la conexión murió, vuelve a abrirla.
			cur.execute("insert into checkin_bloom (checkin) values (%s)",(visit,))
			nuevas_visitas +=1
		else:
			visitas_existentes +=1

	
	cur.close()

	ts1 =time()
	tiempo = str(ts1 - ts0)
	
	results = {

		'visitas_existentes': visitas_existentes,
		'nuevas_visitas' : nuevas_visitas,
		'tiempo_en_segundos' : tiempo

	}
	
	return results

@app.route('/check_bloom_db/',methods=['GET'])
def check_number_bloom_db():

	global unique_inserts_counter
	global pos_connection
	
	try:
		cur = pos_connection.cursor()
	except:
		pos_connection = pg.connect(dbname='flujo', user='usuario_flujo', host="pos1.cjp3gx7nxjsk.us-east-1.rds.amazonaws.com", password='flujos',connect_timeout=8)
		cur = pos_connection.cursor()

	cur.execute("""select count(*) from checkin""")
	cuenta = cur.fetchone()[0]

	results = {

		'elementos_insertados_en_bloom': unique_inserts_counter,
		'elementos_en_db': cuenta

	}
	

	return results


@app.route('/insert_elements_db/',methods=['POST'])
def insert_elements_on_db():

	records = request.data.get('records')
	
	##Cuantas ya existían.
	##### real database stats.
	
	inserted_base = 0
	visitas_existentes_base = 0

	global pos_connection
	#Si la conexión murió, vuelve a abrirla.
	try:
		cur = pos_connection.cursor()
	except:
	 	pos_connection = pg.connect(dbname='flujo', user='usuario_flujo', host="pos1.cjp3gx7nxjsk.us-east-1.rds.amazonaws.com", password='flujos',connect_timeout=8)
	 	pos_connection.set_session(autocommit=True)
	 	cur = pos_connection.cursor()
	
	ts0 =time()
	#inserta los records
	for record in records:
		try:
			cur.execute("insert into checkin (checkin) values (%s)",(record,))
			inserted_base += 1
		except pg.IntegrityError:
			visitas_existentes_base +=1
		
	cur.close()
	ts1 =time()

	tiempo = str(ts1 - ts0)
	
	results = {

		'nuevas_visitas_base': inserted_base,
		'visitas_existentes_base' : visitas_existentes_base,
		'tiempo_en_segundos':tiempo
	}
	
	return results


	

@app.route('/insert_elements_db_window/',methods=['POST'])
def insert_elements_on_window_db():

	records = request.data.get('records')
	
	##Cuantas ya existían.
	##### real database stats.
	
	insertados = 0

	global pos_connection
	global canasta
	#Si la conexión murió, vuelve a abrirla.
	try:
		cur = pos_connection.cursor()
	except:
	 	pos_connection = pg.connect(dbname='flujo', user='usuario_flujo', host="pos1.cjp3gx7nxjsk.us-east-1.rds.amazonaws.com", password='flujos',connect_timeout=8)
	 	cur = pos_connection.cursor()
	
	#inserta los records
	for record in records:
	
		cur.execute("insert into window_flujo (mac,ts) values (%s,%s)",(record[0],record[1]))
		pos_connection.commit()
		insertados +=1
		
	cur.close()


	###si cae en cubeta 1, guardar record.

	if hash_bucket(record[0]) == 1:

		canasta.add_element(record)
	
	results = {

		"hola" : "Has insertado {}".format(insertados)
	}
	
	return results

@app.route('/check_window_sample/',methods=['GET'])
def check_time_window_sample_db():


	global pos_connection
	global canasta
	#Si la conexión murió, vuelve a abrirla.
	# try:
	# 	cur = pos_connection.cursor()
	# except:
	#  	pos_connection = pg.connect(dbname='flujo', user='usuario_flujo', host="pos1.cjp3gx7nxjsk.us-east-1.rds.amazonaws.com", password='flujos',connect_timeout=8)
	cur = pos_connection.cursor()
	
	query = """select AVG(duracion) from (select t.mac,t.first - t.last as duracion from
	 			(select mac,MAX(ts) as first,MIN(ts) as last from window_flujo group by mac) as t) as e"""

	cur.execute(query)
	duracion_promedio = cur.fetchone()[0]
	cur.close()
	
	df_canasta = pd.DataFrame(canasta.values)
	df_canasta.columns = ['mac','tiempo']
	tabla_prom_max = df_canasta.groupby('mac')['tiempo'].max().reset_index()
	tabla_prom_max.columns = ['mac_1','first']
	tabla_prom_min = df_canasta.groupby('mac')['tiempo'].min().reset_index()
	tabla_prom_min.columns = ['mac_2','last']
	tabla = tabla_prom_max.merge(tabla_prom_min,how='inner',left_on='mac_1',right_on='mac_2')

	tabla['duracion'] = tabla['last'] - tabla['first']
	tabla_prom = tabla.groupby('mac_1')['duracion'].mean().reset_index()
	duracion_promedio_canasta = int(tabla_prom.duracion.mean())

	# print('holi2')
	
	results = {

		"db_duracion_promedio" : int(duracion_promedio),
		"canasta_duracion_promedio" : duracion_promedio_canasta
	}
	
	return results


@app.route('/is_in_filter/',methods=['POST'])
def check_is_in_filter():

	records = request.data.get('records')

	estan = 0

	ts0 =time()

	for record in records:

		esta = filtro_bloom.is_in_filter(record[0])
		estan += esta

	ts1 =time()

	tiempo = str(ts1 - ts0)

	results = {

		'ya_en_filtro': estan,
		'no_estan_filtro' : len(records) - estan,
		'tiempo_en_segundos' : tiempo

	}

	return results

@app.route('/is_in_db/',methods=['POST'])
def check_is_in_db():

	records = request.data.get('records')
	estan = 0

	ts0 =time()

	global pos_connection

	try:
		cur = pos_connection.cursor()
	except:
	 	pos_connection = pg.connect(dbname='flujo', user='usuario_flujo', host="pos1.cjp3gx7nxjsk.us-east-1.rds.amazonaws.com", password='flujos',connect_timeout=8)
	 	cur = pos_connection.cursor()

	for record in records:

		cur.execute("""select checkin from checkin where checkin=%s""",(record[0],))
		if cur.fetchone() is None:
			estan += 0
		else:
			estan += 1

	ts1 =time()

	tiempo = str(ts1 - ts0)


	results = {

		'ya_en_la_db': estan,
		'no_estan_db' : len(records) - estan,
		'tiempo_en_segundos' : tiempo

	}

	return results


@app.route('/check_unique/',methods=['POST'])
def check_unique():

	global pos_connection
	#Si la conexión murió, vuelve a abrirla.
	try:
		cur = pos_connection.cursor()
	except:
	 	pos_connection = pg.connect(dbname='flujo', user='usuario_flujo', host="pos1.cjp3gx7nxjsk.us-east-1.rds.amazonaws.com", password='flujos',connect_timeout=8)
	 	cur = pos_connection.cursor()

	records = request.data.get('records')

	for record in records:

		cur.execute("insert into flujo (registro) values (%s)",(record,))

	pos_connection.commit()

	cur.execute("select count(distinct(registro)) from flujo")
	unicas_base = cur.fetchone()[0]
	unicas_hll = hloglog.count()

	results = {

		'unicas_base': unicas_base,
		'unicas hloglog': unicas_hll

	}

	return results


if __name__ == "__main__":
    app.run(debug=True)
