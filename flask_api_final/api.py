from flask_api import FlaskAPI
from flask import request
from helper_functions import *
import psycopg2 as pg
import sys
from time import time


app = FlaskAPI(__name__)

###
#genera salts
salts = hash_family(k=20)
big_prime = 1042043
#genera vector	 de bits 
filtro_bloom = bloom_filter(salts,big_prime)
#genera hyperloglog
hloglog = hyperloglog(5,salts)
###

###Cońexión a postgres 
#Borra lo que tenga.
pos_connection = pg.connect(dbname='flujo', user='usuario_flujo', host="pos1.cjp3gx7nxjsk.us-east-1.rds.amazonaws.com", password='flujos', connect_timeout=8)
cur = pos_connection.cursor()
cur.execute("TRUNCATE checkin")
cur.execute("TRUNCATE checkin_bloom")
cur.execute("TRUNCATE window_flujo")
pos_connection.commit()
cur.close()

@app.route('/limpia_db_bloom/')
def clean_db():

	pg_connection = pg.connect(dbname='flujo', user='usuario_flujo', host="pos1.cjp3gx7nxjsk.us-east-1.rds.amazonaws.com", password='flujos', connect_timeout=8)
	cur = pg_connection.cursor()
	cur.execute("TRUNCATE checkin")
	cur.execute("TRUNCATE checkin_bloom")
	cur.execute("TRUNCATE window_flujo")
	pg_connection.commit()
	cur.close()
	pg_connection.close()

	global filtro_bloom
	global big_prime

	filtro_bloom.bits_vector = np.zeros(big_prime)

	results = {"mensaje":"Se borraron registros y bloom_filter"}

	return results 

@app.route('/insert_elements_bloom/',methods=['POST'])
def insert_elements_bloom_filter():

	records = request.data.get('records')
	#counter
	nuevas_visitas = 0 
	visitas_existentes = 0

	global pos_connection

	ts0 =time()

	for visit in records:	
		#revisa si esta en el filtro, si no, insertalo.
		es_nuevo = filtro_bloom.new_observation(visit)
		
		
		if es_nuevo == 1:
	
			#Si la conexión murió, vuelve a abrirla.
			try:
				cur = pos_connection.cursor()
			except:
			 	pos_connection = pg.connect(dbname='flujo', user='usuario_flujo', host="pos1.cjp3gx7nxjsk.us-east-1.rds.amazonaws.com", password='flujos',connect_timeout=8)
			 	cur = pos_connection.cursor()

			cur.execute("insert into checkin_bloom (checkin) values (%s)",(visit,))
			cur.close()
			pos_connection.commit()
			nuevas_visitas +=1
		else:
			visitas_existentes +=1

	# #reportar cuantas existe, cuantas no segun base.
	# #saca fp
	##Cuantas ya existían.

	ts1 =time()
	tiempo = str(ts1 - ts0)
	
	results = {

		'visitas_existentes': visitas_existentes,
		'nuevas_visitas' : nuevas_visitas,
		'tiempo_en_segundos' : tiempo

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
	 	cur = pos_connection.cursor()
	
	ts0 =time()

	#inserta los records
	for record in records:
		try:
			cur.execute("insert into checkin (checkin) values (%s)",(record,))
			inserted_base += 1
		except pg.IntegrityError:
			visitas_existentes_base +=1

		pos_connection.commit()
		
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
	#Si la conexión murió, vuelve a abrirla.
	try:
		cur = pos_connection.cursor()
	except:
	 	pos_connection = pg.connect(dbname='flujo', user='usuario_flujo', host="pos1.cjp3gx7nxjsk.us-east-1.rds.amazonaws.com", password='flujos',connect_timeout=8)
	 	cur = pos_connection.cursor()
	
	#inserta los records
	for record in records:
	
		cur.execute("insert into window_flujo (nodo,pot) values (%s,%s)",(record[0],record[1]))
		pos_connection.commit()
		insertados +=1
		
	cur.close()
	
	results = {

		"hola" : "Has insertado {}".format(insertados)
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
	#
	unicas_hll = hloglog.count()

	results = {

		'unicas_base': unicas_base,
		'unicas hloglog': unicas_hll

	}

	return results


if __name__ == "__main__":
    app.run(debug=True)
