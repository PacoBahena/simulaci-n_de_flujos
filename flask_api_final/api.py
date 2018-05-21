from flask_api import FlaskAPI
from flask import request
from helper_functions import *
import psycopg2 as pg
import sys


app = FlaskAPI(__name__)

###
#genera salts
salts = hash_family(k=sys.argv[1])
big_prime = 526717
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
cur.execute("TRUNCATE flujo")
pos_connection.commit()
cur.close()

@app.route('/insert_elements/',methods=['POST'])
def insert_elements():

	records = request.data.get('records')
	#counter
	inserted_count = 0 
	
	for visit in records:	
		#revisa si esta en el filtro, si no, insertalo.
		inserted_count += filtro_bloom.check_element(visit)

	# #reportar cuantas existe, cuantas no segun base.
	# #saca fp

	nuevas_visitas_filtro = inserted_count
	##Cuantas ya existían.
	visitas_existentes_filtro = len(records) - inserted_count

	##### real database stats.
	
	global pos_connection
	#Si la conexión murió, vuelve a abrirla.
	try:
		cur = pos_connection.cursor()
	except:
	 	pos_connection = pg.connect(dbname='flujo', user='usuario_flujo', host="pos1.cjp3gx7nxjsk.us-east-1.rds.amazonaws.com", password='flujos',connect_timeout=8)
	 	cur = pos_connection.cursor()
	
	#inserta los records
	visitas_existentes_base = 0

	for record in records:
		try:
			cur.execute("insert into checkin (checkin) values (%s)",(record,))
		except pg.IntegrityError:
			visitas_existentes_base +=1

		pos_connection.commit()
		
	cur.close()
	
	results = {

		'macs_existentes_filtro': visitas_existentes_filtro,
		'nuevas_macs_filtro' : nuevas_visitas_filtro,
		'visitas_existentes_base' : visitas_existentes_base

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