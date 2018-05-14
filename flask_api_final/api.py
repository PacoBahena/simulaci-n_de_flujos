from flask_api import FlaskAPI
from flask import request
from helper_functions import *


app = FlaskAPI(__name__)

###
#genera salts
salts = hash_family()
big_prime = 526717
#genera vector	 de bits 
filtro_bloom = bloom_filter(salts,big_prime)
###


@app.route('/insert_elements/',methods=['POST'])
def insert_elements():

	macs = request.data.get('macs')
	# #for every mac_address in macs.
	inserted_count = 0 
	for mac in macs:	
		#revisa si esta en el filtro, si no, insertalo.
		inserted_count += filtro_bloom.check_element(mac)

	# #reportar cuantas existe, cuantas no segun base.
	# #saca fp

	nuevas_macs_filtro = inserted_count
	macs_existentes_filtro = len(macs) - inserted_count

	# #### real database stats.

	# #select all macs, 

	# #insert unexistent


	results = {

		'macs_existentes_filtro': macs_existentes_filtro,
		'nuevas_macs_filtro' : nuevas_macs_filtro,

	}
	
	return results



if __name__ == "__main__":
    app.run(debug=True)