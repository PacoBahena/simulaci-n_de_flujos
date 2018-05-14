import string
from random import choice,randint,seed
import numpy as np
from pyblake2 import blake2b

def hash_family(semilla=12345,k=100):
	"""
	Dado una semilla y un número k, 
	genera los salts (semilla en string)
	para generar k funciónes de hash blake2b
	"""
	seed(semilla)
	hash_salts = []
	for i in range(k):
		min_char = 8
		max_char = 12
		allchar = string.ascii_letters + string.digits
		password = "".join(choice(allchar) for x in range(randint(min_char, max_char)))
		salt = password.encode()
		hash_salts.append(salt)

	return hash_salts


def hash_generator(elemento,salts,modulo_primo=526717):
    """
    Recibe un elemento, y genera los hashes blake2b del elemento, se mapean a
    enteros y se les aplica módulo de un primo dado.
    """
    elemento = elemento.encode() 
    hashes = [int(blake2b(elemento,salt=salt).hexdigest()[:10],16) % modulo_primo\
    for salt in salts] 
    
    return hashes


class bloom_filter:
	"""
	Un bloom filter.
	
	Recibe los salts generados para loss hashes con blake2b y un primo, que es el largo del]
	vector de bits.
	"""     
	def __init__(self,salts,big_prime):
	    self.salts = salts
	    self.big_prime = big_prime
	    self.bits_vector = np.zeros(self.big_prime)

	def check_element(self,element):
		"""
		Checa, dado un elemento, 
		si este ya existe, si no, lo inserta.
		"""#generate hashes
		hashes = hash_generator(element,self.salts,self.big_prime)
		if self.bits_vector[hashes].sum() == len(hashes):
		    print('elemento {} ya esta en la lista'.format(element))
		    return 0
		else:
		    print('elemento {} no esta en la lista'.format(element))
		    #inserción en el filtro de bloom
		    self.bits_vector[hashes] = 1
		    return 1
        
    