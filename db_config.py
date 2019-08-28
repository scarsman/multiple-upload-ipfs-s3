import inspect
import os

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

DB_DIR = "db"
DB_NAME = "images.db"

DATABASES = {
    'sqlite': {
        'driver': 'sqlite',
        'database': os.path.join(path, DB_DIR, DB_NAME),
    }
}

#using mysql
"""DATABASES = {
	'mysql': {
		'driver': 'mysql',
		'host': 'host',
		'database': 'db',
		'user': 'username',
		'password': 'pass',
		'prefix': '',
		'use_unicode': True,
		'charset' : 'utf8'
	}
}"""
