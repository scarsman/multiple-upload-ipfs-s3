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
