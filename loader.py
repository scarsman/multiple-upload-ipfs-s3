import os

from shutil import copyfile
from multiprocessing import Process

from orator import DatabaseManager
from db_config import DATABASES

#test
def get_all_images(): #return list

	image_path = os.path.join(os.getcwd(), "images")
	dog_image = os.path.join(image_path, "dog.jpg")

	images = []

	for i in range(10):
		dest = os.path.join(image_path, '%s.jpg' %i)
		copyfile(dog_image, dest)
		images.append("%s" %(dest))

	return images

class Loader(Process):

	def __init__(self):
		super(Loader, self).__init__()
		self.db = DatabaseManager(DATABASES)

	def run(self):

		print("*** Loader online")

		images = get_all_images() ##get all images in list and put that images in the database and set to new PENDING
		if images:
			for image in images:
				print("Inserting image file-path %s into the database" %image)
				self.db.table("tasks").insert({'image_path': image})

if __name__ == "__main__":
    loader = Loader()
    loader.start()
    loader.join()
