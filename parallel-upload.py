import os
import sys
import time
import requests
import json
from requests_toolbelt import MultipartEncoder

from multiprocessing import Process, Semaphore, Manager
from threading import Thread

from orator import DatabaseManager
from db_config import DATABASES

from shutil import copyfile


MAX_CONTRACTOR_WORKERS = 25
MAX_WRITER_WORKERS = 5

#task status
PENDING = 0
PROCESSING = 1
SUCCESS = 2
FAILED = 3

#nginx basic auth
USERNAME = 'user'
PASSWORD = 'password'

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
		
		print("Loader online")

		images = get_all_images() ##get all images in list and put that images in the database and set to new PENDING
		if images:
			for image in images:
				self.db.table("tasks").insert({'image_path': image})

		
class Contractor(Process):
	
	def __init__(self, queue):
		super(Contractor, self).__init__()
		self.worker_control = Semaphore(MAX_CONTRACTOR_WORKERS)
		self.db = DatabaseManager(DATABASES)
		self.result_queue = queue

	def run(self):
		
		print("Contractor online")

		while True:
			if self.worker_control.acquire(False):

				try:
					#get pending task
					task = self.db.table('tasks').where('processing_status', PENDING).first()
					
					if task:
						self.db.table("tasks").where('id', task.id).update({'processing_status' : PROCESSING})

						worker = ContractorWorker(task, self.worker_control, self.result_queue)
						worker.start()

				except Exception as err:
					print(err)	

			time.sleep(0.2)		

class ContractorWorker(Thread):
	
	def __init__(self, task, worker_control, queue):
		Thread.__init__(self)
		self.task = task
		self.worker_control = worker_control
		self.db = DatabaseManager(DATABASES)
		self.ipfs_s3_auth = (USERNAME, PASSWORD)
		self.result_queue = queue

	def upload_file(self, file_path):
		status = True
		file_hash = None

		try:
			m = MultipartEncoder( fields={'file': ('filename', open(file_path, 'rb'), 'application/octet-stream')})
			r = requests.post("https://ipfs-s3.shortcut-lab.com/api/v0/add", data=m, headers={'Content-Type': m.content_type},auth=self.ipfs_s3_auth)
		
			if r.status_code == 200:
				file_hash = json.loads(r.text)["Hash"]
			else:
				raise Exception("IPFS FILE UPLOAD FAILED")

		except Exception as err:
			print(err)
			status = False

		return(status, file_hash)
			
	def run(self):
		#execute task
		print("contractor worker for task: %s" % self.task)
		#get image path
		task_id = self.task.id
		file_path = self.task.image_path
		
		try:			
			print("Uploading file `%s` to ipfs" % file_path)

			status, ipfs_hash = self.upload_file(file_path)
			#set status to sucess
			#self.db.table("tasks").where('id', task_id).update({'processing_status': SUCCESS})

			processing_status = SUCCESS

			print("filepath: %s , status: %s" %(file_path, status))

			if not status:
				ipfs_hash = None
				processing_status = FAILED

			self.result_queue.put({'id': task_id,'ipfs_hash': ipfs_hash ,'processing_status': processing_status})

		except Exception as err:
			print("contractor exception %s" % err)
			#set status to failure
			#self.db.table("tasks").where('id', task_id).update({'processing_status': FAILED})
			self.result_queue.put({'id': task_id, 'ipfs_hash': ipfs_hash , 'processing_status': FAILED})

		self.worker_control.release() 



class DbWriter(Process):
	
	def __init__(self, queue):
		super(DbWriter, self).__init__()
		self.worker_control = Semaphore(MAX_WRITER_WORKERS)
		self.result_queue = queue

	def run(self):
		
		print("DB Writer online")

		while True:

			if self.worker_control.acquire(False):

				task = self.result_queue.get()
				
				if task:
					try:
						worker = WriterWorker(task, self.worker_control) 
						worker.start()

					except Exception as err:
						print(err)
						print("Invalid task %s" % task)

			time.sleep(0.3)			

class WriterWorker(Thread):
	
	def __init__(self, task, worker_control):
		Thread.__init__(self)
		self.task = task
		self.worker_control = worker_control	
		self.db = DatabaseManager(DATABASES)

	def update_task_status(self, params):
		print("Updating processing_status for task %s" % params['id'])
		self.db.table("tasks").where("id", params['id']).update({"ipfs_hash": params["ipfs_hash"], "processing_status": params['processing_status']})
		print("- Done")

	def run(self):

		print("writer worker online for task: %s" % self.task)

		try:
			self.update_task_status(self.task)
	
		except Exception as err:
			print(err)

		self.worker_control.release()


if __name__ == "__main__":

	manager = Manager()
	queue = manager.Queue()

	loader = Loader()
	contractor = Contractor(queue)
	writer = DbWriter(queue)
	loader.start()
	contractor.start()
	writer.start()

	loader.join()
	contractor.join()
	writer.join()		
