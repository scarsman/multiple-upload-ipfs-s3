import os
import time
import requests
import json
from requests_toolbelt import MultipartEncoder

from multiprocessing import Process, Semaphore, Manager, Value
from threading import Thread

from orator import DatabaseManager
from db_config import DATABASES


MAX_CONTRACTOR_WORKERS = 30
MAX_WRITER_WORKERS = 10

#task status
PENDING = 0
PROCESSING = 1
SUCCESS = 2
FAILED = 3

#nginx basic auth since our ipfs-s3 server need credentials to access the resources
USERNAME = 'pygmalion1357'
PASSWORD = 'galatea2468'


class Contractor(Process):

	def __init__(self, queue):
		super(Contractor, self).__init__()
		self.worker_control = Semaphore(MAX_CONTRACTOR_WORKERS)
		self.db = DatabaseManager(DATABASES)
		self.result_queue = queue

	def reset_processing_to_pending(self):
		self.db.table("tasks").where('processing_status', PROCESSING).update({'processing_status' : PENDING})

	def run(self):

		print("*** Contractor online")

		print("**** Resetting all halted tasks which status is processing into new pending")
		self.reset_processing_to_pending()

		while True:

			if self.worker_control.acquire(False):
				try:
					#get pending task
					task = self.db.table('tasks').where('processing_status', PENDING).first()

					if task:
						self.db.table("tasks").where('id', task.id).update({'processing_status' : PROCESSING})

						worker = ContractorWorker(task, self.worker_control, self.result_queue)
						worker.start()

					else:
						self.worker_control.release()

				except Exception as err:
					print(err)
					self.worker_control.release()

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
				raise Exception("--- IPFS FILE UPLOAD FAILED FOR IMAGE FILEPATH %s" %(file_path))

		except Exception as err:
			print(err)
			status = False

		return(status, file_hash)

	def run(self):
		#execute task
		print("** Contractor worker alive for task-id: %s" % self.task.id)
		#get image path
		task_id = self.task.id
		file_path = self.task.image_path

		try:
			print("--- Uploading file `%s` to ipfs server with task id of %s \n" % (file_path, task_id))

			status, ipfs_hash = self.upload_file(file_path)
			processing_status = SUCCESS

			#print("filepath: %s , status: %s" %(file_path, status))

			if not status:
				ipfs_hash = None
				processing_status = FAILED

			self.result_queue.put({'id': task_id,'ipfs_hash': ipfs_hash ,'processing_status': processing_status})

		except Exception as err:
			print("--- Contractor exception %s" % err)
			self.result_queue.put({'id': task_id, 'ipfs_hash': ipfs_hash , 'processing_status': FAILED})

		self.worker_control.release()

class DbWriter(Process):

	def __init__(self, queue):
		super(DbWriter, self).__init__()
		self.worker_control = Semaphore(MAX_WRITER_WORKERS)
		self.result_queue = queue

	def run(self):

		print("*** DB Writer online")

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
						self.worker_control.release()
				else:
					self.worker_control.release()

			time.sleep(0.3)

class WriterWorker(Thread):

	def __init__(self, task, worker_control):
		Thread.__init__(self)
		self.task = task
		self.worker_control = worker_control
		self.db = DatabaseManager(DATABASES)

	def update_task_status(self, params):

		print("-- Updating processing_status for task id %s with processing_status of %s into the database" % (params['id'], params['processing_status']))

		self.db.table("tasks").where("id", params['id']).update({"ipfs_hash": params["ipfs_hash"], "processing_status": params['processing_status']})

		print("--- Done updating the task-id %s into the database" % params['id'])

	def run(self):

		print("** Writer worker alive for task-id: %s" % self.task["id"])

		try:
			self.update_task_status(self.task)

		except Exception as err:
			print(err)

		self.worker_control.release()

class Launcher:
	def __init__(self):
		pass

	def start(self):

		loader = None
		contractor = None
		writer = None

		try:
			# START CONTRACTOR AND DBWRITER
			manager = Manager()
			queue = manager.Queue()

			contractor = Contractor(queue)
			contractor.start()
			contractor_pid = contractor.pid
			print("contractor pid: %s" %contractor_pid)

			writer = DbWriter(queue)
			writer.start()
			writer_pid = writer.pid
			print("writer pid: %s" % writer_pid)

			# wait for the loader and contractor and writer to exit
			contractor.join()
			writer.join()

		except (KeyboardInterrupt, SystemExit): #stop processes immediately,halted task will be handle during the restart of the script
			print("Stopping all Process. Done.")
			contractor.terminate()
			writer.terminate()

if __name__ == "__main__":

	launcher = Launcher()
	launcher.start()
