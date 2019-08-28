import os
import signal
import sys
import time
import requests
import json
from requests_toolbelt import MultipartEncoder

from multiprocessing import Process, Semaphore, Manager, Value
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
USERNAME = 'pygmalion1357'
PASSWORD = 'galatea2468'

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


class Contractor(Process):

	def __init__(self, queue, stop_flag):
		super(Contractor, self).__init__()
		self.worker_control = Semaphore(MAX_CONTRACTOR_WORKERS)
		self.db = DatabaseManager(DATABASES)
		self.result_queue = queue
		self.stop_flag = stop_flag

	def reset_processing_to_pending(self):
		self.db.table("tasks").where('processing_status', PROCESSING).update({'processing_status' : PENDING})

	def run(self):

		print("*** Contractor online")
		#reset all processing status tasks resulted from control + c or halted

		print("Resetting all halted tasks which status is processing into new pending")
		self.reset_processing_to_pending()

		while True and self.stop_flag.value != 1:

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

		print("stop flag: %s" % self.stop_flag.value)

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
				raise Exception("IPFS FILE UPLOAD FAILED FOR IMAGE FILEPATH %s" %(file_path))

		except Exception as err:
			print(err)
			status = False

		return(status, file_hash)

	def run(self):
		#execute task
		print("contractor worker alive for task-id: %s" % self.task.id)
		#get image path
		task_id = self.task.id
		file_path = self.task.image_path

		try:
			print("-- Uploading file `%s` to ipfs server with task id of %s \n" % (file_path, task_id))

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
			self.result_queue.put({'id': task_id, 'ipfs_hash': ipfs_hash , 'processing_status': FAILED})

		self.worker_control.release()



class DbWriter(Process):

	def __init__(self, queue, stop_flag):
		super(DbWriter, self).__init__()
		self.worker_control = Semaphore(MAX_WRITER_WORKERS)
		self.result_queue = queue
		self.stop_flag = stop_flag

	def run(self):

		print(" *** DB Writer online")

		while True and self.stop_flag.value != 1:

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

		print("stop flag: %s" % self.stop_flag.value)

class WriterWorker(Thread):

	def __init__(self, task, worker_control):
		Thread.__init__(self)
		self.task = task
		self.worker_control = worker_control
		self.db = DatabaseManager(DATABASES)

	def update_task_status(self, params):

		print("- Updating processing_status for task id %s with processing_status of %s into the database" % (params['id'], params['processing_status']))

		self.db.table("tasks").where("id", params['id']).update({"ipfs_hash": params["ipfs_hash"], "processing_status": params['processing_status']})

		print("-- Done updating the task-id %s into the database" % params['id'])

	def run(self):

		print("Writer worker online for task-id: %s" % self.task["id"])

		try:
			self.update_task_status(self.task)

		except Exception as err:
			print(err)

		self.worker_control.release()


class PIDSingleton(object):

	__instance = None

	def __init__(self):

		self.LOADER_PID = None
		self.CONTRACTOR_PID = None
		self.WRITER_PID = None
		self.STOP_FLAG = None

	def get_instance(self):

		if not PIDSingleton.__instance:
			PIDSingleton.__instance = self

		return PIDSingleton.__instance


def graceful_exit(signum, frame):

	print("GRACEFUL EXIT HANDLER INVOKED !")

	s = PIDSingleton().get_instance()
	s.STOP_FLAG.value = 1

	print(s.STOP_FLAG.value)
	print(s.CONTRACTOR_PID)

	if s.LOADER_PID:
		print("TERMINATING LOADER PID: %s" % s.LOADER_PID)
		os.kill(s.LOADER_PID, signal.SIGTERM)
		s.LOADER_PID = None

	if s.CONTRACTOR_PID:
		print("TERMINATING CONTRACTOR PID: %s" % s.CONTRACTOR_PID)
		os.kill(s.CONTRACTOR_PID, signal.SIGTERM)
		s.CONTRACTOR_PID = None

	if s.WRITER_PID:
		print("TERMINATING WRITER PID: %s" % s.WRITER_PID)
		os.kill(s.WRITER_PID, signal.SIGTERM)
		s.WRITER_PID = None

def start():

	signal.signal(signal.SIGTERM, graceful_exit)
	signal.signal(signal.SIGINT, graceful_exit)
	signal.signal(signal.SIGUSR1, graceful_exit)

	# START LOADER, CONTRACTOR AND DBWRITER
	manager = Manager()
	queue = manager.Queue()

	s = PIDSingleton().get_instance()
	s.STOP_FLAG = Value('i', 0)


	loader = Loader()
	loader.start()
	loader_pid = loader.pid
	print("loader pid: %s" % loader_pid)

	contractor = Contractor(queue, s.STOP_FLAG)
	contractor.start()
	contractor_pid = contractor.pid
	print("contractor pid: %s" %contractor_pid)

	writer = DbWriter(queue,s.STOP_FLAG)
	writer.start()
	writer_pid = writer.pid
	print("writer pid: %s" % writer_pid)

	s.LOADER_PID = loader_pid
	s.CONTRACTOR_PID = contractor_pid
	s.WRITER_PID = writer_pid

	# wait for the loader and contractor and writer to exit
	loader.join()
	contractor.join()
	writer.join()

if __name__ == "__main__":
	start()
