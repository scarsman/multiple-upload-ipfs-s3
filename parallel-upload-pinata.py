import os
import time
import requests
import json
import signal
import sys
import time
from datetime import datetime


from requests_toolbelt import MultipartEncoder
from threading import Thread
from multiprocessing import  Semaphore, Process, Value, Manager, Queue

MAX_WORKERS = 5
MAX_CONTRACTORS = 10

#ipfs with s3
USERNAME = 'user'
PASSWORD = 'pass'

#pinata
API_KEY = "your api key"
SECRET_KEY = "your secret api key"

def process_images(images):

	class Contractor(Process):
		
		def __init__(self, images, stop_flag, input_queue, result_queue, api_key, secret_key, time_worker_run):
			super(Contractor, self).__init__()
			self.worker_control = Semaphore(MAX_WORKERS)
			self.images = images
			self.stop_flag = stop_flag
			self.input_queue = input_queue
			self.result_queue = result_queue
			self.api_key = api_key
			self.secret_key = secret_key
			self.time_worker_run = time_worker_run

		def run(self):
			
			print(" ++ Contractor online ++ \n")
			
			while True:
			
				temp_value = None
				temp_value = self.stop_flag.value
				
				#print(">> %s temp value"% temp_value)
					
				if temp_value != 1:

					if self.worker_control.acquire(False):
						
						try:
							task = self.input_queue.get(False)
							
							if task:
								
								worker = Worker(self.worker_control, task, self.input_queue, self.api_key, self.secret_key, self.stop_flag, self.result_queue, self.images, self.time_worker_run)
								worker.start()

							else:
								self.worker_control.release()			
						except:
							self.worker_control.release()	

				else:
					break
				
				time.sleep(0.2)
				
			print("contractor done")

			
	class Worker(Thread):

		def __init__(self, worker_control, task, input_queue, api_key, secret_key, stop_flag, result_queue, images, time_worker_run):
			Thread.__init__(self)
			self.worker_control = worker_control
			self.ipfs_s3_auth = (USERNAME, PASSWORD)
			self.task = task
			self.input_queue = input_queue
			self.api_key = api_key
			self.secret_key = secret_key
			self.stop_flag = stop_flag
			self.result_queue = result_queue
			self.images = images
			self.time_worker_run = time_worker_run
		
		def upload_file(self, file_path):
			status = True
			file_hash = None

			try:
				#m = MultipartEncoder(fields={'file': ('filename', open(file_path, 'rb'), 'application/octet-stream')}) #ipfs - s3
				m = MultipartEncoder( fields={'file': ('filename', open(file_path, 'rb'), 'application/octet-stream'),'pinataMetadata': '{"name": "%s"}' % os.path.basename(file_path) }) #pinata
				
				#r = requests.post("https://ipfs-s3.mydomain.com/api/v0/add", data=m, headers={'Content-Type': m.content_type},auth=self.ipfs_s3_auth) #ipfs with s3-backend(load balancer)
				r = requests.post("https://api.pinata.cloud/pinning/pinFileToIPFS", data=m, headers={'Content-Type': m.content_type, 'pinata_api_key':self.api_key, 'pinata_secret_api_key': self.secret_key }) #pinata cloud
							
				if r.status_code == 200:
					#file_hash =  json.loads(r.text)["Hash"] #for ipfs s3
					file_hash = json.loads(r.text)["IpfsHash"] #pinata
				else:
					raise Exception("--- IPFS FILE UPLOAD FAILED FOR IMAGE FILEPATH %s" %(file_path))

			except Exception as err:
				print(err)
				status = False

			return(status, file_hash)
			
		def run(self):
			
			print("** Worker alive for task: %s" % self.task)

			file_path = self.task
			start_time = datetime.timestamp(datetime.now())
			
			try:
				print("--- Uploading file `%s` to ipfs server \n" % file_path )

				status, ipfs_hash = self.upload_file(file_path)

				if not status:
					raise Exception("Failed Upload")
				
				self.result_queue.put({file_path : ipfs_hash})
				
				print("---- Successfully uploaded to ipfs for file %s \n" % file_path)

			except Exception as err:
				
				print("--- Contractor exception %s" % err)
				print("--- Will retry to upload again")
				self.input_queue.put(self.task)
			
			self.worker_control.release()
			
			print("\n %s finished upload out of %s \n" %(self.result_queue.qsize(), len(self.images)))
			
			lapse_time = datetime.timestamp(datetime.now()) - start_time
			
			print("lapse time processing for task %s is about %s seconds \n" % (file_path, lapse_time))
			
			self.time_worker_run.append(lapse_time)
			
			if self.result_queue.qsize() == len(self.images):
				self.stop_flag.value = 1
				
				print("\n All workers were finished. \n")
			
	class Launcher:
		
		def __init__(self, images):
			self.images = images
			self.manager = Manager()
			self.input_queue = self.manager.Queue()
			self.result_queue = self.manager.Queue()
			self.stop_flag =  self.manager.Value('i', 0)
			self.time_worker_run = self.manager.list()
			self.start_time = None
			
			self.start()
			
		def start(self):
			
			self.start_time = datetime.timestamp(datetime.now())
			
			if self.images:
				for image in self.images:
					self.input_queue.put(image)
					
			contractors = []
			
			for i in range(MAX_CONTRACTORS):
				
				contractor = Contractor(self.images, self.stop_flag, self.input_queue, self.result_queue, API_KEY, SECRET_KEY, self.time_worker_run)
				contractor.start()
				contractors.append(contractor)
			
			for contractor in contractors:
				contractor.join()
				
			print("\n All contractors were finished. Cleaning up")
		
		def get_results(self):
						
			results = {}
			
			while not self.result_queue.empty():
				task = self.result_queue.get(False)
				results.update(task)

			lapse_time = datetime.timestamp(datetime.now()) - self.start_time		
			average_time_per_worker = sum(self.time_worker_run)/len(self.time_worker_run)

			_time = "seconds"

			if lapse_time > 60:
				lapse_time = lapse_time/60
				_time = "minute/s"
				
			print("\n--- average time(sec) per worker: %s" % average_time_per_worker)
			print("--- lapse time in %s %s \n" % (lapse_time, _time))
			
			return results
		
	launcher = Launcher(images)
	results = launcher.get_results()
	
	return results

if __name__ == '__main__':

	#test
	
	list_images = []
	
	image_dir = os.path.join(os.getcwd(), "image")
	
	accepted_images_ext = [".jpeg", ".jpg", ".png", ".tif", ".gif"]
	
	print("------Inserting test data ---------")
	
	for image in os.listdir(image_dir):
		fp = os.path.join(image_dir, image)
		
		if not os.path.isdir(fp):
			
			ext = image[image.rfind("."):]
			
			if ext in accepted_images_ext:
				
				print("appending image file %s " % fp)
				
				list_images.append(fp)
	
	#example lists data as parameter you need to pass the function
	print("------Test Data ----")
	print(list_images)
	
	#call the function process_images and load your list of images
	print("\n------Result Data ----")
	print(json.dumps(process_images(list_images), indent=2))
	
