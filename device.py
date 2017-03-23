"""
This module represents a device.

Computer Systems Architecture Course
Assignment 1
March 2016
"""

from threading import Event, Thread, Lock, Condition, BoundedSemaphore
import Queue

class ReusableBarrier():
    """
    Reusuable Condition Barrier
    """
    def __init__(self, num_threads):
        """
        Constructor
        """
        self.num_threads = num_threads
        self.count_threads = self.num_threads
        self.cond = Condition()

    def wait(self):
        """
        Wait method
        """
        self.cond.acquire()
        self.count_threads -= 1
        if self.count_threads == 0:
            self.cond.notify_all()
            self.count_threads = self.num_threads
        else:
            self.cond.wait()
        self.cond.release()

				
class Worker(Thread):
	
	def __init__(self, scripts_buffer, device, id):
		Thread.__init__(self)
		self.device = device
		self.script_buffer = scripts_buffer
		self.id = id
		
	def run(self):
		while True:
			(neighbours, script, location) = self.script_buffer.get()
			if script is None:
				self.script_buffer.task_done()
				break
			with self.device.location_locks[location]:
				script_data = []
				# collect data from current neighbours
				for device in neighbours:
					data = device.get_data(location)
					if data is not None:
						script_data.append(data)
				# add our data, if any
				data = self.device.get_data(location)
			
				if data is not None:
					script_data.append(data)
			
				if script_data != []:
					# run script on data
					result = script.run(script_data)
				
					for device in neighbours:
						device.set_data(location, result)
					self.device.set_data(location, result)
			self.script_buffer.task_done()			
			self.device.semaphore.release()

		

class WorkerPool(object):
	
	def __init__(self, workers, device):
		self.workers = workers
		self.workers_scripts = []
		self.scripts_buffer = Queue.Queue()
		for i in range(0, workers):
			self.workers_scripts.append(Worker(self.scripts_buffer, device, i))
			self.workers_scripts[-1].start()
		
	def add_script(self, neighbours, script, location):
		self.scripts_buffer.put((neighbours, script, location))

	
	def join_workers(self):
		for i in (0, self.workers-1):
			self.scripts_buffer.join()
			self.workers_scripts[i].join()
		for i in (0, self.workers-1):
			del self.workers_scripts[-1]

		
class Device(object):
	"""
	Class that represents a device.
	"""
	def __init__(self, device_id, sensor_data, supervisor):
		"""
		Constructor.

		@type device_id: Integer
		@param device_id: the unique id of this node; between 0 and N-1

		@type sensor_data: List of (Integer, Float)
		@param sensor_data: a list containing (location, data) as measured by this device

		@type supervisor: Supervisor
		@param supervisor: the testing infrastructure's control and validation component
		"""
		self.device_id = device_id
		self.sensor_data = sensor_data
		self.supervisor = supervisor
		self.scripts = []
		self.num_threads = 0;
		
		self.script_received = Event()
		self.setup = Event()
		self.timepoint_done = Event()
		self.scriptThreads = []
		self.location_locks = []
		self.worker_pool = WorkerPool(8, self)
		
		self.semaphore = BoundedSemaphore(8)
		self.lock = Lock();
		self.barrier = None;
		self.thread = DeviceThread(self)
		self.thread.start()

	def __str__(self):
		"""
		Pretty prints this device.

		@rtype: String
		@return: a string containing the id of this device
		"""
		return "Device %d" % self.device_id

	def setup_devices(self, devices):
		"""
		Setup the devices before simulation begins.

		@type devices: List of Device
		@param devices: list containing all devices
		"""
		# we don't need no stinkin' setup
		if self.device_id == len(devices)-1:
			for i in range(0, 25):
				self.location_locks.append(Lock())
			self.barrier = ReusableBarrier(len(devices))
			for device in devices:
				device.barrier = self.barrier
				device.location_locks = self.location_locks
				device.setup.set()

		pass

	def assign_script(self, script, location):
		"""
		Provide a script for the device to execute.

		@type script: Script
		@param script: the script to execute from now on at each timepoint; None if the
		current timepoint has ended

		@type location: Integer
		@param location: the location for which the script is interested in
		"""
		if script is not None:
			self.scripts.append((script, location))
			self.script_received.set()
		else:
			self.timepoint_done.set()

	def get_data(self, location):
		"""
		Returns the pollution value this device has for the given location.

		@type location: Integer
		@param location: a location for which obtain the data

		@rtype: Float
		@return: the pollution value
		"""
		return self.sensor_data[location] if location in self.sensor_data else None

	def set_data(self, location, data):
		"""
		Sets the pollution value stored by this device for the given location.

		@type location: Integer
		@param location: a location for which to set the data

		@type data: Float
		@param data: the pollution value
		"""
		if location in self.sensor_data:
			self.sensor_data[location] = data

		
	def shutdown(self):
		"""
		Instructs the device to shutdown (terminate all threads). This method
		is invoked by the tester. This method must block until all the threads
		started by this device terminate.
		"""
		self.thread.join()

class DeviceThread(Thread):
	"""
	Class that implements the device's worker thread.
	"""
	def __init__(self, device):
		"""
		Constructor.

		@type device: Device
		@param device: the device which owns this thread
		"""
		Thread.__init__(self, name="Device Thread %d" % device.device_id)
		self.device = device
	

	def run(self):
		self.device.setup.wait()
		# hope there is only one timepoint, as multiple iterations of the loop are not supported
		while True:	
			# get the current neighbourhood
			with self.device.lock:
				neighbours = self.device.supervisor.get_neighbours()
				if neighbours is None:
					for i in range(0, 8):
						self.device.worker_pool.add_script(None, None, None)
					self.device.worker_pool.join_workers()
					break
			self.device.barrier.wait()
			self.device.timepoint_done.wait()
			# run scripts received until now
			for (script, location) in self.device.scripts:
				self.device.semaphore.acquire()
				self.device.worker_pool.add_script(neighbours, script, location)
				
			self.device.barrier.wait()
			
			self.device.timepoint_done.clear()