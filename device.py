"""
This module represents a device.

Computer Systems Architecture Course
Assignment 1
March 2016
"""

from threading import Event, Thread, Semaphore, Lock, Condition




class MyCondition():
	
	def __init__(self, num_threads):
		self.condition = Condition()
		self.num_threads = num_threads
		self.current_thread = 0;
		
	def decrease_num_threads(self):
		self.num_threads -= 1;
	def acquire(self):
		self.condition.acquire()
		self.current_thread = self.current_thread + 1
		
	def release(self):
		self.condition.release()
		self.current_thread = self.current_thread - 1
		
	def is_full(self):
		if self.current_thread == self.num_threads:
			return 1
		else:
			return 0
			
	def wait(self):
		self.condition.wait()
		
	def notifyAll(self):
		self.condition.notifyAll()
		
			
class ScriptThread(Thread):
	
	def __init__(self, device):
		Thread.__init__(self)
		self.neighbours = []
		self.location = 0
		self.parent_device = device
		self.script = None
		
	def update_data(self, neighbours, location, script):
		self.neighbours = neighbours
		self.location = location
		self.script = script
		
	def run(self):
		if self.script is not None:
			with self.parent_device.location_locks[self.location]:
				script_data = []
				# collect data from current neighbours
				for device in self.neighbours:
					data = device.get_data(self.location)
					if data is not None:
						script_data.append(data)
				# add our data, if any
				data = self.parent_device.get_data(self.location)
				if data is not None:
					script_data.append(data)
				
				
				if script_data != []:
					# run script on data
					result = self.script.run(script_data)
					
					with self.parent_device.neigh_lock:
						for device in self.neighbours:
							device.set_data(self.location, result)

				self.parent_device.semaphore.release()
				self.parent_device.num_threads = self.parent_device.num_threads - 1

		
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
		self.location_locks = [Lock()] * 30
		
		for i in range(0, 8):
			self.scriptThreads.append(ScriptThread(self))
			self.scriptThreads[i].start()
		
		self.semaphore = Semaphore()
		self.neighbours_condition = MyCondition(-1)
		self.lock = Lock();
		self.neigh_lock = None
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
		if self.device_id == 0:
			self.semaphore = Semaphore(8)
			self.neighbours_condition = MyCondition(len(devices))
			self.location_locks = 25 * [Lock()]
			self.neigh_lock = Lock()
			for device in devices:
				device.semaphore = self.semaphore
				device.neighbours_condition = self.neighbours_condition
				device.location_locks = self.location_locks
				device.neigh_lock = self.neigh_lock
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

	def get_locks_location(self, location):
		return self.location_locks[location]
		
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
				self.device.neighbours_condition.acquire()
				neighbours = self.device.supervisor.get_neighbours()
				if neighbours is None:
					self.device.neighbours_condition.release()
					self.device.neighbours_condition.decrease_num_threads()
					break
				res = self.device.neighbours_condition.is_full()
				if res == 1:
					self.device.neighbours_condition.notifyAll()
				else:
					self.device.neighbours_condition.wait()				
				self.device.neighbours_condition.release()
				
			self.device.timepoint_done.wait()
			
			# run scripts received until now
			for (script, location) in self.device.scripts:
				self.device.semaphore.acquire()
				self.device.scriptThreads[self.device.num_threads].update_data(neighbours, location, script)
				self.device.scriptThreads[self.device.num_threads].run()
				self.device.num_threads = self.device.num_threads+1;

			
		self.device.timepoint_done.clear()
			