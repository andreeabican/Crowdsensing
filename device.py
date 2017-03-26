"""
This module represents a device.

Bican Andreea Sanziana,333CC
Computer Systems Architecture Course
Assignment 1
March 2016
"""

from threading import Event, Thread, Lock, Condition
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
    """
    Device's worker thread
    """
    def __init__(self, scripts_buffer, device):
        """
        Constructor
        
        @type scripts_buffer: Queue
        @param scripts_buffer: buffer that stores the jobs which have to be
        executed by workers
        
        @type device: Device
        @param device: the device that the thread belongs to
        """
        Thread.__init__(self)
        self.device = device
        self.script_buffer = scripts_buffer
        
    def get_script_data(self, job):
        """
        Get script data from neighbours
        
        @type job: Job
        @param job: job that needs to be executed at the moment
        """
        script_data = []
        for device in job.neighbours:
            data = device.get_data(job.location)
            if data is not None:
                script_data.append(data)
        # add our data, if any
        data = self.device.get_data(job.location)
    
        if data is not None:
            script_data.append(data)
        return script_data
        
    def update_data_on_neighbours(self, job, result):
        """
        Update the data on neighbours
        
        @type job: Job
        @param job: Object that stores all the information about location and
        neighbours
        
        @type result: list
        @param result: Contains the data that needs to be updated on neighbours
        """
        for device in job.neighbours:
            device.set_data(job.location, result)
        self.device.set_data(job.location, result)
    
    def run(self):
        while True:
        # get job out of Queue
            job = self.script_buffer.get()
            # end task if there are no more scripts
            if job.script is None:
                self.script_buffer.task_done()
                break
            
            # operation which needs to be executed one per location
            with self.device.sync.get_location_lock(job.location):
                script_data = self.get_script_data(job)
                
                if script_data != []:
                    # run script on data
                    result = job.script.run(script_data)    
                    self.update_data_on_neighbours(job, result)
            
            # notify the queue that the current task is done
            self.script_buffer.task_done()      

class WorkerPool(object):
    """
    Class that contains the logic of the 8 worker threads
    """
    
    def __init__(self, workers, device):
        """
        Constructor
        
        @type workers: Integer
        @param workers: number of workers which need to be managed
        
        @type device: Device
        @param device: device that the worker pool belongs to
        """
        self.workers = workers
        self.workers_scripts = []
        self.scripts_buffer = Queue.Queue()
        self.device = device
        self.start_workers()
        
    def start_workers(self):
        """
        Starts the worker threads
        """
        for i in range(0, self.workers):
            self.workers_scripts.append(Worker(self.scripts_buffer, 
                                               self.device))
            self.workers_scripts[i].start()
            
    def add_job(self, job):
        """
        Adds in buffer a job that needs to be executed
        """
        self.scripts_buffer.put(job)

    def delete_workers(self):
        """
        Delete all workers threads
        """
        for _ in (0, self.workers-1):
            del self.workers_scripts[-1]
            
    def join_workers(self):
        """
        Join workers and buffer queue
        """
        for i in (0, self.workers-1):
            self.scripts_buffer.join()
            self.workers_scripts[i].join()
            
    def make_workers_stop(self):
        """
        Make workers threads stop by sending them a NULL job
        """
        for _ in range(0, 8):
            self.add_job(Job(None, None, None))
        self.join_workers()
        

class Job():
    """
    Class that stores the job data needed by workers
    """
    def __init__(self, neighbours, script, location):
        """
        Constructor.
        
        @type neighbours: List of Integers
        @param neighbours: a list containing the neighbours of the current 
        device during a timepoint
        
        @type script: Script
        @param script: script that needs to be executed by the device in the
        current job
        
        @type location: Integer
        @param location: location of the device during the current timepoint
        """
        self.neighbours = neighbours
        self.script = script
        self.location = location

    def get_neighbours(self):
        """
        Returns neighbours of the current job
        
        @rtype: List of Integers
        @return: a list of the neighbours
        """
        return self.neighbours
    
    def get_script(self):
        """
        Returns script of the current job
        
        @rtype: Script
        @return: the script
        """
        return self.script
     
class DeviceSync(object):
    """
    Class that contains syncronization elements for a device
    """
    def __init__(self):
        self.setup = Event()
        self.timepoint_done = Event()
        self.location_locks = []
        self.barrier = None
        
    def init_location_locks(self, locations):
        for _ in range(0, locations):
            self.location_locks.append(Lock())
            
    def init_barrier(self, threads):
        self.barrier = ReusableBarrier(threads)
        
    def set_setup_event(self):
        self.setup.set()
        
    def wait_setup_event(self):
        self.setup.wait()
        
    def set_timepoint_done(self):
        self.timepoint_done.set()
        
    def wait_timepoint_done(self):
        self.timepoint_done.wait()
        
    def clear_timepoint_done(self):
        self.timepoint_done.clear()
        
    def wait_threads(self):
        self.barrier.wait()
        
    def get_location_lock(self, location):
        return self.location_locks[location]
        
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
        
        self.sync = DeviceSync()
        self.worker_pool = WorkerPool(8, self)
        
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
        if self.device_id == len(devices)-1:
            self.sync.init_location_locks(25)
            self.sync.init_barrier(len(devices))
            for device in devices:
                device.sync.barrier = self.sync.barrier
                device.sync.location_locks = self.sync.location_locks
                device.sync.set_setup_event()
            
    def add_job(self, job):
        """
        Add a job in the worker pool
        """
        self.worker_pool.add_job(job)
        
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
        else:
            self.sync.set_timepoint_done()

    def get_data(self, location):
        """
        Returns the pollution value this device has for the given location.

        @type location: Integer
        @param location: a location for which obtain the data

        @rtype: Float
        @return: the pollution value
        """
        if location in self.sensor_data:
            return self.sensor_data[location]
        else:
            return None

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
        # wait for all devices to have the setup done
        self.device.sync.wait_setup_event()
        while True: 
            # get the current neighbourhood
            neighbours = self.device.supervisor.get_neighbours()
            # stop the device threads if there are no neighbours
            if neighbours is None:
                self.device.worker_pool.make_workers_stop()
                break
            
            # wait for all device threads to get neighbours
            self.device.sync.wait_threads()
            self.device.sync.wait_timepoint_done()
            
            # add the scripts receives in current device's worker pool
            for (script, location) in self.device.scripts:
                self.device.add_job(Job(neighbours, script, location))
            
            # wait for all devices threads to run scripts
            self.device.sync.wait_threads()
            self.device.sync.clear_timepoint_done()