"""
This module represents a device.

Computer Systems Architecture Course
Assignment 1
March 2016
"""

from threading import Thread, Lock, Event, Condition, Semaphore

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
        @param sensor_data: a list containing
        (location, data) as measured by this device

        @type supervisor: Supervisor
        @param supervisor: the testing infrastructure's
         control and validation component
        """

        self.device_id = device_id
        self.sensor_data = sensor_data
        self.supervisor = supervisor
        self.scripts = []
        self.timepoint_done = Event()
        self.setup_event = Event()

        self.lock_location = []
        self.lock_n = Lock()
        self.barrier = None

        self.thread_script = []
        self.num_thread = 0
        self.sem = Semaphore(value=8)

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
        # give to every device a list of locks for locations
        # and share the same barrier with each device
        if self.device_id == 0:
            barrier = ReusableBarrier(len(devices))
            for _ in xrange(25):
                self.lock_location.append(Lock())

            for dev in devices:
                dev.barrier = barrier
                dev.lock_location = self.lock_location
                dev.setup_event.set()

    def assign_script(self, script, location):
        """
        Provide a script for the device to execute.

        @type script: Script
        @param script: the script to execute from
        now on at each timepoint; None if the
            current timepoint has ended

        @type location: Integer
        @param location: the location for which the script
        is interested in
        """
        if script is not None:
            self.scripts.append((script, location))
        else:
            self.timepoint_done.set()

    def get_data(self, location):
        """
        Returns the pollution value this device has for
         the given location.

        @type location: Integer
        @param location: a location for which obtain the data

        @rtype: Float
        @return: the pollution value
        """
        return self.sensor_data[location] if location in \
            self.sensor_data else None

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

    def shutdown_script(self):
        """
        Join all active threads within a thread_script
        """
        for i in xrange(self.num_thread):
            self.thread_script[i].join()

        for i in xrange(self.num_thread):
            del self.thread_script[-1]

        self.num_thread = 0

class NewThreadScript(Thread):
    """
    Class used to apply script on data
    """
    def __init__(self, parent, neighbours, location, script):
        Thread.__init__(self)
        self.neighbours = neighbours
        self.parent = parent
        self.location = location
        self.script = script

    def run(self):
        with self.parent.lock_location[self.location]:
            script_data = []
            # collect data from current neighbours
            for device in self.neighbours:
                data = device.get_data(self.location)
                if data is not None:
                    script_data.append(data)
            # add our data, if any
            data = self.parent.get_data(self.location)
            if data is not None:
                script_data.append(data)

            if script_data != []:
                # run script on data
                result = self.script.run(script_data)

                # update data of neighbours
                for device in self.neighbours:
                    device.set_data(self.location, result)
                # update our data
                self.parent.set_data(self.location, result)
            self.parent.sem.release()

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

        # setup devices before starting the process
        self.device.setup_event.wait()

        while True:
            # get the current neighbourhood
            with self.device.lock_n:
                neighbours = self.device.supervisor.get_neighbours()
                if neighbours is None:
                    print "Iese ", self.device.device_id
                    break

            # wait all scripts to be received
            self.device.timepoint_done.wait()

            # run scripts received until now
            for (script, location) in self.device.scripts:
                self.device.sem.acquire()
                self.device.thread_script.append(NewThreadScript \
                    (self.device, neighbours, location, script))

                self.device.num_thread = self.device.num_thread + 1
                self.device.thread_script[-1].start()



            # join current threads
            self.device.shutdown_script()
            # clear timepoint event
            self.device.timepoint_done.clear()
            # wait for all devices to finish last step
            self.device.barrier.wait()