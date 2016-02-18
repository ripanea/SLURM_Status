#!/usr/bin/python
#
# Copyright (C) 2015, Razvan Panea
# Contact: razvan.panea@duke.edu

from subprocess import Popen, PIPE

class Job(object):
    """
    Class that contains all the information about one job.
    """

    def __init__(self):
        self.job_id     = 0
        self.username   = ""

        self.is_running = False

        self.nodes      = []
        self.cpus       = 0
        self.mem        = 0

        self.type       = ""

def getType(node):
    """
    Function that returns a given node's type.
    """

    if "node09" in node or "node10" in node:
        return "himem"

    elif "node01-1" in node:
        return "interactive"

    else:
        return "lowmem"

def getNodes(nodelist):
    """
    Function that expands the compacted node list.
    
    Example:
        Input:  "node01-[1-3],node02-[1,2-4]"
        Output: [node01-1, node01-2, node01-3, node02-1, node02-2, node02-3, node02-4]
    """

    l = []

    # Replacing "," between node sets for correct splitting
    nodelist = nodelist.replace(",har", "*har")

    # Splitting the list in smaller sets
    for nodeset in nodelist.split("*"):
        if "[" in nodeset:
            head    = nodeset.split("[")[0]
            rang    = nodeset.split("[")[-1].split("]")[0]

            # Splitting ranges in subranges
            for sub_rang in rang.split(","):
                if "-" in sub_rang:
                    limits = [int(val) for val in sub_rang.split("-")]
                    for i in range(limits[0], limits[1] + 1):
                        l.append(head + str(i))
                else:
                    l.append(head + sub_rang)
        else:
            l.append(nodeset)

    return l

# Obtaining the list of jobs from slurm queue
queue   = Popen("squeue --array --noheader -o \"%i %u %N %c %m\"", stdout=PIPE, shell=True)
output  = queue.communicate()[0]

# Generating the Job instances
jobs    = []
for line in output.strip("\n").split("\n"):
    data        = line.split(" ")
    
    # Filtering empty lines
    if len(data) < 5:
        continue

    # Creating a new instance and importing data
    job             = Job()
    job.job_id      = data[0]
    job.username    = data[1]
    job.is_running  = data[2] != ""
    job.nodes       = getNodes(data[2])
    job.type   = getType(job.nodes[0])
    job.cpus        = int(data[3])
    if "G" in data[4]:
        job.mem = float(data[4].strip("G"))
    else:
        job.mem = float(data[4].strip("M"))/1000

    jobs.append(job)

# Obtaining the list of users
users   = []
for job in jobs:
    if job.username not in users:
        users.append(job.username)

users.sort()

# Printing information based on user
print("\n*** User Based (Lowmem/Himem/Interactive) ***\n")

# Printing header
print("%8s\t%8s\t%8s\t%11s\t%11s\t%20s" % ("USER", "RUNNING", "PENDING", "NODES(L/H/I)", "CPUS(L/H/I)", "MEM(GB)(L/H/I)"))

# Printing information for each user
for user in users:

    # Creating a list of user's jobs
    user_jobs   = [job for job in jobs if job.username == user]

    # Obtaining the number of pending and running jobs
    running     = 0
    pending     = 0    
    for job in user_jobs:
        if job.is_running:
            running += 1
        else:
            pending += 1 

    # Obtaining the number of nodes "touched"
    nodes_hi    = set()     # himem
    nodes_lo    = set()     # lowmem
    nodes_in    = set()     # interactive
    for job in user_jobs:
        if job.is_running:
             for node in job.nodes:
                if   job.type == "himem":
                    nodes_hi.add(node)
                elif job.type == "lowmem":
                    nodes_lo.add(node)
                elif job.type == "interactive":
                    nodes_in.add(node)
    nodes_hi = len(nodes_hi)
    nodes_lo = len(nodes_lo)
    nodes_in = len(nodes_in)

    # Obtaining the number of CPUs and Memory allocated
    cpus_hi     = 0
    cpus_lo     = 0
    cpus_in     = 0
    for job in user_jobs:
        if job.is_running:
            if   job.type == "himem":
                cpus_hi += job.cpus*len(job.nodes)
            elif job.type == "lowmem":
                cpus_lo += job.cpus*len(job.nodes)
            elif job.type == "interactive":
                cpus_in += job.cpus*len(job.nodes)

    # Obtaining the number of GB RAM allocated
    mem_hi      = 0
    mem_lo      = 0
    mem_in      = 0
    for job in user_jobs:
        if job.is_running:
            if   job.type== "himem":
                mem_hi  += job.mem*len(job.nodes)
            elif job.type == "lowmem":
                mem_lo  += job.mem*len(job.nodes)
            elif job.type == "interactive":
                mem_in  += job.mem*len(job.nodes)

    # Printing the data
    print("%8s\t%8s\t%8s\t%3s/%3s/%3s\t%3s/%3s/%3s\t%6.1f/%6.1f/%6.1f" % (user, running, pending, nodes_lo, nodes_hi, nodes_in, cpus_lo, cpus_hi, cpus_in, mem_lo, mem_hi, mem_in))

# Obtaining the nodes info
info    = Popen("scontrol show nodes --oneliner", stdout=PIPE, shell=True)
output  = info.communicate()[0]

# Printing information based on nodes
print("\n\n*** Node Based ***\n")

# Printing header
print("%15s\t%15s\t%15s\t%15s\t  %s" % ('NAME', 'TYPE', 'CPU_USAGE', 'MEM_USAGE', 'STATUS'))

# Printing information for each node
for line in output.strip("\n").split("\n"):

    # Converting node info to dict
    data            = {}
    settings_list   = line.split()
    last_setting    = None
    for i in range(len(settings_list)):
        if "=" in settings_list[i]:
            info = settings_list[i].split("=")
            data[info[0]] = info[1]
            last_setting = info[0]
        else:
            data[last_setting] += " " + settings_list[i]

    # Obtaining node information
    node        = data["NodeName"] 
    type        = getType(node)
    is_down     = data["State"] == "DOWN"
    
    # Obtaining CPU usage on node
    cpus        = int(data["CPUAlloc"])
    cpus_u      = cpus * 100 /int(data["CPUTot"])

    # Obtaining memory usage on node    
    mem         = int(data["AllocMem"]) / 1024.0
    mem_u       = mem * 1024 * 100.0 / int(data["RealMemory"])

    # Generating the status
    red     = "\033[31m"
    green   = "\033[32m"
    blue    = "\033[34m"
    reset   = "\033[0m"

    if is_down:
        status = red + "DOWN" + reset
    elif cpus_u > 95:
        status = red + "FULL(CPU)" + reset
    elif mem_u > 95:
        status = red + "FULL(MEM)" + reset
    elif cpus_u == 0 and mem_u == 0:
        status = green + "FREE" + reset
    else:
        status = blue + "BUSY" + reset

    # Printing the data
    print("%15s\t%15s\t%8d (%3d%%)\t%8.2f (%3d%%)\t  %s" % (node, type, cpus, cpus_u, mem, mem_u, status))
