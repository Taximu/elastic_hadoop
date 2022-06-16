import xml.etree.ElementTree as ET
from xml.etree.ElementTree import SubElement

wrong_value = 'EMPTY_VALUE'

class JobResourceAllocator:
    'Class to create, edit, delete queue.'
    
    configFile = '/mnt/maxim/hadoop/etc/hadoop/fair-scheduler.xml'

    def __init__(self, configs):
        if not self.existsQueue(configs['queueName']):
            self.queueName = configs.get('queueName', wrong_value)
            self.minResources = configs.get('minResources', wrong_value)
            self.maxResources = configs.get('maxResources', wrong_value)
            self.maxRunningApps = configs.get('maxRunningApps', wrong_value)
            self.weight = configs.get('weight', wrong_value)
            self.schedulingPolicy = configs.get('schedulingPolicy', wrong_value)
            self.writeQueueToXml()
        else:
            raise ValueError("Queue %s exists already." % configs['queueName'])

    @staticmethod
    def existsQueue(queueName):
        '''Checks existence of queue'''
        tree = ET.parse(JobResourceAllocator.configFile)
        root = tree.getroot()
        for queue in root.findall('queue'):
            if queue.attrib['name'] == queueName:
                return True
        return False
    
    def writeQueueToXml(self):
        '''Writes new queue and its parameters to xml'''
        tree = ET.parse(JobResourceAllocator.configFile)
        root = tree.getroot()
        queue = ET.Element("queue")
        queue.attrib["name"] = self.queueName
        root.insert(0, queue)
        minResources = ET.SubElement(queue, "minResources")
        minResources.text = self.minResources
        maxResources = ET.SubElement(queue, "maxResources")
        maxResources.text = self.maxResources
        maxRunningApps = ET.SubElement(queue, "maxRunningApps")
        maxRunningApps.text = self.maxRunningApps
        weight = ET.SubElement(queue, "weight")
        weight.text = self.weight
        schedulPolicy = ET.SubElement(queue, "schedulingPolicy")
        schedulPolicy.text = self.schedulingPolicy
        tree.write(JobResourceAllocator.configFile)

    @staticmethod
    def editQueue(queueName, configs):
        '''Edits queue parameters'''
        tree = ET.parse(JobResourceAllocator.configFile)
        root = tree.getroot()
        for child in root:
            if child.tag == 'queue' and child.attrib['name'] == queueName:
                for key, value in configs.iteritems():
                    for subchild in child:
                        if subchild.tag == key:
                            subchild.text = value
                print 'Queue ' + child.attrib['name'] + ' updated.'
                tree.write(JobResourceAllocator.configFile)
    
    @staticmethod
    def deleteQueueFromXml(queueName):
        '''Deletes queue from fair-scheduler.xml'''
        tree = ET.parse(JobResourceAllocator.configFile)
        root = tree.getroot()
        for child in root:
            if child.tag == 'queue' and child.attrib['name'] == queueName:
                root.remove(child)
                print 'Queue ' + child.attrib['name'] + ' deleted.'
                tree.write(JobResourceAllocator.configFile)
    
    @staticmethod
    def displayQueue(queueName):
        '''Displays queue parameters'''
        tree = ET.parse(JobResourceAllocator.configFile)
        root = tree.getroot()
        for child in root:
            if child.tag == 'queue' and child.attrib['name'] == queueName:
                print 'Queue ' + queueName + ' parameters:'
                for subchild in child:
                    print subchild.tag + ' : ' + subchild.text
