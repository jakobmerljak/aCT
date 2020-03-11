#!/usr/bin/python
#Compatibility changes on slsXML.py file
import xml.dom.minidom
import datetime
import sys

class xml_doc:

    def __init__(self) :
        # print ("Init of object")
        # init object data, create base common doc. header, get current timestamp
        self.info = {}
        self.data = []
        self.interventions = []
        self.info['timestamp'] = self.timestring()
        self.doc = xml.dom.minidom.Document()
        self.create_header()

    def create_header(self) :
        # print ("Creating header info")
        self.mainchild = self.doc.createElement("serviceupdate")
        self.mainchild.setAttribute("xmlns", 'http://sls.cern.ch/SLS/XML/update')
        self.doc.appendChild(self.mainchild)

    def set_id(self, id_info) :
        self.info['id'] = id_info

    def set_status(self, av_info) :
        self.info['status'] = av_info

    #def set_availability(self, av_info) :
    #    self.info['availability'] = av_info

    def add_info(self, name, value):
        self.info[name] = value
        self.append_value(name)

    def add_data(self, name, desc, value) :
        tmp_dict = {}
        tmp_dict['name'] = name
        tmp_dict['desc'] = desc
        tmp_dict['value'] = value
        self.data.append( tmp_dict )

    def add_intervention(self, start, length, message) :
        tmp_dict = {}
        tmp_dict['start'] = start
        tmp_dict['length'] = length
        tmp_dict['message'] = message
        self.interventions.append(tmp_dict)

    def timestring(self) :
        currenttime = datetime.datetime.now()

        tmpstring = '%04d-%02d-%02dT%02d:%02d:%02d' % (currenttime.year,
            currenttime.month, currenttime.day, currenttime.hour,
            currenttime.minute, currenttime.second)

        return tmpstring

    def append_value(self, value) :
        if(value in self.info) :
            valelem = self.doc.createElement(value)
            valtext = self.doc.createTextNode(self.info[value])
            valelem.appendChild(valtext)
            self.mainchild.appendChild(valelem)
            return 1
        else :
            print('Err: need to define the %s value with set_%s.' % (value, value))
            return 0

    def append_data(self) :
        if(len(self.data) > 0) :
            dataelem = self.doc.createElement('data')

            for tmp_dict in self.data :
                numelem = self.doc.createElement('numericvalue')
                numelem.setAttribute("name", tmp_dict['name'])
                numelem.setAttribute("desc", tmp_dict['desc'])
                numtext = self.doc.createTextNode(tmp_dict['value'])
                numelem.appendChild(numtext)
                dataelem.appendChild(numelem)

            self.mainchild.appendChild(dataelem)
        return 1

    def append_interventions(self) :

        if(len(self.interventions) > 0) :
            dataelem = self.doc.createElement('interventions')

            for tmp_dict in self.interventions :
                numelem = self.doc.createElement('intervention')
                numelem.setAttribute("start", tmp_dict['start'])
                numelem.setAttribute("length", tmp_dict['length'])
                numtext = self.doc.createTextNode(tmp_dict['message'])
                numelem.appendChild(numtext)
                dataelem.appendChild(numelem)

            self.mainchild.appendChild(dataelem)
        return 1

    def print_xml(self) :
        # build the xml from the object data info
        err = self.append_value('id')
        if err == 0 : return

        #err = self.append_value('availability')
        #if err == 0 : return

        err = self.append_value('status')
        if err == 0 : return

        err = self.append_interventions()
        if err == 0 : return

        err = self.append_data()
        if err == 0 : return

        err = self.append_value('timestamp')
        if err == 0 : return

        # return self.doc.toprettyxml()
        return self.doc.toxml()

