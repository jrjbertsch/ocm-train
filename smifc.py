#! /usr/bin/python3
########################################################################################
########################################################################################
##### James R. Bertsch Copyright. All rights reserved. #####
##### jbertsch@ourcaingmatters.org                    #####
########################################################################################
########################################################################################

import argparse
import base64
import boto3
import botocore
import io
from libtiff import TIFF
from lxml import etree as ET 
import os
from pathlib import Path
import pdf2image
from PIL import Image
import re
import sys

class storageMgr_format_action(argparse.Action):
    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        super().__init__(option_strings, dest, **kwargs)

    def __call__(self, parser_mifc, namespace, values, option_string) :
        print('%r %r %r' % (namespace, values, option_string))
        setattr(namespace, self.dest, values)

#    def fs( fmt, path) :
#        print( '%s, %s' %(fmt, path))

#    def s3( fmt, path, acl, bucket, region) :
#        print( '%s, %s, %s, %s, %s' %(fmt, path, acl, bucket, region))

#    def st (fmt, name, stream) :
#        print ('%s, %s, %s'%(fmt, name, stream))


################################################
#############   INITIALIZE CLASS ###############
################################################

##### Initialize Arguments, subcommand parameters, variabes, constants and methods #####

class _initialize() :
    def __init__(self) :

# Instanitiate Pathlib
#        class Path.PurePath(os.environ.get('HOME'))
        PurePath(os.environ.get('HOME'))
##### Initialize Body Forms -- Used by Derived Classes  #####
        self.image = None
        self.text = ''
        self.tree = ''

##### Supported Storage-Format Patterns #####
        self._gif_pattern = '[.]?[Gg][Ii][Ff]'
        self._jpeg_pattern = '[.]?[Jj][Pp][Ee]?[Gg]'
        self._nb_pattern = '[.]?[Nn][Bb]'
        self._png_pattern = '[.]?[Pp][Nn][Gg]'
        self._rgb_pattern = '[.]?[Rr][Gg][Bb]'
        self._text_pattern = '[.]?[Tt][Ee]?[Xx][Tt]'
        self._tiff_pattern = '[.]?[Tt][Ii][Ff]?[Ff]]'
        self._xml_pattern = '[.]?[Xx][Mm][Ll]'

        self._supported_atomic_formats = [ self._gif_pattern, self._nb_pattern, self._png_pattern, self._rgb_pattern, self._text_pattern, self._tiff_pattern ]
        self._supported_complex_formats = [ self._xml_pattern ]
        self._supported_image_formats = [ self._gif_pattern, self._png_pattern, self._rgb_pattern, self._tiff_pattern ]
        self._supported_formats = supported_atomic_formats + supported_complex_formats 
        self._supported_structured_data = [ self._xml_pattern ]
        self._supported_text_streams = [ self._nb_pattern, self._text_pattern ]

#####  Supported Storage-Type Patterns #####
        self._posixFS_storageMgr_pattern = '[Ff][Ii][Ll][Ee]'
        self._s3_storageMgr_pattern = '[Ss][3]'
        self._st_storageMgr_pattern = '[Ss][Tt]'
    
        self._supported_storageMgr_patterns =  [ self._posixFS_storageMgr_pattern, self._s3_storageMgr_pattern ]

#####  Complex Storage-Format Associations #####
        self.__set_associates()
#        format.put([[File-path], [Y/N], ...])

#####  Exceptions #####
        self._exceptions['fatal']['format'] = 'Critical Failure: Unsupported file-format.'
        self._exceptions['fatal']['gif'] = 'Critical Failure: Not a valid <.gif> file extension.'
        self._exceptions['fatal']['jpeg'] = 'Critical Failure: Not a valid <.jpeg> file extension.'
        self._exceptions['fatal']['nb'] = 'Critical Failure: Not a valid <.nb> file extension.'
        self._exceptions['fatal']['nopath'] = 'Critical Failure:No path.'
        self._exceptions['fatal']['png'] = 'Critical Failure: Not a valid <.png> file extension.'
        self._exceptions['fatal']['rgb'] = 'Critical Failure: Not a valid <.rgb> file extension.'
        self._exceptions['fatal']['text'] = 'Critical Failure: Not a valid <.text> file extension.'
        self._exceptions['fatal']['tiff'] = 'Critical Failure: Not a valid <.tiff> file extension.'
        self._exceptions['fatal']['unknown format'] = 'Critical Failure: Unknown file-format.'
        self._exceptions['fatal']['xml'] = 'Critical Failure: Not a valid <.xml> file extension.'
        self._exceptions['fatal']['storageMgr'] = 'Critical Failure: Unsupported storage-manager.'
        self._exceptions['Message']['put usage'] = '%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n'%(
            'Name: Storage Format: Put',
            'Purpose: Puts a set of formatted data onto the current storage-manager.',
            'Minimal requirements: <<format instance>,<Storage_manager>>,<data available>,<format>',
            'Usage 1: storageMgr_format(<storageMgr>, <[**kwargs]>).put([*args])',
            'Usage 2: format.put([*args])',
            'storageMgr_format(<"file-sys">, <[FORMAT=], [PATH=]>).put(<file-path: Default="existing path">, [Y/N])',
            'storageMgr_format(<"s3">, <<ACL= :Default="public-read">, <BUCKET=>, <[FORMAT=], [PATH=]>)',
            '    <REGION= :Default="us-east-2">, [ROOT_DIR=]>).put([uri: Default="existing uri]" '
            '<File-or-uri-path>: - new path or uri, default="existing path or uri"',
            '[Y/N] - Replace existing file? -- Default [No].')
        self._exceptions['Message']['usage'] = '%s\n%s\n%s\n%s\n'%(
            'Name:  Storage Format'
            'Purpose: get, getstream, put, putstream methods retrieve and store formatted data at the storage-manager'
            'Usage:storageMgr_format(<storageMgr>, <[**kwargs]>),'
            'storageMgr_format(<"file">, <[FORMAT=], [PATH=]>)',
            'storageMgr_format(<"s3">, <<ACL= :Default="public-read">, <BUCKET=>, <[FORMAT=], [PATH=]>',
            '    <REGION= :Default="us-east-2">, [ROOT_DIR=]>)')
        self._exceptions['message']['search'] = '%s %s'%('Message: searching at:', self.path) 
        self._exceptions['warning']['put'] = 'Warning: Data missing, nothing "put"' 
        self._exceptions['warning']['args'] = 'Warning: Extra arguments were ignored!'
        self._exceptions['warning']['s3_not_found'] = '%s::%s/%s::%s'%('Warning: s3 Object',self.bucket,self.key,'not found.')

##### Define Methods #####
    def __del__(self) :
        print ("terminating ...")

    def __set_associates() :       
        self.associates = []

    def __set_jpg() :       
        self._format = "pe".join(self._format.split("p", 1))
        return(self._format)

    def __set_storageMgr_format() :       
        if self.__verify_path() :
            self._format = str(self.path.suffix.rsplit('.',1)).lower()
        elif self.__verify_storageMgr_format() :
            self._format = self._format.lower()
# standardize the accepted but non-standard format names (e.g. jpg,tif,txt) 
        else :
            self._set_format_name = '__%s_%s'%('set',self._format)
            if hasattr(self, self._set_format_name) :
                return (getattr(self, self._set_format_name)()) 
# standard format names (e.g. jpeg,tiff,text)
            else :
                return (self._format)

    def __set_storageMgr() :       
        if self.__verify_storageMgr() :
            self._argv['STORAGE_MGR'] = self._storageMgr = str(self._storageMgr).lower()
    def __set_txt() :       
        self._format = "ex".join(self._format.split("x", 1))
        return(self._format)

    def __set_tif() :       
        self._format = "ff".join(self._format.split("f", 1))
        return (self._format)

    def __verify_storageMgr_format() :
        format_match = False
        for format_pattern in self._supported_formats :
           if re.match(format_pattern, self._format) :
               format_match = True
               break
        return (format_match)  

    def __verify_storageMgr() :
        storageMgr_match = False
        for storageMgr_pattern in self._supported_storageMgr_patterns :
           if re.match(storageMgr_pattern, self._storageMgr) :
               storageMgr_match = True
               break
        return (storageMgr_match)  

################################################
#############   STORAGE_MGR-FORMAT CLASS ###########
################################################

##### The StorageMgr Format class is the Main class and #####
##### the user interface to this class system. #####

class storageMgr_format (_initialize) :
    def __init__(self, *args, **kwargs) :
        super(self).__init__()

##### Receive Arguments #####
        self._storageMgr = args[0]
        if 'posixFS' == self._storageMgr :
            self.path = kwargs.get('PATH', PurePath(''))
            self._format = kwargs.get('(FORMAT','')
        elif 's3' == self._storageMgr :
            self.acl = kwargs.get('ACL','public-read')
            self.bucket = kwargs.get('BUCKET','')
            self._format = kwargs.get('(FORMAT','')
            self.path = kwargs.get('PATH', PurePath(''))
            self.region = kwargs.get('REGION','us-east-2')
            self.root_dir = kwargs.get('ROOT_DIR', PurePath(''))
        else :
            print (self._exceptions['message']['usage'])
            del(self)

##### Initialize Variables #####
        self.__set_storageMgr_format()
        self.__set_storageMgr()
##### Verify Interface #####
        if ( 'posixFS' == self._storageMgr
            and self.__verify_storageMgr_format()) :
##### Instanitate #####
            return (_instantiate_interface(
                self._storageMgr,
                FORMAT = self.data_format,
                PATH = self.path)) 
        elif ( 's3' == self._storageMgr
            and self.__verify_acl()
            and self.__verify_bucket()
            and self.__verify_region()
            and self.__verify_storageMgr_format()) :
                return (_instantiate_interface(
                    self._storageMgr,
                    ACL = self.acl, 
                    BUCKET = self.bucket, 
                    FORMAT = self.data_format,
                    REGION = self.region,
                    PATH = self.path, 
                    ROOT_DIR = self.root_dir))
##### Define Methods #####
        def __del__(self) :
            super().__del__() 

################################################
########## Instantiate Storage Class ###########
################################################

#####  Selects a Storage-Class/File-Format interface based #####
##### on incoming parametrs #####

class  _instantiate_interface(_initialize) :
    def __init__(self, *args, **kwargs) :
        super(self).__init__()
        self._argv['STORAGE_MGR'] = self._storageMgr = args[0]

        self._argv['ACL'] = self.acl = kwargs.get('ACL','public-read')
        self._argv['BUCKET'] = self.bucket = kwargs.get('BUCKET','')
        self._argv['PATH'] = self.path = PurePath(kwargs.get('PATH', ''))
        self._argv['REGION'] = self.region = kwargs.get('REGION','us-east-2')
        self._argv['ROOT_DIR'] = self.root_dir = PurePath(kwargs.get('ROOT_DIR', ''))

        self._format = kwargs.get('(FORMAT','')

##### Initialize Variables #####
        self._argv['FORMAT'] = self.__set_storageMgr_format()
        self._argv['STORAGE_MGR'] = self.__set_storageMgr()

##### Verify Supported Storage and Format Capabilities #####
        if not self.__verify_storageMgr_format() :
            print (self._exceptions['fatal']['format'])
            self.__del__()
        if not self.__verify_storageMgr() :
            print (self._exceptions['fatal']['storageMgr'])
            self.__del__()

##### Instantiate Interface #####
        self.__set_interface_name()
        self.__instantiate_interface()

##### Define Methods #####
    def __del__(self) :
        super().__del__() 

##### Instantiate Interface Class #####
    def __instanitate_interface() :
        if hasattr(self, self._interface_name) :
            return (getattr(self, self._interface_name)(self._argv))
        else :
            return (None)
##### Set Class Name #####
    def __set_interface_name () :
        self._interface_name = ('_%s_%s'%(self._storageMgr, self._format))

###############################################
############# BASE FORMAT CLASS ###############
###############################################

##### Derived from Initialize class static #####
##### Manages file-format associatians #####
##### on incoming parametrs #####

class _stream_format(_initialize) :
    def __init__(self, *args, **kwargs ) :
        super(self).__init__()
        self._argv['STORAGE_MGR'] = self._storageMgr = args[0]

##### Receive Arguments #####
        self._argv['ACL'] = self.acl = kwargs.get('ACL','public-read')
        self._argv['BUCKET'] = self.bucket = kwargs.get('BUCKET','')
        self._argv['FORMAT'] = self._format = kwargs.get('(FORMAT','')
        self._argv['PATH'] = self.path = PurePath(kwargs.get('PATH', ''))
        self._argv['REGION'] = self.region = kwargs.get('REGION','us-east-2')
        self._argv['ROOT_DIR'] = self.root_dir = PurePath(kwargs.get('ROOT_DIR', ''))

##### Set storage Type, and format #####
        self.__set_storageMgr_format()
        self.__set_storageMgr()
##### Define Methods #####
    def __del__(self) :
        super().__del__() 
        print ('Terminating ...')

    def __set_storageMgr_format() :       
        if self.__verify_path() :
            self._argv['FORMAT'] = self._format = str(self.path.suffix.rsplit('.',1)).lower()
        elif self.__verify_storageMgr_format() :
            self._argv['FORMAT'] = self._format = self._format.lower()

    def __set_storageMgr() :       
        if self.__verify_storageMgr() :
            self._argv['STORAGE_MGR'] = self._storageMgr = str(self._storageMgr).lower()

    def __verify_atomic_storageMgr_format() :
        format_match = False
        for format_pattern in supported_atomic_formats :
            if re.match(format_pattern, self._format) :
                format_match = True
                break
        return (format_match)  
  
    def __verify_body() :
        if self.body == None :
            data_avaialble = False
        else :
            data_available = True
        return(data_available)

    def __verify_complex_storageMgr_format() :
        format_match = False
        for format_pattern in self._supported_complex_formats :
           if re.match(format_pattern, self._format) :
               format_match = True
               break
        return (format_match)  

    def __verify_gif() :
        format_match = False
        if re.match(self._gif_pattern, self._format) :
            format_match = True
        return (format_match)  

    def __verify_jpeg() :
        format_match = False
        x = "".join(self._format.rsplit("e", 1)) # Delete the 'e' in jpeg 
        if re.match(self._gif_pattern, self._format) :
            format_match = True
        return (format_match)  

    def __verify_image() :
        format_match = False
        for format_pattern in self._supported_image_formats :
           if re.match(format_pattern, self._format) :
               format_match = True
               break
        return (format_match)  

    def __verify_nb() :
        format_match = False
        if re.match(self._nb_pattern, self._format) :
            format_match = True
        return (format_match)  

    def __verify_path() :
        if self.path.exists() :
            data_available = True
        else :
            data_avaialble = False
        return(data_available)

    def __verify_png() :
        format_match = False
        if re.match(self._png_pattern, self._format) :
            format_match = True
    
    def __verify_storageMgr_format() :
        format_match = False
        for format_pattern in self._supported_formats :
           if re.match(format_pattern, self._format) :
               format_match = True
               break
        return (format_match)  

    def __verify_storageMgr() :
        storageMgr_match = False
        for storageMgr_pattern in self._supported_storageMgrs :
           if re.match(storageMgr_pattern, self._storageMgr) :
               storageMgr_match = True
               break
        return (storageMgr_match)  


################################################
##########  POSIXFS FORMAT CLASS ###########
################################################
##### Derived from BASE FORMAT CLASS ############
##### Methods for '_posixFS' storage-manager and #####
##### all associated formats #####
class  _posixFS_format(_stream_format) :
    def __init__(self, *args, **kwargs) :
        super(self).__init__()
        self._argv['STORAGE_MGR'] = self._storageMgr = args[0]

##### Receive Arguments #####
        self._argv['ACL'] = self.acl = kwargs.get('ACL','public-read')
        self._argv['BUCKET'] = self.bucket = kwargs.get('BUCKET','')
        self._argv['FORMAT'] = self._format = kwargs.get('(FORMAT','')
        self._argv['PATH'] = self.path = PurePath(kwargs.get('PATH', ''))
        self._argv['REGION'] = self.region = kwargs.get('REGION','us-east-2')
        self._argv['ROOT_DIR'] = self.root_dir = PurePath(kwargs.get('ROOT_DIR', ''))


##### Verify Supported Storage and Format Capabilities #####
        if not self.__verify_storageMgr_format() :
            print (self._exceptions['fatal']['format'])
            self.__del__()
        if not self.__verify_storageMgr() :
            print (self._exceptions['fatal']['storageMgr'])
            self.__del__()

##### Set Body #####
        self.__set_body()

##### Define Methods #####
    def __del__(self) :
        super().__del__() 

    def __get_body() :
        if self.body == None : # s3_object doesn't exist
            if self.__verify_path() :
                if self.__verify_image() :
                    with Image.open(self.path) as im :
                        self.body = self.image = im.load()
                        im.seek(0) 
#                        self._in_string = ''  # A base64 image received from s3, not relevant here
                elif self.__verify_xml() :
                    self.tree = self.body = ET.parse(self.path)
                elif self.__verify_text() :
                    with open(self.path,'r') as f :
                        self.text = self.body = f.read()
                else :
                    self.body = None
            else :
                self.body = None

    def __put_body() :
        if len(self.path) != 0:
            if self.__verify_image() :
                self.image.save(self.path, self._format)
            elif self.__verify_xml() :
                root = self.tree.getroot()
                root.write(self.path, pretty_print=True)
                for format_object in self.associates :
                    format_object.put()
            elif self.__verify_text_stream() :
                with open (path, 'w') as f :
                    f.write(path)
        else :
            print (self._exceptions['fatal']['nopath'])

    def __set_body(path) :
            if len(path) != 0 :
                self.path = path
            self.__get_body
            self.__update_associates()

    def __update_associates() : 
        if self.__verify_xml() :
            parent_dir = PurePath( '%s/%s'%(self.path.parents[0]))
            for im in self.tree.getroot().iter('img') :
                image_name = PurePath(im.attrib['file'])
                path = PurePath( '%s/%s'%(parent_dir, image_name))
                if path.exists() :
                    format_object = storageMgr_format('_posixFS', PATH = path)
                    self.associates.append(format_object)
            for path in self.path.parents[0].glob('*' + self._nb_pattern) :
                 _posixFS_nb_object = storageMgr_format('_posixFS', PATH = path, )
                 self.associates.append(_posixFS_nb_object)

    def get (path) :
        if  PurePath(path).exists() :
            self.path = path
            self.__set_body

    def put (*args) :
        replace = 'No'
        self.__set_body()
        if __verify_body() :
            if args :
                if len(args) >= 1  :
                    self.path = PurePath(args[0])
                if len(args) >= 2  :
                    replace = args[1]
                if len(args) >= 3  :
                    print(self._exceptions['message']['put usage'])
                    print(self._exceptions['warning']['args'])
                if self.__verify_path() :
                    if not re.match('[Yy][Ee]?[Ss]?',replace) : # Replace existing stored data? 
                        response = input('%s:%s %s'%('PATH', self.path, ' exists, replace it? [Y/N]\n'))
                        if re.match('[Yy][Ee]?[Ss]?',response) : # Request permission to replace stored data.
                            if self.__verify_supported_storageMgr_format() :
                                self.__put_body()
                    else : # Replace existing stored data (pre-approved) 
                        if self.__verify_supported_storageMgr_format() :
                            self.__put_body()
            else : # Put new file, nothing to replace
                if self.__verify_supported_storageMgr_format() :
                    self.__put_body()
        else :
            print(self._exceptions['warning']['put'])
###############################################
############# POSIX-FS GIF CLASS ###############
###############################################

##### Derived from POSIX-FS FORMAT CLASS ############
##### Interface for '_posixFS' storage-class and #####
##### associated GIF formats #####

class _posixFS_gif(_stream_format) :
    def __init__(self, **kwargs) :
        super().__init__(**kwargs)

##### Verify Format #####
        if not self.__verify_gif() :
            print ( self._exceptions['fatal']['gif'])

##### Define Methods #####
    def __del__(self) :
        super().__del__() 

###############################################
############# POSIX-FS NB CLASS ###############
###############################################

##### Derived from POSIX-FS FORMAT CLASS ############
##### Interface for '_posixFS' storage-class and #####
##### associated NB formats #####

class _posixFS_nb(_stream_format) :
    def __init__(self, **kwargs) :
        super().__init__(**kwargs)

##### Verify Format #####
        if not self.__verify_nb() :
            print ( self._exceptions['fatal']['nb'])
            self.__del__()

##### Define Methods #####
    def __del__(self) :
        super().__del__() 

###############################################
############# POSIX-FS PNG CLASS ###############
###############################################

##### Derived from POSIX-FS FORMAT CLASS ############
##### interface for '_posixFS' storage-class and #####
##### associated PNG formats #####

class _posixFS_png(_stream_format) :
    def __init__(self, **kwargs) :
        super().__init__(**kwargs)

##### Verify Format #####
        if not self.__verify_png() :
            print ( self._exceptions['fatal']['png'])
            self.__del__()

##### Define Methods #####
    def __del__(self) :
        super().__del__() 

###############################################
############# POSIX-FS RGB CLASS ###############
###############################################

##### Derived from POSIX-FS FORMAT CLASS ############
##### interface for '_posixFS' storage-class and #####
##### associated RGB formats #####

class _posixFS_rgb(_stream_format) :
    def __init__(self, **kwargs) :
        super().__init__(**kwargs)

##### Verify Format #####
        if not self.__verify_rgb() :
            print ( self._exceptions['fatal']['rgb'])
            self.__del__()

##### Define Methods #####
    def __del__(self) :
        super().__del__() 

###############################################
############# POSIX-FS TEXT CLASS ###############
###############################################

##### Derived from POSIX-FS FORMAT CLASS ############
##### interface for '_posixFS' storage-class and #####
##### associated TEXT formats #####

class _posixFS_text(_stream_format) :
    def __init__(self, **kwargs) :
        super().__init__(**kwargs)

##### Verify Format #####
        if not self.__verify_text() :
            print ( self._exceptions['fatal']['text'])
            self.__del__()

##### Define Methods #####
    def __del__(self) :
        super().__del__() 

###############################################
############# POSIX-FS TIFF CLASS ###############
###############################################

##### Derived from POSIX-FS FORMAT CLASS ############
##### interface for '_posixFS' storage-class and #####
##### associated TIFF formats #####

class _posixFS_tiff(_stream_format) :
    def __init__(self, **kwargs) :
        super().__init__(**kwargs)

##### Verify Format #####
        if not self.__verify_tiff() :
            print ( self._exceptions['fatal']['tiff'])
            self.__del__()

##### Define Methods #####
    def __del__(self) :
        super().__del__() 

###############################################
############# POSIX-FS XML CLASS ###############
###############################################

##### Derived from POSIX-FS FORMAT CLASS ############
##### interface for '_posixFS' storage-class and #####
##### associated XML formats #####

class _posixFS_xml(_stream_format) :
    def __init__(self, **kwargs) :
        super().__init__(**kwargs)

##### Verify Format #####
        if not self.__verify_xml() :
            print ( self._exceptions['fatal']['xml'])
            self.__del__()
#####  Set Content  ######
        self.__set_body()

##### Define Methods #####
    def __del__(self) :
        super().__del__() 

    def __get_body() :
        if self.__verify_path() :
            self.tree = self.body = ET.parse(self.path)

    def __set_body () :
        if self.body == None :
            self.__get_body()
        if self.body != None :
            self.__update_associates()


    def get (*args) :
        if args :
            self.path = PurePath(args[0])
        self.__get_body()
        if self.body != None :
            self.__update_associates()

    def __update_associates() : 
            parent_dir = PurePath( '%s/%s'%(self.path.parents[0]))
            for im in self.tree.getroot().iter('img') :
                image_name = PurePath(im.attrib['posixFS'])
                path = PurePath( '%s/%s'%(parent_dir, image_name))

###############################################
##### Derived from POSIX-FS-FORMAT Class ############
##### Manages 's3' storage-class and #####
##### associated file-formats #####

class _s3_format (_posixFS_format) :
    def __init__(self, *args, **kwargs) :
        super().__init__(**kwargs)
        self._storageMgr = args[0]

##### Receive Key word Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.bucket = kwargs.get('BUCKET','')
        self._format = kwargs.get('(FORMAT','')
        self.path = PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = PurePath(kwargs.get('ROOT_DIR', ''))

##### Set Parameters ######
        self.__set_key ()

##### Connect to the s3 Service ######
        self.__set_service()
        s3 = boto3.resource( self.service, region_name = self.region )
        client = boto3.client( self.service )
        self.__set_location ()

#####  Set Content  ######
        self.__set_uri()
        self.__set_body()
#        self.__get_associates

#####  Define Methods ######
    def __del__(self) :
        super().__del__() 

    def __get_associates () :                    
         parent_path = self.path.parents[0]
         parent_key = self.key.parents[0]
         parent_uri = 'https://s3-%s.amazonaws.com/%s/%s'%(self.location, self.bucket, parent_key)
         response = client.list_objects(Bucket=self.bucket, Prefix=parent_uri)
         for uri_name in response :
             object_name = uri_name.rsplit('/',1)[1].lower()
             if re.match('*' + self._nb_pattern, object_name) :
                 path = '%s/%s'%(parent_key, object_name) 
                 nb_object = storageMgr_format('s3', BUCKET = self.bucket, PATH = path)
                 self.associates.append(nb_object)
             else :
                 image_list.append(object_name)
         image_tags = self.tree.getroot().iter('img')
         for tag in image_tags :
             image_name = tag.attrib['file'].lower()
             image_stack.push(image_name)
         while image_stack :
             image_name = image_stack.pop()
             if image_name in image_list :
                 image_path = '%s/%s'%(parent_path, image_name) 
                 image_object = storageMgr_format('s3', BUCKET = self.bucket, PATH = image_path)
                 self.associates.append(image_object)
             else :
                 print ('%s::%s/%s::%s'%('Warning: s3 Object',self.bucket,image_key,'not found.'))
                 

    def __get_body() :
        try :
            s3.Object(self.bucket, self.key).load() #check to see if s3 object exists
        except botocore.exceptions.ClientError as e : 
            if e.response['Error']['Code'] == "404" : # s3 object not found
                if super().path.exists() :
                    self.__upload()
                    self.__update_body()  # _posixFS file found, upload s3_object
#                print(self._exceptions['warning']['s3_not_found'] )
            else :
                raise
        else : # s3 object found
            self._in_string = s3.Object(self.bucket, self.key).get() #s3 does'nt consume or send binary informationn
            self.__update_body() 
                    
    def __put_body () :
        if len(self.path) != 0:
            if self.__verify_image() :
                buffered = BytesIO()
                self.image.save(buffered, self.format)
                self._out_string = base64.b64encode(buffered.getvalue())
                self.__put_object() 
            elif self.__verify_xml() :
                root = self.tree.getroot()
                self._out_string = self.tree.tostring(root, pretty_print=True)
                self.__put_object() 
                for _s3_format_object in self.associates :
                    _s3_format_object.__put_body()
            elif self.__verify_text_stream() :
                self._out_string = self.body()
                self.__put_object()
        else :
            print (self._exceptions['fatal']['nopath'])

# Put an s3 object    
    def __put_object() : 
        try :
            s3.Object(self.bucket, self.key).load()
        except botocore.exceptions.ClientError as e : 
            if e.response['Error']['Code'] == "404" : # file not found
                client.put_object( 
                    Body = self._out_string,
                    Bucket = self.bucket,
                    Key = self.key)
                client.put_object_acl( Bucket = self.bucket, Key = self.key, ACL = self.acl)
            else : # Exception
                raise
        else : # file found
            response = input('/%s/%s %s'%(self.bucket, self.key, 'already exists, would you like to over-write it?\n'))
            if re.match('[Yy][Ee]?[Ss]?',response) :
                client.put_object( 
                    Body = self._out_string,
                    Bucket = self.bucket,
                    Key = self.key)
                client.put_object_acl( Bucket = self.bucket, Key = self.key, ACL = self.acl)

    def __set_body () :
        self.__get_body()
        self.__update_associates()

    def __set_key() :     
        self.key = PurePath('%s/%s/%s'%(self.acl, self.root_dir, self.path))

    def __set_location() :
        self.location = client.get_bucket_location(Bucket=bucket)['LocationConstraint']

    def __set_service() :
        self.service = 's3'

    def __set_uri () :
        self.uri = 'https://s3-%s.amazonaws.com/%s/%s'%(self.location, self.bucket, self.key)

    def __update_associates() : 
        if self.__verify_xml() :
            try :
                s3.Object(self.bucket, self.key).load() #check to see if s3 object exists
            except botocore.exceptions.ClientError as e : 
                if e.response['Error']['Code'] == "404" : # s3 object not found
                    if super().__verify_path() :
                        self.__upload()
                        self.__get_associates()
                else :
                    raise
            else : # s3 object found
                self.__get_associates()

    def __update_body() :
        if self.__verify_image() :
            im = Image.open(BytesIO(self._in_string))
            self.image = self.body = im.load()
            im.seek(0)
        elif self.__verify_xml() :
            root = ET.tree.fromstring(self._in_string)
            self.tree = self.body = ET.tostring(root)
        elif self.__verify_text() :
                self.text = self.body = self._in_string
        try :
            s3.Object(self.bucket, self.key).load() #check to see if s3 object exists
        except botocore.exceptions.ClientError as e : 
            if e.response['Error']['Code'] == "404" : # s3 object not found
                self.__put_body()
            else :
                raise

# uploads a _posixFS file to s3 storage
    def __upload(**kwargs) :  # assumes s3 objects do not exist
        super().path = PurePath(kwargs['FILE_PATH'],super().path)
        _posixFS_:qparent_dir = PurePath( '%s/%s'%(self.file_path.parents[0]))
        if super().path.exists() :
            if super().__verify_image() :
                im = Image.open(super().path, super().format)
                buffered = BytesIO()
                super().body = super().image = im.save(buffered, format=super().format)
                im.seek(0)
                self._out_string = self._in_string = base64.b64encode(buffered.getvalue())
                self.image = self.body = super().image 

                self.__put_object() 

                self._in_string = self._out_string
            elif self.__verify_xml() :
                super().tree = self.body = ET.parse(self.path)
                root = super().tree.getroot()
                for im in root.iter('img') :
                    image_name = PurePath(im.attrib['file'])
                    _posixFS_image_path = PurePath('%s/%s'%(_posixFS_parent_dir, image_name))
                    s3_image_object = storageMgr_format('s3',
                        BUCKET = self.bucket,
                        PATH = _posixFS_image_path, 
                        REGION = self.region,
                        ROOT_DIR = self.root_dir ) # if _s3_format obj == s3_tiff obj, converts to S3_png obj 
                    if  re.match(self._tiff_pattern, image_name.suffix) : # edit tiff tags in body_xml
                        im.attrib['file'] = image_name.with_suffix('png')
                        im.attrib['img-format'] = 'png'
                    parent = im.getparent() # add an s3-href to the image-tag in the body_xml
                    new_parent = ET.Element('a')
                    new_parent.extend('parent')
                    parent.append(new_parent)
                    new_parent.attrib['href'] = self.uri
                for posixFS_path in super().parents[0].glob('*' + self._nb_pattern) :
                    s3_nb_object = storageMgr_format('s3',
                        BUCKET = self.bucket,
                        PATH = posixFS_path,
                        REGION = self.region,
                        ROOT_DIR = self.root_dir)
                self._out_string = super().tree.tostring(root, pretty_print=True)
                self.__put_object() 
                self._in_string = super().tree.fromstring(root)
            elif self.__verify_text() :
                with open(super().path, 'r') as f :
                    self._out_string = super()._out_string = f.read()
                    self.__put_object() 
                    self._in_string = self._out_string

    def get (uri) :
        if len(uri) == 0 :
           uri = self.uri
        request = urllib.request.Request(uri)
        try :
            urllib.request.urlopen(request)
        except urllib.error.URLError as e :
            if request.getcode() == '404' : # s3 object not found
                print ('%s:%s: %s'%('Warning', self.uri,'not found.'))
            else :
                raise
        else : # s3 object found
            self.body = request
            self.__update_body()

    def get (*args) :
        if args :
            self.path = PurePath(args[0])
        self.__get_body()
        if self.body != None :
            self.__update_associates()

###############################################
############# s3 JPEG CLASS ###############
###############################################

##### Derived from s3 FORMAT CLASS ############
##### interface for 's3' Storage Nanager and #####
##### associated formats #####

class _s3_jpeg (_s3_format) :
    def __init__(self, **kwargs) :
        super().__init__(**kwargs)

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.body = kwargs.get('BODY',None)
        self.bucket = kwargs.get('BUCKET','')
        self._format = kwargs.get('(FORMAT','')
        self.path = PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = PurePath(kwargs.get('ROOT_DIR', ''))
        self._storageMgr = kwargs.get(STORAGE_MGR,'posixFS')

        if not self.__verify_jpeg() :
            print ( self._exceptions['fatal']['jpeg'])
            self.__del__()

##### define Methods #####
    def __del__(self) :
        super().__del__() 

###############################################
############# s3 NB CLASS ###############
###############################################

##### Derived from s3_FORMAT Class ############
##### Interface for 's3' storage-class and #####
##### associated NB file-formats #####
class _s3_nb (_s3_format) :
    def __init__(self, *args, **kwargs) :
        super().__init__(**kwargs)

##### Receive Arguments #####
        self._storageMgr = args[0]
        self.acl = kwargs.get('ACL','public-read')
        self.bucket = kwargs.get('BUCKET','')
        self._format = kwargs.get('(FORMAT','')
        self.path = PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = PurePath(kwargs.get('ROOT_DIR', ''))

        if not self.__verify_nb() :
            print ( self._exceptions['fatal']['nb'])
            self.__del__()

##### define Methods #####
    def __del__(self) :
        super().__del__() 

###############################################
############# s3 PNG CLASS ###############
###############################################

##### Derived from s3_FORMAT Class ############
##### Interface for 's3' storage-class and #####
##### associated PNG file-formats #####
class _s3_png (_s3_format) :
    def __init__(self, **kwargs) :
        super().__init__(**kwargs)

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.body = kwargs.get('BODY',None)
        self.bucket = kwargs.get('BUCKET','')
        self._format = kwargs.get('(FORMAT','')
        self.path = PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = PurePath(kwargs.get('ROOT_DIR', ''))
        self._storageMgr = kwargs.get(STORAGE_MGR,'posixFS')

        if not self.__verify_png() :
            print ( self._exceptions['fatal']['png'])
            self.__del__()

##### define Methods #####
    def __del__(self) :
        super().__del__() 

###############################################
############# s3 RGB CLASS ###############
###############################################

##### Derived from s3_FORMAT Class ############
##### Interface for 's3' storage-class and #####
##### associated RGB file-formats #####
class _s3_rgb (_s3_format) :
    def __init__(self, **kwargs) :
        super().__init__(**kwargs)

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.bucket = kwargs.get('BUCKET','')
        self._format = kwargs.get('(FORMAT','')
        self.path = PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = PurePath(kwargs.get('ROOT_DIR', ''))
        self._storageMgr = kwargs.get(STORAGE_MGR,'posixFS')

        if not self.__verify_rgb() :
            print ( self._exceptions['fatal']['gif'])
            self.__del__()

##### define Methods #####
    def __del__(self) :
        super().__del__() 


###############################################
############# s3 TEXT CLASS ###############
###############################################

##### Derived from s3_FORMAT Class ############
##### Interface for 's3' storage-class and #####
##### associated TEXT file-formats #####
class _s3_text (_s3_format) :
    def __init__(self, **kwargs) :
        super().__init__(**kwargs)
        self._storageMgr = kwargs.get(STORAGE_MGR,'posixFS')

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.body = kwargs.get('BODY',None)
        self.bucket = kwargs.get('BUCKET','')
        self._format = kwargs.get('(FORMAT','')
        self.path = PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = PurePath(kwargs.get('ROOT_DIR', ''))

        if not self.__verify_text() :
            print ( self._exceptions['fatal']['gif'])
            self.__del__()

##### define Methods #####
    def __del__(self) :
        super().__del__() 

###############################################
############# s3 TIFF CLASS ###############
###############################################

##### Derived from s3_FORMAT Class ############
##### Interface for 's3' storage-class and #####
##### associated TIFF file-formats #####
class _s3_tiff(_s3_format) :
    def __init__(self, args, **kwargs) :
        super().__init__(**kwargs)
        self._storageMgr = args[0]

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.bucket = kwargs.get('BUCKET','')
        self._format = kwargs.get('(FORMAT','')
        self.path = PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = PurePath(kwargs.get('ROOT_DIR', ''))

        if not self.__verify_tiff() :
            print ( self._exceptions['fatal']['tiff'])

##### Set Content #####
        self.__set_body()

    def __del__(self) :
        super().__del__() 

    def __set_body () :
        if self.body == None :
            self.__get_body()
        if self.body != None :
            s3_png_object = self.get_tiff2png(self)
            self._format = s3_png_object.format
            self.body = s3_png_object.body
            self.path = self.path.with_suffix('PNG')
            self.__set_key()
            self.__set_uri()

###############################################
############# s3 XML CLASS ###############
###############################################

##### Derived from s3_FORMAT Class ############
##### Interface for 's3' storage-Manager and #####
##### associated XML images #####

class _s3_xml (_s3_format) :
    def __init__(self, **kwargs) :
        super().__init__(**kwargs)

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.bucket = kwargs.get('BUCKET','')
        self._format = kwargs.get('(FORMAT','')
        self.path = PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.gem.attrib['file']('REGION','us-east-2')
        self.root_dir = PurePath(kwargs.get('ROOT_DIR', ''))
        self._storageMgr = kwargs.get(STORAGE_MGR,'posixFS')

        if not self.__verify_xml() :
            print ( self._exceptions['fatal']['xml'])

##### Set Content #####
        self.__set_body

##### define Methods #####
    def __del__(self) :
        super().__del__() 

    def __set_body () :
        self.__get_body()
        self.__update_associates()

    def __update_associates() : 
            parent_dir = PurePath( '%s/%s'%(self.path.parents[0]))
            root = self.tree.getroot()
            for im in root.iter('img') :
                image_name = PurePath(im.attrib['file'])
                path = PurePath('%s/%s'%(parent_dir, image_name))
                s3_image_object = storageMgr_format('s3',
                    BUCKET = self.bucket,
                    PATH = path, 
                    REGION = self.region,
                    ROOT_DIR = self.root_dir )
                if path.exists() :
                    if  re.match(self._tiff_pattern, image_name.suffix) :
                        im.attrib['file'] = image_name.with_suffix('png')
                        im.attrib['img-format'] = 'png'
                    parent = im.getparent()
                    new_parent = ET.Element('a')
                    new_parent.extend('parent')
                    parent.append(new_parent)
                    new_parent.attrib['href'] = self.uri
                self.body = self.tree
                self.associates.append({'s3-image':s3_format_object})
            for path in self.path.parents[0].glob('*' + self._nb_pattern) :
                s3_nb_object = storageMgr_format('s3',
                    BUCKET = self.bucket,
                    PATH = path,
                    REGION = self.region,
                    ROOT_DIR = self.root_dir)
                self.associates.append({'s3-nb':s3_nb_object})


#############   Setup the patent environment   ###############
local_home = os.getenv('HOME')
s3_acl = 'public-read'
s3_bucket = 'ocm-train-s3.std-dev-us'
s3_region = 'us-east-2'
uspto_format = 'xml'

## argpase.NameSpace()
class smifc :

    def fs( fmt, path) :
        print( '%s, %s' %(fmt, path))

    def s3( fmt, path, acl, bucket, region) :
        print( '%s, %s, %s, %s, %s' %(fmt, path, acl, bucket, region))

    def st (fmt, name, stream) :
        print ('%s, %s, %s'%(fmt, name, stream))

print(f'Arguments count: {len(sys.argv)}')
for i, arg in enumerate(sys.argv):
    print(f"Argument {i:>6}: {arg}") #note -- integer right justified 6 characters
p = argparse.ArgumentParser(conflict_handler='resolve')
p.add_argument('--fs')
xg = p.add_mutually_exclusive_group(required=True)
sp = p.add_subparsers()
ns = smifc()
cmds = [ 'fs', 's3', 'st' ] 
#for cmd in cmds :
#    cmds[cmd] = [
#        setattr('argparse.ArgumentParser','add_argument','%s_%s'%('xg',cmd),xg.add_argument(cmd)),
#        setattr('argparse','ArgumentParser','%s_%s'%('sp',cmd),sp.add_parser(sp))]
##   verify_storage_mgr_format -- needs to be a factory pattern (@classmethod)
#    if re.match(cmd, sys.argv[1].lower()) :
#        cmdline = sys.argv[1:]
#    elif re.match(cmd, sys.argv[2].lower()) :
#        cmdline = sys.argv[2:]
p.parse_args(args=sys.argv, namespace=ns)
ns.fs
exit()
#    Option Parsers
ap = argparse.ArgumentParser(conflict_handler='resolve')
ap.add_argument('-a','--acl', required=False, type=str, nargs=1, default=s3_acl,
    help='Specify s3 access control list.')
bp = argparse.ArgumentParser(conflict_handler='resolve')
bp.add_argument('-b','--bucket', required=True, type=str, nargs=1,
    help='Specify s3 bucket')
dp = argparse.ArgumentParser(conflict_handler='resolve')
dp.add_argument('-d','--data', required=False,type=None, nargs=1,
    help='Specify format, name:value pair')
fp = argparse.ArgumentParser(conflict_handler='resolve')
fg=fp.add_argument_group('group_s3_args',  description = 'format and path-format args')
fg.add_argument('-f','--fmt', metavar='format', required=False, type=str, nargs=1, 
    help='Specify data format. Supported data formats: gif, jpeg, nb, png, rgb, text, tiff, xml')
fg.add_argument('-p','--path', metavar='path.ext', required=False, type=Path, nargs=1,
    help='Specify posix file system path. If extension is missing, file-storage-format is required.')
np = argparse.ArgumentParser(conflict_handler='resolve')
np.add_argument('-n','--name', required=False,type=str, nargs=1,
    help='Specify format, name:value pair')
rp = argparse.ArgumentParser(conflict_handler='resolve')
rp.add_argument('-r','--region', required=False, type=str, nargs=1, default=s3_region,
    help='Specify s3 region')




################# command-line interface #################
#group_fs = group_smifc.add_argument_group('group_fs',
#    description = 'Supports posix file systems and data streams. Usage: \
#    smifc fs <[--fmt <data-format>] | [--path <path.ext>]>. Streaming methods use\
#    instantiated object data format specification.  Methods:\
#    get(<name:stream>,[...]), get(<path.ext>), put(<name:stream>,[...]), put(<path.ext>\
#\
#group_smifc = parser_smifc.add_mutually_exclusive_group(required=True)\
#    description = Usage: smifc fs <[--path <path.ext> ] | [--fmt <data-format>]>,\
#        smifc s3 --acl,--bucket, <[--path <path.ext>] | [--fmt <data-format>]>, --region\
#        smifc stream <--fmt data_format>, data_stream.\
#\
#        Shorthand notation uses a dash followed by the first letter\
#        of the argument identifier, e.g., -p instead --path\
#\
#        Purpose: Provide data interchage between storage managers\
#        and data conversion between data formats.\
#\
#        Description: This application enables the user to perform basic\
#        Create Read Update and Delete (CRUD) activites within the realm \
#        of each storage manager. Storage managers supported are\
#        data streams, Posix file systems, and AWS Simple Storage Service "s3".\
#        Data interchange between storage managers for simple text,\
#        graphics, and complex xml structures are supported.\
#\
#        Format conversion from tiff to png data formats is supported.\
#\
#        The Application Interfaces (API) for pathlib (text), lxml (xml), and\
#        PIL (gif, jpeg, rbg, tiff) can be used for further data manipulation.')
#group_fs.add_argument('--fs', action='store_true', help='Usage:smifc --fs')
#.group_s3 = group_smifc.add_argument_group('group_s3',
#    title = 'AWS Simple Storage Service Storage Manager',
#    description = 'Supports AWS s3, posix file systems and data streams. Usage:')
#group_st = group_smifc.add_argument_group('group_st',
#    description = 'Supports data streams.\
#    Usage: smifc stream <-f format, -n name, data-stream>,[...]\
#    Methods: get(<name:stream>,[...]), put(<name:stream>,[...]')
#subparsers = parser_smifc.add_subparsers(title='Storage Manager Subcommands')
################# [fs] Posix File System Storage Manager Subcommand #################
#parser_smifc_fs = subparsers.add_parser('fs', help='Storage Manager is a posix file system.')
#parser_smifc_fs.add_argument('-f','--fmt', nargs=1, type=str, required=False,
#    help='Specify data format. Supported data formats: gif, jpeg, nb, png, rgb, text, tiff, xml')
#    title = 'Posix File System Storage Manager',\
fs_xg = fs_sp.add_mutually_exclusive_group(required=True)
fs_xg.add_argument('-f','--fmt', metavar='format', required=False, type=str, nargs=1, 
    help='Specify data format. Supported data formats: gif, jpeg, nb, png, rgb, text, tiff, xml')
fs_xg.add_argument('-p','--path', metavar='path.ext', required=False, type=Path, nargs=1,
    help='Specify posix file system path. Extension is required for file-path.')

################# [s3] AWS Simple Storage Service subcommand #################
#group_s3 = group_smifc.add_argument_group('group_s3',
#    title = 'AWS Simple Storage Service Storage Manager',
#    description = 'Supports AWS s3, posix file systems and data streams. Usage:\
#    smifc s3 <[--path <path.ext> ] | [--fmt <data-format>]>, [<--acl> [<acl_name default:"public-read">],\
#    <--bucket bucket_name>, [<--region> <region_name default: "us-east-2">]\
#    Streaming methods use instantiated data format specification.  Methods:\
#\
#    get( <uri> )\
#\
#    get ([<ACL= acl_name default:"public-read">], <BUCKET = bucket_name>,\
#    <PATH = path.ext>, [<REGION = > <region_name default: "us-east-2">] \
#\
#    put ([<ACL= acl_name default:"public-read">], <BUCKET = bucket_name>,\
#    <PATH = path.ext>, [<REGION = > <region_name default: "us-east-2">]\
#\
#    get(<name:stream>,[...]), get(<path.ext>), put(<name:stream>,[...]), put(<path.ext>.')
#
#group_s3.add_argument('--s3', action='store_true', help='Usage:smifc --s3')
s3_xg = s3_sp.add_mutually_exclusive_group(required=True)
s3_sp.add_argument('-o','--ofmt', metavar='obj-format', required=False, type=str, nargs=1,
    help='Specify data format. Supported data formats: gif, jpeg, nb, png, rgb, text, xml.')
s3_sp.add_argument('-t','--ssspath', metavar='s3path.ext', required=False, type=Path,
        nargs=1, default = '', help='Specify s3 path.ext. Extension is required for s3 object.')
group_s3_args=group_s3.add_argument_group('group_s3_args',  description = 's3 args')
group_s3_args.add_argument('-a','--acl', required=False, type=str, nargs=1, default=s3_acl,
        help='Specify s3 access control list.')
group_s3_args.add_argument('-b','--bucket', required=True, type=str, nargs=1,
        help='Specify s3 bucket')
group_s3_args.add_argument('-r','--region', required=False, type=str, nargs=1, default=s3_region,
        help='Specify s3 region')

################# [stream] stream data subcommand #################
group_st = group_smifc.add_argument_group('group_st',
#    title = 'AWS Simple Storage Service Storage Manager',
    description = 'Supports data streams.\
    Usage: smifc stream <-f format, -n name, data-stream>,[...])\
    Methods: get(<name:stream>,[...]), put(<name:stream>,[...]')
group_st.add_argument('--st', action='store_true', help='Usage:smifc --st')
group_st_args=group_st.add_argument_group('group_st_args',  description = 'st args')
group_st_args.add_argument('-m','--mime', required=False,type=str, nargs=1, 
    help='Specify format, name:value pair')
group_st_args.add_argument('-n','--name', required=False,type=str, nargs=1,
    help='Specify format, name:value pair')
group_st_args.add_argument('-d','--data', required=False,type=None, nargs=1,
    help='Specify format, name:value pair')

#args = parser_smifc.parse_args(sys.argv[0])
#print ('cmd:%s'%(args))
 
#patent_xml = storageMgr_format(
#    self._storageMgr,
#    ACL = acl, 
#    BUCKET = bucket, 
#    FORMAT = mime,
#    REGION = region,
#    PATH = path, 
#    ROOT_DIR = root_dir)
