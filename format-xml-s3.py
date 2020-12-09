#! /usr/bin/python3

import argparse
import boto3
import botocore
import io
from libtiff import TIFF
from lxml import etree as ET 
import os
from pathlib import Path
from PIL import Image
import re
import sys

################################################
#############   INITIALIZE CLASS ###############
################################################

##### Initialize Arguments, subcommand parameters, variabes, constants and methods #####

class _initialize() :
    def __init__(self) :

##### Initialize Body Forms -- Used by Derived Classes  #####
        self.image = None
        self.text = ''
        self.tree = ''

##### Supported Storage-Format Patterns #####
        self._gif_pattern = '[.]?[Gg][Ii][Ff]'
        self._nb_pattern = '[.]?[Nn][Bb]'
        self._png_pattern = '[.]?[Pp][Nn][Gg]'
        self._rgb_pattern = '[.]?[Rr][Gg][Bb]'
        self._text_pattern = '[.]?[Tt][Ee]?[x][Tt]'
        self._tiff_pattern = '[.]?[Tt][Ii][Ff]'
        self._xml_pattern = '[.]?[x][Mm][Ll]'

        self._supported_atomic_formats = [ self._gif_pattern, self._nb_pattern, self._png_pattern, self._rgb_pattern, self._text_pattern, self._tiff_pattern ]
        self._supported_complex_formats = [ self._xml_pattern ]
        self._supported_image_formats = [ self._gif_pattern, self._png_pattern, self._rgb_pattern, self._tiff_pattern ]
        self._supported_storage_formats = supported_atomic_formats + supported_complex_formats 
        self._supported_structured_data = [ self._xml_pattern ]
        self._supported_text_streams = [ self._nb_pattern, self._text_pattern ]

#####  Supported Storage-Type Patterns #####
        self._file_storage_pattern = '[Ff][Ii][Ll][Ee]'
        self._s3_storage_pattern = '[Ss][3]'
    
        self._supported_storage_types =  [ self._file_storage_pattern, self._s3_storage_pattern ]

#####  Complex Storage-Format Associations #####
        self.__set_associates()

_Errors['Usage'] = 'Usage: "Put" method accepts 0 - 2 arguments.
            storage_format.put([[File-path], [Y/N], ...])

            Description:
            [First argument] - File-path for new or replacement file - Default:
                    Storage_format.file-path.
            [Y/N] - Replace existing file? -- Default [No].
            All other arguments are ignored.'
##### Define Methods #####
    def __del__(self) :
        print ("terminating ...")

    def __set_associates() :       
        self.associates = []
    
    def __set_storage_format() :       
        if self.__verify_path() :
            self.storage_format = str(self.path.suffix.rsplit('.',1)).lower()
        elif self.__verify_storage_format() :
            self._argv['FORMAT'] = self.storage_format = self.storage_format.lower()

    def __set_storage_type() :       
        if self.__verify_storage_type() :
                self._argv['STORAGE'] = self.storage_type = str(self.storage_type).lower()
    
    def __verify_storage_format() :
        format_match = False
        for format_pattern in self._supported_storage_formats :
           if re.match(format_pattern, self.storage_format) :
               format_match = True
               break
        return (format_match)  

    def __verify_storage_type() :
        storage_match = False
        for storage_pattern in self._supported_storage_types :
           if re.match(storage_pattern, self.storage_type) :
               storage_match = True
               break
        return (storage_match)  


################################################
#############   STORAGE-FORMAT CLASS ###########
################################################

##### The Storage Format class is the Main class and #####
##### the user interface to this class system. #####

#    def __init__(self, ({ STORAGE:storage_type, BODY:body,

#    def __init__(self, ({ STORAGE:storage_type, BODY:body,
#        PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, FORMAT:data_format })) :
class storage_format (_initialize) :
    def __init__(self, **kwargs) :
        super(storage_format,self).__init__()

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.body = kwargs.get('BODY',None)
        self.bucket = kwargs.get('BUCKET','')
        self.storage_format = kwargs.get('(FORMAT','')
        self.path = kwargs.get('PATH', Path.PurePath(''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = kwargs.get('ROOT_DIR', Path.PurePath(''))
        self.storage_type = kwargs.get('STORAGE', 'file')

##### Initialize Variables #####
        self.__set_storage_format()
        self.__set_storage_type()

##### Instanitate #####
        return (_instantiate_interface(
            ACL = self.acl, 
            BODY = self.body, 
            BUCKET = self.bucket, 
            FORMAT = self.data_format,
            REGION = self.region,
            PATH = self.path, 
            ROOT_DIR = self.root_dir,
            STORAGE = self.storage_type))

##### Define Methods #####
        def __del__(self) :
            super(storage_format, self).__del__() 

################################################
########## Instantiate Storage Class ###########
################################################

#####  Selects a Storage-Class/File-Format interface based #####
##### on incoming parametrs #####

class  _instantiate_interface(_initialize) :
    def __init__(self, **kwargs) :
        super(_instantiate_interface,self).__init__()

        self._argv['ACL'] = self.acl = kwargs.get('ACL','public-read')
        self._argv['BODY'] = self.body = kwargs.get('BODY',None)
        self._argv['BUCKET'] = self.bucket = kwargs.get('BUCKET','')
        self._argv['FORMAT'] = self.format = kwargs.get('(FORMAT','')
        self._argv['PATH'] = self.path = Path.PurePath(kwargs.get('PATH', ''))
        self._argv['REGION'] = self.region = kwargs.get('REGION','us-east-2')
        self._argv['ROOT_DIR'] = self.root_dir = Path.PurePath(kwargs.get('ROOT_DIR', ''))
        self._argv['STORAGE'] = self.storage = kwargs.get('STORAGE', 'file')

##### Initialize Variables #####
        self.__set_storage_format()
        self.__set_storage_type()

##### Verify Supported Storage and Format Capabilities #####
        if not self.__verify_storage_format() :
            print ('::%s::'%('Critical Failure: Unsupported file-format.' ))
            self.__del__()
        if not self.__verify_storage_type() :
            print ('::%s::'%('Critical Failure: Unsupported storage-class.' ))
            self.__del__()

##### Instantiate Interface #####
        self.__set_interface_name()
        if hasattr(self, self._interface_name) :
            self.__instantiate_interface()

##### Define Methods #####
    def __del__(self) :
        super(_instantiate_interface, self).__del__() 

##### Instantiate Interface Class #####
    def __instanitate_interface():
        return (getattr(self, self._interface_name)(self._argv))
            else
                return (None)
##### Set Class Name #####
    def __set_interface_name ()
        self._interface_name = ('_%s_%s'(self.storage_type, self.storage_format)))

###############################################
############# BASE FORMAT CLASS ###############
###############################################

##### Derived from Initialize class static #####
##### Manages file-format associatians #####
##### on incoming parametrs #####

class _base_format(_initialize) :
    def __init__(self, **kwargs ) :
        super(_base_format,self).__init__()

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.body = kwargs.get('BODY',None)
        self.bucket = kwargs.get('BUCKET','')
        self.storage_format = kwargs.get('(FORMAT','')
        self.path = kwargs.get('PATH', Path.PurePath(''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = kwargs.get('ROOT_DIR', Path.PurePath(''))
        self.storage_type = kwargs.get('STORAGE', 'file')

##### Define Methods #####
    def __del__(self) :
        super(_base_format, self).__del__() 
        print ('Terminating ...')

    def __verify_atomic_format() :
        format_match = False
        for format_pattern in supported_atomic_formats :
            if re.match(format_pattern, self.storage_format ) :
                format_match = True
                break
        return (format_match)  
  
    def __verify_body() :
        if self.body == None
            data_avaialble = False
        else :
            data_available = True
        return(data_available)

    def __verify_complex_format() :
        format_match = False
        for format_pattern in self._supported_complex_formats :
           if re.match(format_pattern, self.storage_format) :
               format_match = True
               break
        return (format_match)  

    def __verify_gif() :
        format_match = False
        if re.match(self._gif_pattern, self.storage_format) :
            format_match = True
        return (format_match)  

    def __verify_image() :
        format_match = False
        for format_pattern in self._supported_image_formats :
           if re.match(format_pattern, self.storage_format) :
               format_match = True
               break
        return (format_match)  

    def __verify_nb() :
        format_match = False
        if re.match(self._nb_pattern, self.storage_format) :
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
        if re.match(self._png_pattern, self.storage_format) :
            format_match = True
        return (format_match)  

    def __verify_rgb() :
        format_match = False
        if re.match(self._rgb_pattern, self.storage_format) :
            format_match = True
        return (format_match)  

    def __verify_supported_storage_format() :
        format_match = False
        for format_pattern in self._supported_formats :
           if re.match(format_pattern, self.storage_format) :
               format_match = True
               break
        return (format_match)  

    def __verify_text() :
        format_match = False
        if re.match(self._text_pattern, self.storage_format) :
            format_match = True
        return (format_match)  

    def __verify_text_stream() :
        format_match = False
        for format_pattern in self._supported_text_streams :
           if re.match(format_pattern, self.storage_format) :
               format_match = True
               break
        return (format_match)  

    def __verify_tiff() :
        format_match = False
        if re.match(self._tiff_pattern, self.storage_format) :
            format_match = True
        return (format_match)  

    def __verify_xml() :
        format_match = False
        if re.match(self._xml_pattern, self.format_name) :
            format_match = True
        return (format_match)  

    def get_stream(stream) :
        if self.__verify_image() :
            im = Image.open(io.BytesIO(stream))
            self.image = self.body = im.load()
            im.seek(0)
        elif self.__verify_xml() :
            self.tree = self.body = ET.fromstring(stream) 
        elif self.__verify_text_streams :
            self.text = self.body = stream 

    def get_tiff2png (storage_format_object) :
        if re.match (self._tiff_pattern, storage_format_object.storage_format) :
            tiff_image = get_stream(storage_format_object.body)
            rgb_image = tiff_image.convert('RGB')
            png_image = rgb_image.convert('PNG')
            storage_png_object = storage_format(STORAGE = storage_format_object.storage_type, FORMAT = 'PNG' )
        return(storage_png_object.get_stream(png_image))

###############################################
############# FILE FORMAT CLASS ###############
###############################################

##### Derived from BASE FORMAT Class ############
##### Manages 'file' storage-class and #####
##### associated file-formats #####

class _file_format(_base_format) :
    def __init__(self, **kwargs) :
        super(_file_format, self).__init__(**kwargs)

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.body = kwargs.get('BODY',None)
        self.bucket = kwargs.get('BUCKET','')
        self.storage_format = kwargs.get('(FORMAT','')
        self.path = Path.PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = Path.PurePath(kwargs.get('ROOT_DIR', ''))
        self.storage_type = kwargs.get(STORAGE,'file')

#####  Set Content  ######
        self.__set_body()

##### Define Methods #####
    def __del__(self) :
        super(_file_format, self).__del__() 

    def __get_body () :
        if self.__verify_path() :
            if self.__verify_image() :
                with open(self.path, 'rb') as f :
                    im = Image.open(io.BytesIO(f.read()))
                    self.image = self.body = im.load()
            elif self.__verify_xml() :
                self.tree = self.body = ET.parse(self.path) 
            elif self.__verify_text_stream() :
                with open (self.path, 'r') as f :
                    self.text = self.body = f.read()

    def __set_body () :
        if self.body == None :
            self.__get_body
            self.__update_associates

    def get (path) :
        if  Path.PurePath(path).exists() :
            self.path = path
            self.__set_body

    def put (*args) :
        replace = 'No'
        self.__set_body()
        if __verify_body() :
            if args :
                if len(args) >= 1  :
                    self.path = Path.PurePath(args[0])
                if len(args) >= 2  :
                    replace = args[1]
                if len(args) >= 3  :
                    print('Warning extra arguments were ignored!')
                    print (self._errors['Usage'])
                if self.__verify_path() :
                    if not re.match('[Yy][Ee]?[Ss]?',replace) : # Replace existing stored data? 
                        response = input('%s:%s %s'%('PATH', self.path, ' exists, replace it? [Y/N]\n'))
                        if re.match('[Yy][Ee]?[Ss]?',response) : # Request permission to replace stored data.
                            root = self.tree.getroot()
                            root.write(self.path, pretty_print=True)
                            for key, s3_format_object in self.associates.items() :
                                s3_format_object.put()
                    else : # Replace existing stored data (pre-approved) 
                        root = self.tree.getroot()
                        root.write(self.path, pretty_print=True)
                        for key, s3_format_object in self.associates.items() :
                            s3_format_object.put()
            else : # Put new file, nothing to replace
                root = self.tree.getroot()
                root.write(self.path, pretty_print=True)
        else :
            print ('Warning: Nothing to put()!')

    def __update_associates() : 
            parent_dir = Path.PurePath( '%s/%s'%(self.path.parents[0]))
            for im in self.tree.getroot().iter('img') :
                image_name = Path.PurePath(im.attrib['file'])
                path = Path.PurePath( '%s/%s'%(parent_dir, image_name))
                file_format_object = storage_format(STORAGE = 'file', PATH = self.path)
                self.associates.append({'file-image':file_format_object})
            for path in self.path.parents[0].glob('*' + self._nb_pattern) :
                 file_nb_object = storage_format(STORAGE = 'file', PATH = path, )
                 self.associates.append({ 'file-nb':file_nb_object})
            print (self._errors['Usage'])

###############################################
############# FILE GIF CLASS ###############
###############################################

##### Derived from FILE FORMAT CLASS ############
##### Interface for 'file' storage-class and #####
##### associated GIF formats #####

class _file_gif(file_format) :
    def __init__(self, **kwargs) :
        super(_file_gif, self).__init__(**kwargs)

##### Verify Format #####
        if not self.__verify_gif() :
            print ('%s::%s::'%('Critical Failure: not a valid <.gif> file extension',self.ext))
            self.__del__()

##### Define Methods #####
    def __del__(self) :
        super(_file_gif, self).__del__() 

###############################################
############# FILE NB CLASS ###############
###############################################

##### Derived from FILE FORMAT CLASS ############
##### Interface for 'file' storage-class and #####
##### associated NB formats #####

class _file_nb(file_format) :
    def __init__(self, **kwargs) :
        super(_file_nb, self).__init__(**kwargs)

##### Verify Format #####
        if not self.__verify_nb() :
            print ('%s::%s::'%('Critical Failure: not a valid <.nb> file extension',self.ext))
            self.__del__()

##### Define Methods #####
    def __del__(self) :
        super(_file_nb, self).__del__() 

###############################################
############# FILE PNG CLASS ###############
###############################################

##### Derived from FILE FORMAT CLASS ############
##### interface for 'file' storage-class and #####
##### associated PNG formats #####

class _file_png(file_format) :
    def __init__(self, **kwargs) :
        super(_file_png, self).__init__(**kwargs)

##### Verify Format #####
        if not self.__verify_png() :
            print ('%s::%s::'%('Critical Failure: not a valid <.png> file extension',self.ext))
            self.__del__()

##### Define Methods #####
    def __del__(self) :
        super(_file_png, self).__del__() 

###############################################
############# FILE RGB CLASS ###############
###############################################

##### Derived from FILE FORMAT CLASS ############
##### interface for 'file' storage-class and #####
##### associated RGB formats #####

class _file_rgb(file_format) :
    def __init__(self, **kwargs) :
        super(_file_rgb, self).__init__(**kwargs)

##### Verify Format #####
        if not self.__verify_rgb() :
            print ('%s::%s::'%('Critical Failure: not a valid <.rgb> file extension',self.ext))
            self.__del__()

##### Define Methods #####
    def __del__(self) :
        super(_file_rgb, self).__del__() 

###############################################
############# FILE TIFF CLASS ###############
###############################################

##### Derived from FILE FORMAT CLASS ############
##### interface for 'file' storage-class and #####
##### associated TIFF formats #####

class _file_tiff(file_format) :
    def __init__(self, **kwargs) :
        super(_file_tiff, self).__init__(**kwargs)

##### Verify Format #####
        if not self.__verify_tiff() :
            print ('%s::%s::'%('Critical Failure: not a valid <.tiff> file extension',self.ext))
            self.__del__()

##### Define Methods #####
    def __del__(self) :
        super(_file_tiff, self).__del__() 

###############################################
############# FILE XML CLASS ###############
###############################################

##### Derived from FILE FORMAT CLASS ############
##### interface for 'file' storage-class and #####
##### associated XML formats #####

class _file_xml(file_format) :
    def __init__(self, **kwargs) :
        super(_file_xml, self).__init__(**kwargs)

##### Verify Format #####
        if not self.__verify_xml() :
            print ('%s::%s::'%('Critical Failure: not a valid <.xml> file extension',self.ext))
            self.__del__()

#####  Set Content  ######
        self.__set_body()

##### Define Methods #####
    def __del__(self) :
        super(_file_xml, self).__del__() 

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
            self.path = Path.PurePath(args[0])
        self.__get_body()
        if self.body != None :
            self.__update_associates()

    def put (*args) :
        replace = 'No'
        _replace = False
        if args :
            if len(args) >= 1  :
                self.path = Path.PurePath(args[0])
            if len(args) == 2  :
                replace = args[1]
            else :
                print ('Usage: "Put" method accepts 0 - 2 arguments.')
                print ('<[No arguments] [File-path], [Y/N] ...>')
                print ('[First argument] - File-path for new or replacement file: Default:
                        Storage_format object file-path used.')
                print ('[Y/N] - Replace existing file is OK or not: Default is not OK.')
                print ('All other arguments are ignored.')
        if self.__verify_path() :
            if _replace == False :
                response = input('%s:%s %s'%('PATH', self.path, ' exists, replace it? [Y/N]\n'))
                if re.match('[Yy][Ee]?[Ss]',response) : # user requests replacement
                    root = self.tree.getroot()
                    root.write(self.path, pretty_print=True)
                    for key, s3_format_object in self.associates.items() :
                        s3_format_object.put()
            else : # replacement request was pre-approved
                root = self.tree.getroot()
                root.write(self.path, pretty_print=True)
                for key, s3_format_object in self.associates.items() :
                    s3_format_object.put()
        else : # path doesn't exist -- write file to new path
            root = self.tree.getroot()
            root.write(self.path, pretty_print=True)
            for key, s3_format_object in self.associates.items() :
                s3_format_object.put()

###############################################
############# s3 FORMAT CLASS ###############
###############################################

##### Derived from FILE-FORMAT Class ############
##### Manages 's3' storage-class and #####
##### associated file-formats #####

class _s3_format (_file_format) :
    def __init__(self, **kwargs) :
        super(_s3_format, self).__init__(**kwargs)

##### Set Parameters ######
        self.__set_key ()

##### Connect to the s3 Service ######
        self.__set_service()
        s3 = boto3.resource( self.service, region_name = region )
        client = boto3.client( s3_SERVICE )
        self.__set_location ()

#####  Set Content  ######
        self.__set_uri()
        self.__set_body()

#####  Define Methods ######
    def __del__(self) :
        super(_s3_format, self).__del__() 

    def __get_body () :
        if self.body == None : # s3_object doesn't exist
            try :
                s3.Object(self.bucket, self.key).load() #check to see if s3 object exists
            except botocore.exceptions.ClientError as e : 
                if e.response['Error']['Code'] == "404" : # s3 object not found
                     print ( 's3 Object Not found' )
                else :
                    raise
            else : # s3 object found
                self.body = s3.Object(self.bucket, self.key).get()

    def __set_body () :
        if self.body == None :
            self.__get_body()

    def __set_key () :     
        self.key = Path.PureFilePath('%s/%s/%s'%(self.acl, self.root_dir, self.path))

    def __set_location() :
        self.location = client.get_bucket_location(Bucket=bucket)['LocationConstraint']

    def __set_service() :
        self.service = 's3'

    def __set_uri () :
        self.uri = 'https://s3-%s.amazonaws.com/%s/%s'%(self.location, self.bucket, self.key)

    def get (uri) :
        if uri.empty :
           uri = self.uri
        request = urllib.request.Request(uri)
        try :
            urllib.request.urlopen(request)
        except urllib.error.URLError as e :
            if request.getcode() == '404' : # s3 object not found
                print ('%s:: %s ::'%('Not found error', self.uri))
            else :
                raise
        else : # uri found
            self.body = urllib.urlopen(self.uri).read()

    def put (*args) :
        replace = 'No'
        _replace = False
        if args :
            if len(args) >= 1  :
                self.path = Path.PurePath(args.get)
            if len(args) == 2  :
                replace = args[1]
            else :
                print ('Usage: "Put" method accepts 0 - 2 arguments.')
                print ('<[No arguments] [uri], [Y/N] ...>')
                print ('[First argument] - Uri for new or replacement file: Default:
                        Storage_format object uri used.')
                print ('[Y/N] - Replace existing file is OK or not: Default is not OK.')
                print ('All other arguments are ignored.')
        if self.__verify_path() :
            if _replace == False :
                response = input('%s:%s %s'%('PATH', self.path, ' exists, replace it? [Y/N]\n'))
                if re.match('[Yy][Ee]?[Ss]',response) : # user requests replacement
                    root = self.tree.getroot()
                    root.write(self.path, pretty_print=True)
                    for key, s3_format_object in self.associates.items() :
                        s3_format_object.put(*args, **kwargs)
            else : # replacement request was pre-approved
                root = self.tree.getroot()
                root.write(self.path, pretty_print=True)
                for key, s3_format_object in self.associates.items() :
                    s3_format_object.put("",replace)
        else : # path doesn't exist -- write file to new path
            root = self.tree.getroot()
            root.write(self.path, pretty_print=True)
            for key, s3_format_object in self.associates.items() :
                s3_format_object.put()
        if self.body != None : # body exists
            try :
                s3.Object(self.bucket, self.key).load() #check to see if s3 object exists
            except botocore.exceptions.ClientError as e : 
                if e.response['Error']['Code'] == '404' : # s3 object not found
                    client.put_object( self.body, Bucket = self.bucket, Key = self.key)
                    clienobject_acl( Bucket = self.bucket, Key = self.key, ACL = self.acl)
                else :
                    raise
                re = input('/%s/%s %s'%(OBJECT_BUCK, OBJECT_KEY, 'already exists, would you like to over-write it? [Y/N]\n'))
                if re.match('[Yy][Ee]?[Ss]?',re) : # replace eixsiting file upon request
                    if self.__verify_path() :
                        client.put_object( self.body, Bucket = self.bucket, Key = self.key)
                        client.put_object_acl( Bucket = self.bucket, Key = self.key, ACL = self.acl)
                        for key, s3_format_object in self.associates.items() :
                            s3_format_object.put()
        else : # new file - no existing file
            client.put_object( self.body, Bucket = self.bucket, Key = self.key)
            client.put_object_acl( Bucket = self.bucket, Key = self.key, ACL = self.acl)

###############################################
############# s3 GIF CLASS ###############
###############################################

##### Derived from s3_FORMAT Class ############
##### Interface for 's3' storage-class and #####
##### associated GIF file-formats #####
class _s3_gif (_s3_format) :
    def __init__(self, **kwargs) :
        super(_s3_gif, self).__init__(**kwargs)

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.body = kwargs.get('BODY',None)
        self.bucket = kwargs.get('BUCKET','')
        self.storage_format = kwargs.get('(FORMAT','')
        self.path = Path.PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = Path.PurePath(kwargs.get('ROOT_DIR', ''))
        self.storage_type = kwargs.get(STORAGE,'file')

        if not self.__verify_gif() :
            print ('%s::%s::'%('Critical Failure: not a valid <.gif> file extension',self.ext))
            self.__del__()

##### define Methods #####
    def __del__(self) :
        super(_s3_gif, self).__del__() 

###############################################
############# s3 JPG CLASS ###############
###############################################

##### Derived from s3_FORMAT Class ############
##### Interface for 's3' storage-class and #####
##### associated JPG file-formats #####
class _s3_jpg (_s3_format) :
    def __init__(self, **kwargs) :
        super(_s3_jpg, self).__init__(**kwargs)

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.body = kwargs.get('BODY',None)
        self.bucket = kwargs.get('BUCKET','')
        self.storage_format = kwargs.get('(FORMAT','')
        self.path = Path.PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = Path.PurePath(kwargs.get('ROOT_DIR', ''))
        self.storage_type = kwargs.get(STORAGE,'file')

        if not self.__verify_jpg() :
            print ('%s::%s::'%('Critical Failure: not a valid <.jpg> file extension',self.ext))
            self.__del__()

##### define Methods #####
    def __del__(self) :
        super(_s3_jpg, self).__del__() 

###############################################
############# s3 NB CLASS ###############
###############################################

##### Derived from s3_FORMAT Class ############
##### Interface for 's3' storage-class and #####
##### associated NB file-formats #####
class _s3_nb (_s3_format) :
    def __init__(self, **kwargs) :
        super(_s3_nb, self).__init__(**kwargs)

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.body = kwargs.get('BODY',None)
        self.bucket = kwargs.get('BUCKET','')
        self.storage_format = kwargs.get('(FORMAT','')
        self.path = Path.PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = Path.PurePath(kwargs.get('ROOT_DIR', ''))
        self.storage_type = kwargs.get(STORAGE,'file')

        if not self.__verify_nb() :
            print ('%s::%s::'%('Critical Failure: not a valid <.nb> file extension',self.ext))
            self.__del__()

##### define Methods #####
    def __del__(self) :
        super(_s3_nb, self).__del__() 

###############################################
############# s3 PNG CLASS ###############
###############################################

##### Derived from s3_FORMAT Class ############
##### Interface for 's3' storage-class and #####
##### associated PNG file-formats #####
class _s3_png (_s3_format) :
    def __init__(self, **kwargs) :
        super(_s3_png, self).__init__(**kwargs)

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.body = kwargs.get('BODY',None)
        self.bucket = kwargs.get('BUCKET','')
        self.storage_format = kwargs.get('(FORMAT','')
        self.path = Path.PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = Path.PurePath(kwargs.get('ROOT_DIR', ''))
        self.storage_type = kwargs.get(STORAGE,'file')

        if not self.__verify_png() :
            print ('%s::%s::'%('Critical Failure: not a valid <.png> file extension',self.ext))
            self.__del__()

##### define Methods #####
    def __del__(self) :
        super(_s3_png, self).__del__() 

###############################################
############# s3 RGB CLASS ###############
###############################################

##### Derived from s3_FORMAT Class ############
##### Interface for 's3' storage-class and #####
##### associated RGB file-formats #####
class _s3_rgb (_s3_format) :
    def __init__(self, **kwargs) :
        super(_s3_rgb, self).__init__(**kwargs)

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.body = kwargs.get('BODY',None)
        self.bucket = kwargs.get('BUCKET','')
        self.storage_format = kwargs.get('(FORMAT','')
        self.path = Path.PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = Path.PurePath(kwargs.get('ROOT_DIR', ''))
        self.storage_type = kwargs.get(STORAGE,'file')

        if not self.__verify_rgb() :
            print ('%s::%s::'%('Critical Failure: not a valid <.rgb> file extension',self.ext))
            self.__del__()

##### define Methods #####
    def __del__(self) :
        super(_s3_rgb, self).__del__() 


###############################################
############# s3 TEXT CLASS ###############
###############################################

##### Derived from s3_FORMAT Class ############
##### Interface for 's3' storage-class and #####
##### associated TEXT file-formats #####
class _s3_text (_s3_format) :
    def __init__(self, **kwargs) :
        super(_s3_text, self).__init__(**kwargs)

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.body = kwargs.get('BODY',None)
        self.bucket = kwargs.get('BUCKET','')
        self.storage_format = kwargs.get('(FORMAT','')
        self.path = Path.PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = Path.PurePath(kwargs.get('ROOT_DIR', ''))
        self.storage_type = kwargs.get(STORAGE,'file')

        if not self.__verify_text() :
            print ('%s::%s::'%('Critical Failure: not a valid <.t[e]xt> file extension',self.ext))
            self.__del__()

##### define Methods #####
    def __del__(self) :
        super(_s3_text, self).__del__() 

###############################################
############# s3 TIFF CLASS ###############
###############################################

##### Derived from s3_FORMAT Class ############
##### Interface for 's3' storage-class and #####
##### associated TIFF file-formats #####
class _s3_tiff(_s3_format) :
    def __init__(self, **kwargs) :
        super(_s3_tiff, self).__init__(**kwargs)

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.body = kwargs.get('BODY',None)
        self.bucket = kwargs.get('BUCKET','')
        self.storage_format = kwargs.get('(FORMAT','')
        self.path = Path.PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = Path.PurePath(kwargs.get('ROOT_DIR', ''))
        self.storage_type = kwargs.get(STORAGE,'file')

        if not self.__verify_tiff() :
            print ('%s::%s::'%('Critical Failure: not a valid <.tif> file extension',self.ext))

##### Set Content #####
        self.__set_body()

    def __del__(self) :
        super(_s3_tiff, self).__del__() 

    def __set_body () :
        if self.body == None :
            self.__get_body()
        if self.body != None :
            s3_png_object = self.get_tiff2png(self)
            self.storage_format = s3_png_object.file_format
            self.body = s3_png_object.body
            self.path = self.path.with_suffix('PNG')
            self.__set_key()
            self.__set_uri()

###############################################
############# s3 XML CLASS ###############
###############################################

##### Derived from FILE-FORMAT Class ############
##### Interface for 's3' storage-class and #####
##### associated XML file-formats #####

class _s3_xml (_s3_format) :
    def __init__(self, **kwargs) :
        super(_s3_xml, self).__init__(**kwargs)

##### Receive Arguments #####
        self.acl = kwargs.get('ACL','public-read')
        self.body = kwargs.get('BODY',None)
        self.bucket = kwargs.get('BUCKET','')
        self.storage_format = kwargs.get('(FORMAT','')
        self.path = Path.PurePath(kwargs.get('PATH', ''))
        self.region = kwargs.get('REGION','us-east-2')
        self.root_dir = Path.PurePath(kwargs.get('ROOT_DIR', ''))
        self.storage_type = kwargs.get(STORAGE,'file')

        if not self.__verify_xml() :
            print ('%s::%s::'%('Critical Failure: not a valid <.xml> file extension',self.ext))

##### Set Content #####
        self.__set_body

##### define Methods #####
    def __del__(self) :
        super(_s3_xml, self).__del__() 

    def __set_body () :
        if self.body == None :
            self.__get_body()
        if self.body != None :
            self.tree = self.get_stream(self.body)
            self.__update_associates()

    def __update_associates() : 
            parent_dir = Path.PurePath( '%s/%s'%(self.path.parents[0]))
            root = self.tree.getroot()
            for im in root.iter('img') :
                image_name = Path.PurePath(im.attrib['file'])
                path = Path.PurePath('%s/%s'%(parent_dir, image_name))
                s3_image_object = storage_format(STORAGE = 's3',
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
                s3_nb_object = storage_format(STORAGE = 's3',
                    BUCKET = self.bucket,
                    PATH = path,
                    REGION = self.region,
                    ROOT_DIR = self.root_dir)
                self.associates.append({'s3-nb':s3_nb_object})


#############   Setup the patent environment   ###############
local_home = os.getenv('HOME')
s3_patent_acl = 'public-read'
s3_patent_bucket = 'ocm-train-s3.std-dev-us'
s3_patent_root_dir = 'uspto/patent'
s3_patent_region = 'us-east-2'
uspto_file_format = 'xml'

################# command-line interface #################
parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(help='commands')

################# [file] storage class subcommand line #################
file_parser = subparsers.add_parser(storage_type='file',help='specify storage_type as "file":')
file_parser.add_argument('-f','--file_format', required=True, type=file, nargs=1, default = uspto_format,
    help='Specify file-path with extension else use file-format when file does not exist. Supported file-formats: gif, nb, png, rgb, text, tiff, xml')

################# [s3] storage class subcommand line #################
s3_parser = subparsers.add_parser(storage_type='s3',help='specify storage_type as "s3":')
s3_parser.add_argument('-a','--acl', required=True, type=str, nargs=1, default = s3_patent_acl,
    help='Specify s3 access control list')
s3_parser.add_argument('-b','--bucket', required=True, type=str, nargs=1, default = s3_patent_bucket,
    help='Specify s3 bucket')
s3_parser.add_argument('-d','--directory', required=True, type=str, nargs=1, default = s3_patent_root_dir,
    help='Specify s3 root directory')
s3_parser.add_argument('-f','--file_format', required=True, type=file, nargs=1, default = uspto_file_format,
    help='Specify file-path with extension else use file-format when file does not exist. Supported file-formats: gif, nb, png, rgb, text, tiff, xml')
s3_parser.add_argument('-r','--region', required=True, type=str, nargs=1, default = s3_patent_region,
    help='Specify s3 region')

if Path.PurePath(file_format).exists :
    data_format = Path.PurePath(file_format).suffix
    path = file_format
else :
    data_format = file_format
    path = ''

patent_xml = format_data (
    ACL = acl, 
    BUCKET = bucket, 
    FORMAT = data_format,
    REGION = region,
    PATH = path, 
    ROOT_DIR = root_dir,
    STORAGE = storage_type)
