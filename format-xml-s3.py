#! /usr/bin/python3

import sys
import os
import io
import argparse
import re
from pathlib import Path
from libtiff import TIFF
from PIL import Image
import xml.etree.ElementTree as ET
import boto3
import botocore

#############   Define Classes  ###############

##### Data Format Class #####
# User Interface
# Upload patents to 's3' storage
object_bucket = 'ocm-train-s3.std-dev-us'
object_acl = 'public-read'
object_format = 'xml'
object_name = '%s.%s'%('patents',OBJECT_FORMAT)
# object_key = '%s/%s/%s'%(OBJECT_ACL,OBJECT_DIR,OBJECT_NAME)
object_region = 'us-east-2'
HOME = os.getenv('HOME')
~/data/I20201117
patent_dir_root  = PurePath('/%s/%s'%(HOME,'data/I20201117'))
s3_dir_root = 'uspto/patent'
            for patent_dir in PATENT_DIR_ROOT.glob('*') :
                patent_dir_path = '%s/%s'%(PATENT_DIR_ROOT,patent_dir)
                patent_dir_path.glob('*[.][Xx][Mm][Ll]') :
                xml_path = PurePath('/%s/%s'%(HOME,'data/I20201117'))
                xml_s3_data = data_format(STORAGE = 's3',
                    PATH = xml_path,
                    BUCKET = bucket,
                    REGION = object_region,
                    ROOT_DIR = s3_dir_root,
                    ACL = object_acl,
                    DATA_TYPE='xml')

class data_format :
    __init__(self, { STORAGE: storage_class,
         PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
    super().__init__(self, storage_class,
        { PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
#def __init(self, **args):
#    self.path = PurePath(args.get('PATH'))
#    self.root_dir = args.get('ROOT_DIR', self.path.root())
        self.acl = args[1].set_default(ACL: 'public-read')
        self.bucket = args[1].set_default(BUCKET: '')
        self.region = args[1].set_default(REGION: 'us-east-2')
        self.path = args[1].set_default(PATH: PurePath(''))
        self.root_dir = args[1].set_default(ROOT_DIR: PurePath(''))
        self.storage_class = args[1].set_default(STORAGE_CLASS: 'file')
        self.__set_format()
        self.arg_list = {
            'file':(self.path, self.format),
            'S3':(self.path, self.format, self.bucket, self.region ) }
        if not self.__verify_arg_list() :
            self.__del__()
        _launch(
            ACL = acl, 
            BUCKET = bucket, 
            DATA_TYPE = data_type,
            REGION = region,
            PATH = path, 
            ROOT_DIR = root_dir,
            STORAGE = storage_class)
        def __del__(self):
        def __verify_arg_list () :
            if arg_list.get('file')[0].empty and arg_list.get('file')[1].empty :
                'Critical Error: Missing data-format: path or data-type.'
                valid_args = False
            else if :
                if arg_list.get('S3')[0].empty and arg_list.get('S3')[1].empty :
                    'Critical Error: Missing data-format: path or data-type.'
                    valid_args = False
                else if arg_list.get('S3')[2].empty:
                    'Critical Error: Missing bucket'
                    valid_args = False
                else if arg_list.get('S3')[3].empty :
                    'Critical Error: Missing region: region.'
                    valid_args = False
                else :
                    None
            else :
                valid_args = True
            return(valid_args)         
        def __set_format () :
            if not self.path.empty() :
                self.format = self.path.suffix.split('.',1)
            else
                self.format = args[1].set_default(DATA_TYPE: '')

##### Instantiate  data format#####

class  _launch() :
        __init__(self, { STORAGE: storage_class,
            PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type }) :
        self.acl = args[1].set_default(ACL: 'public-read')
        self.bucket = args[1].set_default(BUCKET: '')
        self.region = args[1].set_default(REGION: 'us-east-2')
        self.path = args[1].set_default(PATH: PurePath(''))
        self.root_dir = args[1].set_default(ROOT_DIR: '')
        self.storage_class = args[1].set_default(STORAGE: 'file')
        self.set_format()

        self.gif = '[.]?[Gg][Ii][Ff]'
        self.nb = '[.]?[Nn][Bb]'
        self.png = '[.]?[Pp][Nn][Gg]'
        self.rgb = '[.]?[Rr][Gg][Bb]'
        self.text = '[.]?[Tt][Ee]?[x][Tt]'
        self.tiff = '[.]?[Tt][Ii][Ff]'
        self.xml = '[.]?[x][Mm][Ll]'

        self.supported_atomic_formats = [ self.gif, self.nb, self.png, self.rgb, self.text, self.tiff ]
        self.supported_complex_formats = [ self.xml ]
        self.supported_formats = supported_atomic_formats + supported_complex_formats
        self.supported_image_formats = { self.gif, self.png, self.rgb, self.tiff }
        self.supported_text_streams = { self.nb, self.text }

        self.supported_format_stores = {
            '_%s_%s'(self.gif,'file'): _gif_file(args[1]),
            '_%s_%s'(self.gif,'S3'): _gif_s3(args[1]),
            '_%s_%s'(self.nb,'file'): _nb_file(args[1]),
            '_%s_%s'(self.nb,'S3'): _nb_s3(args[1]),
            '_%s_%s'(self.png,'file'): _png_file(args[1]),
            '_%s_%s'(self.png,'S3'): _png_s3(args[1]),
            '_%s_%s'(self.rgb,'file'): _rgb_file(args[1]),
            '_%s_%s'(self.rgb,'S3'): _rgb_s3(args[1]),
            '_%s_%s'(self.text,'file'): _text_file(args[1]),
            '_%s_%s'(self.text,'S3'): _text_s3(args[1]),
            '_%s_%s'(self.tiff,'file'): _tiff_file(args[1]),
            '_%s_%s'(self.tiff,'S3'): _tiff_s3(args[1]),
            '_%s_%s'(self.xml,'file'): _xml_file(args[1]),
            '_%s_%s'(self.xml,'S3'): _xml_s3(args[1]) }

        if not self.__verify_format() :
            print ('%s::%s::'%('Critical Failure: Unsupported format.' 
            self.__del__()
        return (eval(supported_format_stores.get('_%s_%s'(self.format, self.storage_class)))

    def __del__(self) :
    def __set_format () :
        if not self.path.empty() :
            self.format = self.path.suffix.split('.',1)
        else
            self.format = args[1].set_default(DATA_TYPE: '')
    def __set_format() :       
        if not self.path.empty() :
            self.format = self.path.suffix()
    def __verify_format()
        _match = False
        for _name in self.supported_formats :
           if re.match(_name, self.format) :
               _match = True
        return (_match)  
        self.path = PurePosixPath(path)

class _base_form() :
    __init__(self, { STORAGE:storage_class,
        PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })

        self.acl = args[1].set_default(ACL: 'public-read')
        self.bucket = args[1].set_default(BUCKET: '')
        self.path = args[1].set_default(PATH: PurePath(''))
        self.region = args[1].set_default(REGION: 'us-east-2')
        self.root_dir = args[1].set_default(ROOT_DIR: '')
        self.storage_class = args[1].set_default(STORAGE: 'file')
        self.__set_format()

        self.gif = '[.]?[Gg][Ii][Ff]'
        self.nb = '[.]?[Nn][Bb]'
        self.png = '[.]?[Pp][Nn][Gg]'
        self.rgb = '[.]?[Rr][Gg][Bb]'
        self.text = '[.]?[Tt][Ee]?[x][Tt]'
        self.tiff = '[.]?[Tt][Ii][Ff]'
        self.xml = '[.]?[x][Mm][Ll]'

        self.supported_atomic_formats = [ self.gif, self.nb, self.png, self.rgb, self.text, self.tiff ]
        self.supported_complex_formats = [ self.xml ]
        self.supported_formats = supported_atomic_formats + supported_complex_formats
        self.supported_image_formats = { self.gif, self.png, self.rgb, self.tiff }
        self.supported_text_streams = { self.nb, self.text }

    def __del__(self):
        print ('Terminating ...')
    def __set_format () :
        if not self.path.empty() :
            self.format = self.path.suffix.rsplit('.',1)
        else
            self.format = args[1].set_default(DATA_TYPE: '')
    def __verify_atomic_format() :
        format_match = False
       for format_name in supported_atomic_formats :
           if re.match(format_name, self.format ) :
               format_match = True
               break
        return (format_match)  
    def __verify_complex_format() :
        format_match = False
        for format_name in self.supported_complex_formats
           if re.match(format_name, self.format) :
               format_match = True
               break
        return (format_match)  
    def __verify_format()
        format_match = False
        for format_name in self.supported_formats :
           if re.match(format_name, self.format) :
               format_match = True
               break
        return (format_match)  
    def __verify_gif() :
        format_match = False
        if re.match(self.gif, self.format) :
            format_match = True
        return (format_match)  
    def __verify_image() :
        format_match = False
        for format_name in self.supported_image_formats :
           if re.match(format_name, self.format) :
               format_match = True
               break
        return (format_match)  
    def __verify_nb() :
        format_match = False
        if re.match(self.nb, self.format_name):
            format_match = True
        return (format_match)  
    def __verify_path() :
        data_available = False
        if self.__verify_format() :
            if self.path.exists :
                data_available = True
        return(data_available)
    def __verify_png() :
        format_match = False
        if re.match(self.png, self.format_name):
            format_match = True
        return (format_match)  
    def __verify_rgb() :
        format_match = False
        if re.match(self.rgb, self.format_name):
            format_match = True
        return (format_match)  
    def __verify_supported_format() :
        format_match = False
        for supported_format in self.supported_formats :
           if re.match(supported_format, format_name):
               format_match = True
               break
        return (format_match)  
    def __verify_text() :
        format_match = False
        if re.match(self.text, self.format) :
            format_match = True
        return (format_match)  
    def __verify_text_stream()
        format_match = False
        for format_name in self.supported_text_streams :
           if re.match(format_name, self.format) :
               format_match = True
               break
        return (format_match)  
    def __verify_tiff() :
        format_match = False
        if re.match(self.tiff,self.format) :
            format_match = True
        return (format_match)  
    def __verify_xml(format_name) :
        format_match = False
           if re.match(self.xml, format_name):
               format_match = True
        return (format_match)  

#### Base Format Class ####
# A Atomic Format Class describes data formats which have no references to any other 
# data formats. 

# The Data Store Class is derived from the File Format Base Class
# It is responsible for format conversions, and I/O operations on supported data sets
class _format_file(base_form) :
    __init__(self, { STORAGE: storage_class,
        PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        super().__init__(self, 
                { STORAGE = storage_class,
             PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        self.__set_body()
    def __del__(self) :
        super(format_file, self).__del__() 
    def __get_associated_files() :   
        for format in supported_complex_formats :
            if self.__verify_xml() :
                 self.__get_xml_files
    def __get_tiff2png (format_data) :
        if re.match (self.tiff, format_data.format) :
            if format_data.body == None :
                if format_data.path.exists :
                    with open(self.path, 'rb') as f :
                        tiff_image = format_data.body = Image.open(io.BytesIO(f.read()))
            if ( format_data.body != None ) :
                rgb_image = tiff_image.convert('RGB')
                png_image = rgb_image.convert('PNG')
                png_data = _launch(DATA_TYPE = 'PNG')
        return(png_data.get(png_image))
    def __get_xml_associates() : 
        if self.__verify_xml() :
            for im in self.tree.getroot().iter('img') :
                im.path = PurePath('%s/%s'%(self.path.parents[0],im.get('file'))))
                if im.path.exists()
                    self.__set_xml_associate(PurePath('%s/%s'%(self.path.parents[0],im.path)))
            for nb in self.path.parents[0].glob('*' + self.nb) :
                self.__set_xml_associate(PurePath(nb))
    def __set_body () :
        if self.__verify_path() :
            if self.__verify_image() :
                with open(self.path, 'rb') as f :
                    im = Image.open(io.BytesIO(f.read()))
                    self.image = self.body = im.load()
                    im.close()
            else if self.__verify_xml() :
                self.tree = self.body = ET.parse(self.path)
                self.__get_xml_associates()
                xml.close()
            else self.__verify_text_stream() :
                with open (self.path, 'r') as f :
                    self.text = self.body = f.read()
                    f.close()
        else
            self.body = None
    def __set_xml_associate(path) :
        if self.__verify_xml() :
            self.xml_files.append( { 'Format Data': _launch(PATH=path)})
    def get(stream) :
        if self.__verify_image() :
            im = Image.open(io.BytesIO(stream))
            self.body = im.load()
            im.close()
        else if self.__verify_xml() :
            self.tree = self.body.parse(stream) 
        else if self.__verify_text_streams 
                self.body = stream 
        else
            self.body = None
#### GIF File Class ####
class _gif_file(format_file) :
    __init__(self, { STORAGE: storage_class,
        PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        super().__init__(self, { STORAGE = storage_class,
             PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        if not self.__verify_gif() :
            print ('%s::%s::'%('Critical Failure: not a valid <.gif> file extension',self.ext)
            self.__del__()
    def __del__(self) :
        super(gif_file, self).__del__() 

#### NB File Class ####
class _nb_file(format_file) :
    __init__(self, { STORAGE: storage_class,
        PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        super().__init__(self, { STORAGE = storage_class,
             PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        if not self.__verify_nb() :
            print ('%s::%s::'%('Critical Failure: not a valid <.nb> file extension',self.ext)
            self.__del__()
    def __del__(self) :
        super(nb_file, self).__del__() 

#### PNG File Class ####
class _png_file(format_file) :
    __init__(self, { STORAGE: storage_class,
        PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        super().__init__(self, { STORAGE = storage_class,
             PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        if not self.__verify_png() :
            print ('%s::%s::'%('Critical Failure: not a valid <.png> file extension',self.ext)
            self.__del__()
    def __del__(self) :
        super(png_file, self).__del__() 

#### RGB File Class ####
class _rgb_file(format_file) :
    __init__(self, { STORAGE: storage_class,
        PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        super().__init__(self, { STORAGE = storage_class,
             PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        if not self.__verify_rgb() :
            print ('%s::%s::'%('Critical Failure: not a valid <.rgb> file extension',self.ext)
            self.__del__()
    def __del__(self) :
        super(rgb_file, self).__del__() 

#### TIFF File Class ####
class _tiff_file(format_file) :
    __init__(self, { STORAGE: storage_class,
        PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        super().__init__(self, { STORAGE = storage_class,
             PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        if not self.__verify_tiff() :
            print ('%s::%s::'%('Critical Failure: not a valid <.tiff> file extension',self.ext)
            self.__del__()
    def __del__(self) :
        super(tiff_file, self).__del__() 

#### ML File Class ####
class _xml_file(format_file) :
    __init__(self, { STORAGE: storage_class,
        PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        super().__init__(self, { STORAGE = storage_class,
             PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        if not self.__verify_xml() :
            print ('%s::%s::'%('Critical Failure: not a valid <.xml> file extension',self.ext)
            for format_data in self.xml_files :
               del format_data 
            self.__del__()
    def __del__(self) :
        super(xml_file, self).__del__() 

#### Object Store Class ####

# The Object Store Class is a derived class from data store class 
#  objects stores use AWS S3 to perform I/O operations
class _format_s3 (format_file) :
    __init__(self, { STORAGE:storage_class,
        PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        super().__init__(self, { STORAGE = storage_class,
             PATH:path, BUCKET:bucket, REGION:region, ROOT_DIR:root_dir, ACL:acl, DATA_TYPE:data_type })
        self.__set_service()
        s3 = boto3.resource( self.service, region_name = region )
        client = boto3.client( S3_SERVICE )
        self.__set_location ()
        self.__set_bucket()
        self.__set_key ()
        self.__set_body()
        self.__put_object()
        self.__set_uri ()
    def __del__(self):
        super(object_store, self).__del__() 
    def __get_xml_associates() : 
        if self.__verify_xml() :
            for im in self.tree.getroot().iter('img') :
                path = PurePath('%s/%s'%(self.path.parents[0],im.path))
                if path.exists :
                    format_data = _launch(STORAGE = 's3',
                        PATH = path, 
                        BUCKET = self.bucket,
                        ROOT_DIR = self.root_dir )
                    self.__set_xml_associate(format_data)))
            for path in self.path.parents[0].glob('*' + self.nb) :
                format_data = _launch(STORAGE = 's3',
                    PATH = path,
                    BUCKET = self.bucket,
                    ROOT_DIR = self.root_dir )
                self.__set_xml_associate(PurePath(format_data))
    def __get_s3_Object ()
        if self.s3_object == None : # S3_object doesn't exist
            try:
                s3.Object(self.bucket, self.key).load() #check to see if s3 object exists
            except botocore.exceptions.ClientError as e: 
                if e.response['Error']['Code'] == "404": # s3 object not found
                     self.s3_object = None
                else
                    raise
            else: # s3 object found
                self.s3_object = s3.Object(self.bucket, self.key).get()
    def __put_object () :
        if self.body != None : # body exists
            try:
                s3.Object(self.bucket, self.key).load() #check to see if s3 object exists
            except botocore.exceptions.ClientError as e: 
                if e.response['Error']['Code'] == "404": # s3 object not found
                    client.put_object( self.body, Bucket = self.bucket, Key = self.key)
                    client.put_object_acl( Bucket = self.bucket, Key = self.key, ACL = self.acl)
                else
                    raise
                if re.match('[Yy][Ee]?[Ss]?',response):
                    if self.__verify_path() :
                        client.put_object( self.body, Bucket = self.bucket, Key = self.key)
                        client.put_object_acl( Bucket = self.bucket, Key = self.key, ACL = self.acl)
        else
            self.body = None
    def __set_body () :
        if self.s3_object == None :
            self.__get_s3_object()
        if self.s3_object != None :
            if self.__verify_image() :
                im = Image.open(io.BytesIO(self.s3_object))
                self.image = self.body = im.load()
                im.close()
        else if self.__verify_xml() :
            with open(self.path, 'r') as f :
                xml = Text.open(io.StringIO(f.read()))
                self.tree = self.body = xml.parse()
                self.__get_xml_associates()
                xml.close()
        else if self.__verify_text_formats() :
            with open (self.path, 'r') as f :
                self.text = self.body = f.read()
                f.close()
        else
            self.body = None
    def __set_bucket
        self.bucket = PurePath(args[0].set_default(BUCKET: self.bucket)
    def __set_service()
        self.service = 's3'
    def __set_location()
        self.location = client.get_bucket_location(Bucket=bucket)['LocationConstraint']
    def __set_xml_associate(format_data) :
        if self.__verify_xml() :
            if re.match(self.tiff, format_data.format) :
                png_format_data = self.get_tiff2png(format_data)
                self.xml_files.append( { 'Format Data': png_format_data)})
            else :
                self.xml_files.append( { 'Format Data': format_data)})
    def __add_xml_images() :   
        self.xml_images = self.xml_images.append(
            'Object': im_object = xml_s3({im_path, self.root_dir, self.bucket, self.acl})
    def __add_xml_object(path)               
        if re.match(self.tiff,path.suffix) :
            png_image = get_tif2png (im_path)
        else
            png_image = get_body
        png_key = PurePath('%s/%s/%s'%(self.acl,self.root_dir,im_path.with_suffix(".PNG")))
        post_object (image, self.bucket, png_key) :
        uri = "https://s3-%s.amazonaws.com/%s/%s"%(self.location, self.bucket, png_key)
        self.xml_images.append(
            { 'Name': im_name,
            'Object': im_object = object_store(im_path, self.root_dir, self.bucket, self.acl)})
    def __set_key ():     
        self.key = PureFilePath('%s/%s/%s'%(self.acl, self.root_dir, self.path))
    def __set_uri ()
        self.uri = "https://s3-%s.amazonaws.com/%s/%s"%(self.location, self.bucket, self.key)
    def post_object (body, bucket, key) :
        client.put_object( body, Bucket = self.bucket, Key = key)
        client.put_object_acl( Bucket = self.bucket, Key = self.key, ACL = self.acl)
    def __post_object () :
        client.put_object( self.body, Bucket = self.bucket, Key = self.key)
        client.put_object_acl( Bucket = self.bucket, Key = self.key, ACL = self.acl)
        __set_uri ()
    def post_xml_s3 (xml) :
        if exclude-tiff == False : 
            xml.tif = [{name,image},...]
            for tif_name in xml.tif 
                tif_file_path = xml_file_path.with_name('tif_file_name')
                png_key = PureFilePath('%s/%s'(s3_dir,tif_file_path.with_suffix(".PNG")))
                uri = post_image_s3 ( body, bucket, png_key, acl, s3, client, location) :
                png_key = '%s/%s'%(OBJECT_ACL,PurePath(png.path))
         return (xml)
    def set_linked_image_files() : ...
    def set_linked_image_uris() : ...

S3_SERVICE = 's3'
S3_DIR = 'uspto/patent'
OBJECT_BUCKET = 'ocm-train-s3.std-dev-us'
OBJECT_ACL = 'public-read'

s3 = boto3.resource( S3_SERVICE, region_name = REGION )
client = boto3.client( S3_SERVICE )
location = client.get_bucket_location(Bucket=OBJECT_BUCKET)['LocationConstraint']

parser = argparse.ArgumentParser(description='Upload a tiff file to AWS-S3')
parser.add_argument('-t','--tif', required=True, type=Path, nargs=1, default="",
                    help='local path to a TIFF file')
parser.add_argument('-x','--xml', required=True, type=Path, nargs=1, default="",
                    help='local path to an ML file')
parser.add_argument('-e','-E','--exclude-image', required=False, type=Boolean, nargs=0, default=False,
                    help='Used with -x option; Post ML only; image files remain local' 
args = parser.parse_args()
if args.tif[0].verify :
    png = {
        tif_path: args.tif[0],
        path: args.tif[0].with_suffix("PNG"),
        bucket: OBJECT_BUCKET,
        s3_dir: S3_DIR,
        acl: OBJECT_ACL,
        location: location,
        key: '',
        uri: '',
        image: NULL }
    png.key = PureFilePath('%s/%s/%s'%(png.acl,png.s3_dir,png.path))
    png = tif2png (png)
try:
    s3.Object(png.bucket, png.key).load()
except botocore.exceptions.ClientError as e: 
    if e.response['Error']['Code'] == "404": # file not found
        png = post_png_s3 (png) :
    else
        raise
else: # file found
    response = input('/%s/%s::%s'%(bucket, key, 'already exists, would you like to over-write it?\n'))
    if re.match('[Yy][Ee]?[Ss]?',response):
        png = post_png_s3 (png) :

if args.xml[0].verify :
    xml = {
        path: args.xml[0],
        ximage: exclude-image,
        bucket: OBJECT_BUCKET,
        s3_dir: S3_DIR,
        acl: OBJECT_ACL,
        location: location,
        tif = [{ name: '', image: NULL}],
        png = [png],
        key: '',
        uri: '',
        tmesree: '',
        root: '' }
   xml.key = PureFilePath('%s/%s/%s'%(xml.acl,xml.s3_dir,xml.path))
   xml.tree = ET.parse(xml_file_path)
   xml.root = tree.getroot()
   xml = post_xml_s3 (xml) 

sys.exit()

#! /usr/bin/python3

import boto3
import botocore
import jmespath
import json
import os
import re

#  Request and filter interesting 'ec2' Instances from AWS's 'ec2' Service.
#  Instances are filtered using 'ec2' Tags[Name/Value Pairs].
#  Interesting Instances are sent to S3 as a list of json objects
#  Interesting Instances are queried -- JmesPath 
#  Options interface is planned as a future enhancement.

#############   Intialize varaibles ###############

instances = []
InstanceIds = []

#############   Setup the Environment   ###############

REGION = 'us-east-2'

EC2_SERVICE = 'ec2'
TAG = 'Name'
INSTANCES = ['ocm-train-ubuntu2004.ebs.t2.micro1-dev-us', 'ocm-train-ec2(ub2004.ebs.t2.mic2)vpc(cust1)-dev-us' ] 
#FILTERS = [{ 'Name':'%s%s'%('tag:',TAG), 'Values':INSTANCES}]
INSTANCE_PATTERN = [ 'ocm*' ]
FILTERS = [{ 'Name':'%s%s'%('tag:',TAG), 'Values':INSTANCE_PATTERN}]

S3_SERVICE = 's3'
OBJECT_BUCK = 'ocm-train-s3.std-dev-us'
OBJECT_ACL = 'public-read'
OBJECT_DIR = 'Sys-mgt'
OBJECT_FORMAT = 'json'
OBJECT_NAME = '%s.%s'%('Instances',OBJECT_FORMAT)
OBJECT_KEY = '%s/%s/%s'%(OBJECT_ACL,OBJECT_DIR,OBJECT_NAME)

# connect with the EC2 service; use instance 'Tags' to filter interesting instances
ec2 = boto3.resource( EC2_SERVICE, region_name = REGION)
client = boto3.client( EC2_SERVICE )
[ ec2.Instance(id = 'i-054b01cb726c32afd'), ec2.Instance(id = 'i-037345e13d2095e99') ]
instances = list(ec2.instances.filter(Filters=FILTERS))

#############   Serialize the interesting Instances ###############

jsonStr = '[\n' # open the json object array for serializing json objects.
for instance in instances:
    response = client.describe_instances( InstanceIds = [instance.id])
    jsonStr = '%s%s,'%(jsonStr,json.dumps(response, sort_keys='True', indent=4, default=str, separators=(',',': ')))
jsonStr = '%s%s'%("}".join(jsonStr.rsplit("},", 1)),']') # Delete the last comma and close the object array

# connect with the S3 service
s3 = boto3.resource( S3_SERVICE, region_name = REGION )
client = boto3.client( S3_SERVICE )

#############   Post the Instances to S3 -- a json array publically readable on the Internet ###############

location = client.get_bucket_location(Bucket=OBJECT_BUCK)['LocationConstraint']
try:
    s3.Object(OBJECT_BUCK, OBJECT_KEY).load()
except botocore.exceptions.ClientError as e: 
    if e.response['Error']['Code'] == "404": # file not found
        client.put_object( 
            Body = json.dumps(jsonStr, sort_keys='True', indent=4, default=str, separators=(',',': ')), 
            Bucket = OBJECT_BUCK,
            Key = OBJECT_KEY)
        client.put_object_acl( Bucket = OBJECT_BUCK, Key = OBJECT_KEY, ACL = OBJECT_ACL)
        url = "https://s3-%s.amazonaws.com/%s/%s"%(location, OBJECT_BUCK, OBJECT_KEY)
        print (url)
    else: # Exception
        raise
else: # file found
    response = input('/%s/%s %s'%(OBJECT_BUCK, OBJECT_KEY, 'already exists, would you like to over-write it?\n'))
    if re.match('[Yy][Ee]?[Ss]?',response):
        client.put_object( 
            Body = json.dumps(jsonStr, sort_keys='True', indent=4, default=str, separators=(',',': ')), 
            Bucket = OBJECT_BUCK,
            Key = OBJECT_KEY)
        client.put_object_acl( Bucket = OBJECT_BUCK, Key = OBJECT_KEY, ACL = OBJECT_ACL)
        url = 'https://s3-%s.amazonaws.com/%s/%s'%(location, OBJECT_BUCK, OBJECT_KEY)
        print (url)

#############   Query the json dictionary ###############

jsonInstanceList = json.loads(jsonStr)
srchStr = "[*].Reservations[*].Instances[0].Tags[0]"
print(json.dumps(jmespath.search(srchStr,jsonInstanceList), sort_keys='True', indent=4, default=str, separators=(',',': ')))
