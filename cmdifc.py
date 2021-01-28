#! /usr/bin/python3
import sys
import argparse
## argpase.NameSpace()
class smifc :
    name = 'Joe'
    def __init__(self) :
        print ('hello world')

    def fs( fmt, path) :
        print( '%s, %s' %(fmt, path))

    def s3( fmt, path, acl, bucket, region) :
        print( '%s, %s, %s, %s, %s' %(fmt, path, acl, bucket, region))

    def st (fmt, name, stream) :
        print ('%s, %s, %s'%(fmt, name, stream))
print(f'Arguments count: {len(sys.argv)}')
for i, arg in enumerate(sys.argv) :
    print(f"Argument {i:>6}: {arg}") #note -- integer right justified 6 characters
### ns = namespace ###
ns = smifc()
ns.func = lambda fmt, path : smifc.fs(fmt, path)
p = argparse.ArgumentParser(prog='cmdifc.py')
xg = p.add_mutually_exclusive_group(required=True)
sp = p.add_subparsers()
print (f'ns: {ns}')
print (f'ns.fs: {ns.fs}')
print (f'vars(ns): {vars(ns)}')
cmds = [ 'fs', 's3', 'st' ] 
for cmd in cmds :
    cmds[cmd] = [
        setattr(ns,cmd,xg.add_argument(f'--{cmd}',action='store_true')),
        setattr(ns,f'sp_{cmd}',sp.add_parser(cmd))
    ]
    p.parse_args([f'--{cmd}'],namespace=ns)
    print (f'ns: {ns}')
    print (f'ns.cmd: {ns}.{cmd}')
    print (f'vars(ns): {vars(ns)}')
#sp_fs = sp.add_parser('fs')
#`    setattr(ns,cmd,xg.add_argument(f'--fs',action='store_true'))
#for cmd in cmds :
#`    setattr(ns,cmd,xg.add_argument(f'--fs',action='store_true'))
#    p.parse_args([sys.argv[1]],namespace=ns)
#    print (f'ns: {ns}')
#    print (f'ns.fs: {ns.fs}')
#    print (f'vars(ns): {vars(ns)}')
#    cmds[cmd] = [
#    setattr(ns,f'{cmd}',f'xg.add_argument(--{cmd},action="store_true"')
#    p.parse_args([f'--{cmd}'],namespace=ns)
#        setattr('argparse','ArgumentParser','%s_%s'%('sp',cmd),sp.add_parser(sp))]
#print (f'xg_{cmd}: {xg_cmd}')
#   verify_storage_mgr_format -- needs to be a factory pattern (@classmethod)
#    if re.match(cmd, sys.argv[1].lower()) :
#        cmdline = sys.argv[1:]
#    elif re.match(cmd, sys.argv[2].lower()) :
#        cmdline = sys.argv[2:]
#p.parse_args(args=sys.argv, namespace=ns)
#print (f'ns: {ns}')
#print (f'ns.fs: {ns.fs}')
#print (f'vars(ns): {vars(ns)}')

#xg = p.add_mutually_exclusive_group(required=True)
#fs_xg = xg.add_argument('-f','--fmt', metavar='format', required=False, type=str, nargs=1, 
#    help='Specify data format. Supported data formats: gif, jpeg, nb, png, rgb, text, tiff, xml')
#s3_xg = xg.add_argument('-o','--ofmt', metavar='obj-format', required=False, type=str, nargs=1,
#    help='Specify data format. Supported data formats: gif, jpeg, nb, png, rgb, text, xml.')
#st_xg = xg.add_argument('--st', action='store_true', help='Usage:smifc --st')
