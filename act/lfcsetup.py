from distutils.core import setup, Extension

module1 = Extension('arclfc',
                    sources = ['arclfc.c'],
                    include_dirs = ['/opt/lcg/include/lfc'],
                    libraries = ['lfc'],
                    library_dirs = ['/opt/lcg/lib64'])

setup (name = 'PackageName',
       version = '1.0',
       description = 'This is a demo package',
       ext_modules = [module1])
