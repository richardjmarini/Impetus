from distutils.core import setup, Extension

setup(
   name= 'Impetus',
   version= '1.0',
   description= 'Impetus Auto-scaling Asynchronous Distributed Processing Framework',
   author= 'Richard J. Marini',
   author_email= 'richardjmarini@gmail.com',
   url= 'https://github.com/richardjmarini/Impetus',
   long_description= '''
      Impetus is an auto-scaling asynchronous distributed processing framework originally designed for the purpose of building a distributed crawler with machine learning analytics. The Impetus Framework is the auto-scaling asynchronous distributed processing sub-system of that distributed crawler. 
   ''',
   py_modules= ['impetus']
)

