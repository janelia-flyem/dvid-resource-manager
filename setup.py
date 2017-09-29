from setuptools import find_packages, setup

setup( name='dvid_resource_manager',
       version='0.2',
       description='A simple server to manage large-scale batch requests to DVID (or other resources).',
       url='https://github.com/janelia-flyem/DVIDResourceManager',
       packages=find_packages(),
       package_data={},
       entry_points={
          'console_scripts': [
              'dvid_resource_manager = dvid_resource_manager.server:main'
          ]
       }
     )
