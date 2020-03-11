#!/usr/bin/env python

from setuptools import setup, find_packages
from setuptools.command.install import install

import importlib.util
from pathlib import Path

version_file = Path(__file__).parent.joinpath('pyworkers', 'version.py')
spec = importlib.util.spec_from_file_location('pyworkers_version', version_file)
pyworkers_version = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pyworkers_version)

class build_maybe_inplace(install):
    def run(self):
        _dist_file = version_file.parent.joinpath('_dist_info.py')
        assert not _dist_file.exists()
        #if not _dist_file.exists():
        with open(_dist_file, 'w') as df:
            df.write('\n'.join(map(lambda attr_name: attr_name+' = '+repr(getattr(pyworkers_version, attr_name)), pyworkers_version.__all__)) + '\n')

        return super().run()


setup(name='PyWorkers',
      version=pyworkers_version.__version__,
      description='Library providing abstraction over different types of parallel jobs: threads, processes and remote processes.',
      author='Łukasz Dudziak (SAIC-Cambridge, On-Device Team)',
      author_email='l.dudziak@samsung.com',
      url='https://github.sec.samsung.net/l-dudziak/pyworkers',
      download_url='https://github.sec.samsung.net/l-dudziak/pyworkers',
      python_requires='>=3.6.0',
      install_requires=[],
      packages=find_packages(where='.', exclude=['tests']),
      package_dir={ '': '.' },
      cmdclass={
          'install': build_maybe_inplace
      }
)
