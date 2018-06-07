from setuptools import setup

def readme():
    with open('README.md') as f:
        return f.read()

setup(name='paramtaskrun',
      version='0.1',
      description='',
      url='http://github.com/hlibbabii/paramtaskrun',
      author='Hlib Babii',
      author_email='hlibbabii@gmail.com',
      license='MIT',
      packages=['paramtaskrun'],
      scripts=['bin/ptr'],
      classifiers=[
          'Development Status :: 2 - Pre-Alpha',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Natural Language :: English',
          'Programming Language :: Python :: 3',
          'Topic :: Software Development :: Build Tools'
      ],
      keywords='params parameters task runner build',
      install_requires=[
          'deepdiff'
      ],
      zip_safe=False)
