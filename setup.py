import setuptools

setuptools.setup(name='ocycle',
                 version='0.0.4',
                 description='Stream buffering and event triggering for unbalanced frame sizes.',
                 long_description=open('README.md').read().strip(),
                 long_description_content_type='text/markdown',
                 author='Bea Steers',
                 author_email='bea.steers@gmail.com',
                 url='https://github.com/beasteers/ocycle',
                 packages=setuptools.find_packages(),
                 install_requires=[],
                 license='MIT License',
                 zip_safe=False,
                 keywords='buffer callback io bytes bytesio unequal uneven '
                          'frame size thread multiprocessing')
