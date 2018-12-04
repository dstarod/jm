from distutils.core import setup
import shutil
import distutils.command.install_scripts


class StripDot(distutils.command.install_scripts.install_scripts):
    def run(self):
        distutils.command.install_scripts.install_scripts.run(self)
        out_files = self.get_outputs()
        for i, script in enumerate(out_files):
            if script.endswith(".py"):
                newname = script[:-3]
                shutil.move(script, newname)
                out_files[i] = newname


if __name__ == '__main__':
    setup(
        name='json-filter',
        version='0.3.2',
        url='https://github.com/dstarod/json-filter',
        description='JSON filter with MongoDB shell syntax',
        author='dstarod',
        author_email='dmitry.starodubcev@gmail.com',
        scripts=['bin/jf.py'],
        py_modules=['jsonfilter.match'],
        cmdclass={"install_scripts": StripDot},
        classifiers=['Topic :: Utilities'],
        keywords=['json', 'mongodb'],
        download_url='https://github.com/dstarod/json-filter/tarball/0.3.2'
    )
