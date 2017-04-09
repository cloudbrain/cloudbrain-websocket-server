from setuptools import find_packages, setup


def findRequirements():
    """
    Read the requirements.txt file and parse into requirements for setup's
    install_requirements option.
    """
    return [line.strip()
            for line in open("requirements.txt").readlines()
            if not line.startswith("#")]


setup(name="cbws",
      package_dir={'': 'src'},
      packages=find_packages('src'),
      version="0.0.2",
      description="CloudBrain websocket server",
      author="Alessio Della Motta, Marion Le Borgne, William Wnekowicz",
      url="https://github.com/cloudbrain/cloudbrain-websocket-server",
      install_requires=findRequirements(),
      license=open('LICENSE.txt').read(),
      long_description=open('README.md').read())
