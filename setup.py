from setuptools import setup

setup(
    name='DQMFlask',
    packages=['dqmdata'],
    include_package_data=True,
    install_requires=[
        'flask', 'flask-sqlalchemy', 'flask_migrate'
    ],
)
