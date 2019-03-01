# Import flask dependencies
from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for, current_app
from dqmdata import db
from dqmdata.common.models.local_run import LocalRun

common = Blueprint('common', __name__, url_prefix='/common')

import click
import math
from dqmdata import app

# Routes
@common.route('/emap/', methods=['GET'])
def emap():
	pass


# Command line utilities

