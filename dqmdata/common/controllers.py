# Import flask dependencies
from flask import Blueprint, request, render_template, \
                  flash, g, session, redirect, url_for, current_app
from dqmdata import db
from dqmdata.common.models.run import LocalRun

common = Blueprint('common', __name__, url_prefix='/common')
