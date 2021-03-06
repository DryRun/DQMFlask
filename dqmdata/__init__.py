# Import flask and template operators
from flask import Flask, render_template
from flask_migrate import Migrate
 
# Import SQLAlchemy
from flask_sqlalchemy import SQLAlchemy

# Define the WSGI application object
app = Flask(__name__)

# Configurations
app.config.from_object('config')

# Define the database object which is imported
# by modules and controllers
db = SQLAlchemy(app)
migrate = Migrate(app, db)

# Register blueprint(s)
# Import a module / component using its blueprint handler variable (hcal_local)
from dqmdata.common.controllers import common as common
from dqmdata.hcal_local.controllers import hcal_local as hcal_local
from dqmdata.hcal_online.controllers import hcal_online as hcal_online

app.register_blueprint(common)
app.register_blueprint(hcal_local)
app.register_blueprint(hcal_online)
# app.register_blueprint(xyz_module)
# ..

# Sample HTTP error handling
@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404

@app.route('/')
@app.route('/index.html')
def index():
	return '''
<html>
	<body>
		Welcome to HCAL DQMFlask, written by David Yu. This is only a backend API, not a website!
	</body>
</html>'''


# Build the database:
# This will create the database file using SQLAlchemy
#db.create_all() 

def wsgi(*args, **kwargs):
    return app

if __name__ == "__main__":
	app.run()
