# Mini project for an automated ETL script

This mini project aims to go over creating a basic ETL pipeline (with Python and SQL) and attempts to automate a daily process with Airflow.

The goal is to:

1. Scrape daily covid data from 3 countries from the [CoronaTracker](https://documenter.getpostman.com/view/11073859Szmcbeho) API
2. Clean and transform the data into a suitable tabular format (DataFrame)
3. Load the cleaned data into a SQLite database.
4. BONUS: Automate the process to repeat on a daily interval.

## Airflow setup instructions (for future me to refer to)

In your CLI (project directory):

Run in a virtual environment:

1. `virtualenv airflow-venv && source airflow-venv/bin/activate`
2. `pip freeze`

Referring to [Airflow's quick start guide](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html) (running locally):

(Still in your CLI):

3. `export AIRFLOW_HOME=~/airflow`
4. `pip install apache-airflow` (NOTE: `pip uninstall apache-airflow` and reinstall if encountering errors)

Initialize the database

    5. `airflow db init`

Create your user credentials (you'll be asked to provide a password too:):

`airflow users create \`
    `--username admin \`
    `--firstname John \`
    `--lastname Doe \`
    `--role Admin \`
    `--email email@email.com`

Go back to your home directory (`cd`), there should be an airflow folder

5. `cd airflow && nano airflow.cfg`

In the text editor there should be a 'dags_folder' filepath. Change this filepath to the appropriate /dags folder in your project directory.

Now you can activate your airflow webserver

6. `airflow webserver --port 8080`

You'll need to open a new terminal window to open the scheduler (both this and the webserver need to be active).

7. Open a venv again `source airflow-venv/bin/activate`
8. `airflow scheduler`

Now you can open a browser window

9. localhost:8080 and type your user & password.

## CAVEATS/FUTURE IMPROVEMENTS:

- Gain a better understanding of Airflow interface & capabilities
- Troubleshoot on why DAGs remain stuck in scheduled
- Utilize in a Docker container for portabiilty
- Make use of cloud services for scalability
- Identify other potential failure/error points in pipeline
