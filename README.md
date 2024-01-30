# Requirements
- `python3`
- `pip`
- `docker`
- `docker-compose`

# Up and running
- Install all requirements: `pip install -r requirements.txt`;
- Make the initialize redshift script executable: `chmod +x init/init.sql`;
- Launch `Airflow`: `docker-compose up`;
- Check `Airflow` is up: `http://localhost:8080`(username and password are `airflow`);
- Launch the dag: `process_csv`;
- Use the script `run_queries_redshift.sh` to run an example of analaytics query: `./run_queries_redshift.sh most_active_users`.

# Project structure
```
│
│
├── app/
│   └── docker-compose.yaml      # services required to run Airflow
│
├── init/
│   └── init.sql                 # sql script to initialize the target database and table (redshift)
│
├── dags/
│   └── process_csv_dag.py       # Airflow DAG definition
│
├── plugins/
│   └── operators/
│       └── csv_processor.py     # Custom Airflow operator for CSV processing
│
├── scripts/
│   └── process_csv.py           # Python script for CSV processing
│   └── data_transformation.py   # Python script for data transformation 
│   └── clean_redshift_table.py  # Python script for data cleaning
│   └── upload_to_redshift.py    # Python script for copy command
│
├── tests/                       # Tests directory
│
├── run_queries_redshift.sh      # example of queries to run analytics
│
├── requirements.txt             # Python dependencies
│
├── docker-compose.yml           # Airflow and its dependencies
│
├── flake.nix                    # Project's packages
│
├── flake.lock                   # Fixed project's packages versions
│
└── README.md                    # Project documentation
```

# Dag description
- Following ETL best practices, this etl is **idempotent**;
- The first task, connects to `S3`, fetches the file (if exists), process it as `dataframe` (chunks), checks if the duplicates are really duplicates not just an uuid clash (if true duplicates remove the row else change the uuid), extracts company from the email address, keeps only the data related to the processing month and year and finally store it in temporary file, in this example the temporary file is under `Airflow` named `processed_file_path`, the path is shared via `XCom`; For production, we would use an `S3` bucket; This file is used, to prevent relaunching the dag when there's failures in the next tasks;
- The second task is a place holder for data transformation;
- the third task, reads the temporary file `processed_file_path`, insert all ids in a temporary table `temp_ids`, removes from the target table `events` all the ids existing in `temp_ids`, the temporary table is used to store ids, to avoid having slow queries on delete;
- The last task, reads the temporary file, and inserts in redshift table `events`.

![dag_image](/assets/process_csv_dag.png)


# target database tables: analytics
- The target database is `[Redshift] (https://aws.amazon.com/redshift/)`, the company is already using `S3` `AWS`, using another service from the same cloud provider lowers this project complexity;
- For this example, We don't have an AWS account to test `redshift`, we use `postgres` to simulate redshift;
- The target table is `events`, its schema is under `scripts/init.sql`;
- `events`: `event.id` should be the distribution key and `timestamp` the sort key;
- `temp_ids` `temp_ids.id` should be the distribution key.

# industrialization 
- First, before industrialization I would heavily unit test and manual test the DAG;
- Then, I would deploy `Airflow` with the dag in `Beta` environment and continue testing with production like load; 
- Prepare my `CI/CD`, once we have a commit on `main` branch, automatically deploy to `Beta`; deployment to production would be on developer action;
- Usually, when the workload is unknown, I would install `Airflow` with `1 worker` with `CeleryExecutor`, then `test and learn` to adjust the number of instances;
- If the load is very heavy, I would try a `KubernetesExecutor`;
- To make the DAG recurring, I would use `Airflow` scheduling features;
- To handle errors and monitor the results, I would activate the email on failure feature, to warn the team when the dag fails, log aggregation using `ELK` is usually the way to go to ease the monitoring;
- Before deploying to production, I would change the temporary file location from `Airflow` to `S3`;
- To monitor the DAG execution, I would use `Airflow` web UI and `Flower` to monitor and administrate Celery cluster:
`docker-compose up flower` and then `localhost:5555`;

# insights: future developments and analytics opportunities
- Tag analysis: identify most used tags, so we gain insight into trends and preferences among users;
- Geographical activity: Identify most active countries; this can be used for targeted marketing and regional dynamics;
- URI and action correlation: understand how users interact with uris and actions can offer insights into users behaviors and preferences; this reveals pattern in users navigation and can guide future developments and optimize user flow;
- Long-term data: analyze data over extended periods to identify shifts in user behavior

# bonus
- to make the development environment reproducible, this project uses a combination of [Nix](https://nixos.org/) + [direnv](https://direnv.net/);
- `Nix` creates isolated environments for each project; we specify dependencies explicitly in `flake.nix`. This guarantees that every developer works with exactly the same dependencies, down to the version number, reducing the "works on my machine" problem;
- The same `Nix` configuration can be used in both development and CI, ensuring consistency across all stages of development;
- `flake.lock` assures that all the developers are using the same packages versions.
