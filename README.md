# score_calculation

## How to run?

### Prerequisites:

1. Install Python.
   * Download & Install latest version of python from [here](https://www.python.org/downloads/)
2. Update the cassandra_config.ini file.
   ```
    IP_ADDRESS = <<IP>>
    PORT = <<Port>>
    USER = <<User>>
    PWD = <<Password>>
    KEY_SPACE = <<KeySpace>>
    ```

    Ex. Without quote
    ```
    IP_ADDRESS = 255.255.0.0
    PORT = 9042
    USER = username
    PWD = password
    KEY_SPACE = keyspace
    ```

3. Install required libraries from requirements.txt file.
    * Open command prompt and go to project directory.
    * Execute the following command

    ```
    pip install -r requirements.txt
    ```

4. Run the effulgenz_score.py file.
    ```
    python effulgenz_score.py
    ```

## Output

1. Script will create data, log and score folder in the project directory.
    * data - all parquet files
    * log - log files
        * last 10 log files only available (configurable in logging.conf file)
    * score - all score files in csv




