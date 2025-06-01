# ETL Pipeline Automation with Cron Jobs

## 1. Introduction to Cron

`cron` is a time-based job scheduler in Unix-like operating systems. It allows you to schedule commands or scripts to run automatically at specified intervals (e.g., daily, weekly, hourly). For an ETL pipeline, `cron` is a fundamental tool for automating the data loading process.

## 2. Setting Up the Cron Job for `etl_pipeline.py`

To ensure your ETL script runs automatically, follow these steps:

### 2.1. Make Your Script Executable

The `etl_pipeline.py` script needs to be executable by the system.
* **Add Shebang:** Add the following line as the very first line of your `etl_pipeline.py` file:
    ```python
    #!/usr/bin/env python3
    ```
    This tells the system which interpreter to use when running the script directly.
* **Grant Execute Permissions:** Open your terminal and navigate to the directory where `etl_pipeline.py` is located. Then, run:
    ```bash
    chmod +x etl_pipeline.py
    ```

### 2.2. Use Absolute Paths for Data Files (Crucial for Cron)

Cron jobs run with a limited environment and often don't have the same working directory as when you manually run a script. Therefore, all paths to your source CSV files within `etl_pipeline.py` must be absolute.

* **Modification in `etl_pipeline.py`:**
    ```python
    import os
    # ... other imports

    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct absolute paths for your CSV files
    AMAZON_REPORT_PATH = os.path.join(script_dir, 'data', 'Amazon Sale Report.csv')
    INTERNATIONAL_REPORT_PATH = os.path.join(script_dir, 'data', 'International sale Report.csv')
    PRODUCT_MASTER_PATH = os.path.join(script_dir, 'data', 'Sale Report.csv')
    # ... (apply similar logic for your .env file path if using python-dotenv)
    ```

### 2.3. Configure the Cron Job

You will edit your user's crontab (cron table) to add the scheduling entry.

* **Open Crontab:** In your terminal, run:
    ```bash
    crontab -e
    ```
    This will open a text editor (usually `nano` or `vim`) with your crontab file.

* **Add Cron Entry:** Add a line at the end of the file. Here's an example to run your ETL pipeline every day at 3:00 AM:
    ```cron
    0 3 * * * /path/to/your/venv/bin/python /path/to/your/etl_pipeline.py >> /path/to/your/etl_pipeline_cron.log 2>&1
    ```
    * **`0 3 * * *`**: This is the cron schedule.
        * `0`: Minute (0-59)
        * `3`: Hour (0-23, where 0 is midnight)
        * `*`: Day of month (1-31, `*` means every day)
        * `*`: Month (1-12, `*` means every month)
        * `*`: Day of week (0-7, where 0 and 7 are Sunday, `*` means every day)
    * `/path/to/your/venv/bin/python`: **Absolute path** to the Python interpreter within your virtual environment. You can find this by activating your venv and running `which python`.
    * `/path/to/your/etl_pipeline.py`: **Absolute path** to your ETL script.
    * `>> /path/to/your/etl_pipeline_cron.log 2>&1`: This redirects all standard output and standard error from your script to a log file. This is crucial for debugging cron jobs, as errors won't be printed to your terminal.

* **Save and Exit:**
    * If using `nano`, press `Ctrl+O` to save, then `Enter`, then `Ctrl+X` to exit.
    * If using `vim`, press `Esc`, then `:wq` (write and quit), then `Enter`.

## 3. Monitoring and Rationale

* **Verification:** After saving, `crontab -l` will show your active cron jobs.
* **Logging:** Regularly check your `etl_pipeline_cron.log` file for output and errors to ensure the pipeline is running successfully.
* **Rationale for Automation:** Even with static source files, automating the ETL pipeline is a crucial learning step in data engineering. It prepares you for handling incremental data updates, simulates real-world production environments, and ensures data freshness without manual intervention.