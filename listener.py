import argparse
import concurrent.futures
import datetime
import json
import logging
import os
import signal
import sys
import threading
import time
import traceback

import psycopg2
import psycopg2.extensions
from dotenv import load_dotenv
from decimal import Decimal

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


exit_flag = threading.Event()

# Load environment variables from the .env file
load_dotenv()

# Argument parser
parser = argparse.ArgumentParser(description="Odoo Orders Listener - Sheets Sync")

event_trigger = 'pos_order'
# Default Odoo Tables
dbtbl_orders_table = "pos_order"
dbtbl_order_line = "pos_order_line"
dbtbl_payment = "pos_payment"
dbtbl_payment_method = "pos_payment_method"
dbtbl_product_tagging = "product_tag_product_template_rel"

# Add command-line arguments
parser.add_argument(
    "action",
    choices=[
        "listen",
        "install",
        "uninstall",
        "service_install",
        "service_uninstall",
        "service",
    ],
    nargs="?",
    default="listen",
    help="Specify the action to execute",
)

parser.add_argument(
    "--json_db_list_src",
    type=str,
    help="A json file holding the list of database configs to listen",
)

parser.add_argument(
    "--log_directory",
    type=str,
    help="Log directory storage",
)
parser.add_argument(
    "--service_account_json",
    type=str,
    help="JSON credentials from google service account",
)
parser.add_argument(
    "--google_sheet_id",
    type=str,
    help="The google sheet id; can be found in the url",
)
parser.add_argument(
    "--tag_id",
    type=int,
    help="The product tag id to sync or sync all if unset",
)
parser.add_argument(
    "--google_sheet_range",
    type=str,
    default="Sheet1!A2",
    help="Sheet name and range as starting point when adding data",
)
parser.add_argument(
    "--with_console",
    type=str,
    default="yes",
    help="'yes' to Enable or 'no' to Disable console logs",
)

args = parser.parse_args()

glob_tag_id = (
    args.tag_id
    if args.tag_id is not None
    else os.environ.get("tag_id")
)

# Google sheet ID and range to insert data into
SHEET_ID = (
    args.google_sheet_id
    if args.google_sheet_id is not None
    else os.environ.get("google_sheet_id")
)
RANGE = (
    args.google_sheet_range
    if args.google_sheet_range is not None
    else os.environ.get("google_sheet_range")
) or 'Sheet1!A2'
SERVICE_ACCOUNT_JSON = (
    args.service_account_json
    if args.service_account_json is not None
    else os.environ.get("service_account_json")
) or 'credentials.json'

# Console log handler
with_console = (
    False
    if (
        args.with_console
        if args.with_console is not None
        else os.environ.get("with_console")
    )
    != "yes"
    else True
)


# Get current username
def getusername():
    return (
        os.environ["USER"] if "USER" in os.environ else os.environ["LOGNAME"]
    ) or os.getlogin()


# The log directory
log_dir = (
    args.log_directory
    if args.log_directory is not None
    else os.environ.get("log_directory")
) or "logs"
log_dir = os.path.abspath(log_dir)

# Set the working directory explicitly to the directory where the log file is located
os.chdir(os.path.dirname(log_dir))

# Create a "logs" directory if it doesn't exist
os.makedirs(log_dir, exist_ok=True)

# Define the log file path based on the current date
log_file = os.path.join(log_dir, f"{getusername()}@{datetime.date.today()}.log")

# Set up logging to log both to a file and to the console (CLI)
logging_handlers = [logging.FileHandler(log_file, "a")]
if with_console:
    logging_handlers.append(logging.StreamHandler())
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=logging_handlers,
)
logger = logging.getLogger(__name__)

#
# Service Information
#
service_name = "Odoo Orders Listener by Accountador"
service_unit_name = os.environ.get("service_unit_name") or "accountador_order_listener.service"
service_dir = "/etc/systemd/system"

# JSON database List
json_db_list_src = (
    args.json_db_list_src
    if args.json_db_list_src is not None
    else os.environ.get("json_db_list_src", "db.config.json")
)
try:
    if json_db_list_src:
        with open(json_db_list_src, "r") as json_file:
            all_databases = json.load(json_file)
    else:
        logger.error(
            "Error: A json file holding the list of databases to listen is required."
        )
        sys.exit(1)

except FileNotFoundError:
    logger.error(f"The file '{json_db_list_src}' was not found.")
    sys.exit(1)

except json.JSONDecodeError as e:
    logger.error(f"Error decoding JSON: {e}")
    sys.exit(1)

except Exception as e:
    logger.error(f"Error: {str(e)}")
    traceback.print_exc()

def convert_decimal_fields(data):
    """
    Recursively converts Decimal fields in a dictionary to float.
    """
    for key, value in data.items():
        if isinstance(value, Decimal):
            data[key] = float(value)  # Convert Decimal to float
        elif isinstance(value, dict):
            # If the value is a nested dictionary, recurse
            data[key] = convert_decimal_fields(value)
    return data

def format_sync_data(data):
    return [
        [
            entry["datetime"],
            entry["pos_reference"],
            entry["amount_total"],
            entry["full_product_name"],
            entry["qty"],
            entry["price_unit"],
            entry["price_subtotal_incl"],
            entry["payment_method"]
        ]
        for entry in data
    ]
    

def sync_to_sheet(data):
    formatted_data = format_sync_data(data)
    # Load credentials from the service account JSON file
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_JSON, scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    # Build the Google Sheets API client
    service = build('sheets', 'v4', credentials=credentials)
        
    try:
        # Use 'spreadsheets.values.append' to add data to the sheet
        request = service.spreadsheets().values().append(
            spreadsheetId=SHEET_ID,
            range=RANGE,
            valueInputOption="RAW",  # or "USER_ENTERED" if you want Google Sheets to interpret values
            body={"values": formatted_data}
        )
        response = request.execute()
        print("Data added successfully:", response)
    except HttpError as error:
        print(f"An error occurred: {error}")

def connect_with_retry(config):
    max_retries = 2  # Number of maximum connection retry attempts
    retry_delay = 20  # Delay in seconds between retry attempts

    for attempt in range(max_retries):
        try:
            # Attempt to establish a connection to the PostgreSQL database
            connection = psycopg2.connect(**config["db"])

            # If the connection is successful, return it
            return connection
        except psycopg2.OperationalError as e:
            print(f"Connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

    # If all retry attempts fail, raise an exception
    raise Exception("Unable to establish a database connection after multiple retries")

def get_order_info(config, order_id):
    try:
        # Connect to the database
        conn = psycopg2.connect(**config["db"])
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        payment_methods = []

        # Get Payment Method Id
        cur.execute(
            f"SELECT payment_method_id, amount FROM {config['dbtbl_payment']} WHERE pos_order_id={order_id}"
        )
        row = cur.fetchall()
        pm_column_names = [desc[0] for desc in cur.description]
        for item in row:
            payment_method = convert_decimal_fields(dict(zip(pm_column_names, item)))
            # Get the payment method label
            cur.execute(
                f"SELECT name FROM {config['dbtbl_payment_method']} WHERE id={payment_method['payment_method_id']}"
            )
            label = cur.fetchone()[0]['en_US']
            payment_method.update({ "label": label})
            payment_methods.append(payment_method)
        
        formated_payment_method = ','.join([f"{item['label']}" for item in payment_methods])
        order_lines = []

        # Get Order Line
        cur.execute(
            f"SELECT product_id, full_product_name, qty, price_unit, price_subtotal_incl FROM {config['dbtbl_order_line']} WHERE order_id={order_id}"
        )
        # Fetch all rows
        row = cur.fetchall()
        ol_column_names = [desc[0] for desc in cur.description]
        for item in row:
            order_line = convert_decimal_fields(dict(zip(ol_column_names, item)))
            product_id = order_line["product_id"]
            # Check the product id if tagged to sync
            cur.execute(
                f"SELECT product_tag_id FROM {config['dbtbl_product_tagging']} WHERE product_template_id={product_id} AND product_tag_id={config["tag_id"]}"
            )
            found = cur.fetchone()
            if found:
                # Remove the product id in the object
                del order_line["product_id"]
                # Add order to collection
                order_lines.append(order_line)

        return {
            "order_lines": order_lines,
            "payment_methods": payment_methods
            }

    except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
        # Rollback the transaction (if needed)
        # conn.rollback()
        logger.error(f"{connection_format(config)} Error: {e}")
        return 0

    finally:
        # Close the cursor and connection
        if "cur" in locals():
            cur.close()
        if "conn" in locals():
            conn.close()

def listen(config):
    try:
        # Set up a notification handler
        def notification_handler(notify):
            try:
                if not notify.payload:
                    return

                event_name = notify.channel

                # Parsed JSON payload
                parsed_payload = json.loads(notify.payload)

                if event_name == config["psql_channel"]:
                    logger.info(
                        f"{connection_format(config)} New event: {event_name}"
                    )
                    order_id = parsed_payload.get("id")
                    
                    sheet_payload = []
                                        
                    # Parse the date string safely, handle potential errors
                    date_order = datetime.datetime.fromisoformat(parsed_payload.get("date_order"))
                    
                    # Create order info dict
                    order_info = {
                        "datetime": date_order.strftime('%Y-%m-%d %H:%M:%S'),
                        "pos_reference": parsed_payload.get("pos_reference"),
                        "amount_total": parsed_payload.get("amount_total")
                    }
                    # Order Datails
                    order_data = get_order_info(config, order_id)
                    logger.info(f"order_data {order_data}")
                    # Payment Methods
                    payment_methods = order_data.get("payment_methods")
                    order_info.update({"payment_method": ','.join([f"{item['label']}" for item in payment_methods])})
                    # Order Lines
                    order_lines = order_data.get("order_lines")
                    # Loop each order line
                    for index, order_line in enumerate(order_lines):
                        if index == 0:
                            sheet_payload.append({**order_line, **order_info})
                        else:
                            merged_item = order_line.copy()
                            for key in order_info:
                                if key not in order_line:
                                    merged_item[key] = ""
                            sheet_payload.append(merged_item)
                    logger.info(f"sheet_payload {sheet_payload}")
                    sync_to_sheet(sheet_payload)
                    
                else:
                    logger.error(
                        f"{connection_format(config)} Ignored event with name: {event_name}"
                    )

            except Exception as e:
                logger.error(f"{connection_format(config)} Error: {str(e)}")
                traceback.print_exc()

        # Connect to the database
        conn = connect_with_retry(config)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Set up a LISTENER for loyalty points
        cur.execute(f"LISTEN {config['psql_channel']}")
        logger.info(
            f"{connection_format(config)}: Listening for {config['psql_channel']} events..."
        )

        while not exit_flag.is_set():
            conn.poll()
            while conn.notifies:
                notification = conn.notifies.pop()
                notification_handler(notification)

    except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
        # Rollback the transaction (if needed)
        # conn.rollback()
        logger.error(f"{connection_format(config)} Error: {e}")
        traceback.print_exc()

    except Exception as e:
        logger.error(f"{connection_format(config)} Error: {e}")
        traceback.print_exc()

    finally:
        # Close the cursor and connection
        if "cur" in locals():
            cur.close()
        if "conn" in locals():
            conn.close()


def connection_format(config):
    db = config.get("db", {})
    return f"""({db.get('dbname', "UnknownDB")}-{db.get('user', "UnknownUser")}@{db.get('host', "UnknownHost")}:{db.get('port',"0")})"""


def sqlcmd_create_trigger(
    dbtable,
    trigger_name,
    channel_name,
    trigger_on="INSERT",
    sql_payload="",
):
    if not sql_payload or sql_payload == "" or sql_payload == None:
        sql_payload = """row_to_json(NEW)::text"""
    sql_command = f"""
-- Create a function to send a pg_notify event
CREATE OR REPLACE FUNCTION {trigger_name}Fn()
RETURNS TRIGGER AS $$
DECLARE
	psql_channel TEXT = '{channel_name}'; -- Name of the channel for notification
BEGIN
	-- Send a pg_notify event with a custom channel and payload
	PERFORM pg_notify(psql_channel, {sql_payload});
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger that calls the function
CREATE TRIGGER {trigger_name}
AFTER {trigger_on}
ON {dbtable}
FOR EACH ROW
EXECUTE FUNCTION {trigger_name}Fn();
"""
    return sql_command


def sqlcmd_remove_trigger(dbtable, trigger_name):
    sql_command = f"""
-- To drop the trigger and the function
DROP TRIGGER {trigger_name} ON {dbtable};
DROP FUNCTION {trigger_name}Fn;
"""
    return sql_command


def add_trigger_fn(config):
    try:
        # Connect to the database
        conn = psycopg2.connect(**config["db"])
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # The command to perform for loyalty points listener
        sqlcmd_trigger = sqlcmd_create_trigger(
            config["dbtable"], config["psql_trigger"], config["psql_channel"], "INSERT"
        )
        # Execute the command
        cur.execute(f"{sqlcmd_trigger}")
        logger.info(
            f"{connection_format(config)} Success: Added trigger {config['psql_trigger']} and function {config['psql_trigger']}Fn."
        )

    except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
        # Rollback the transaction (if needed)
        # conn.rollback()
        logger.error(f"{connection_format(config)} Error: {e}")

    finally:
        # Close the cursor and connection
        if "cur" in locals():
            cur.close()
        if "conn" in locals():
            conn.close()


def remove_trigger_fn(config):
    try:
        # Connect to the database
        conn = psycopg2.connect(**config["db"])
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # The command to perform for loyalty points
        sqlcmd_unlisten_loyalty_points = sqlcmd_remove_trigger(
            config["dbtable"], config["psql_trigger"]
        )
        # Execute the command
        cur.execute(f"{sqlcmd_unlisten_loyalty_points}")
        logger.info(
            f"{connection_format(config)} Success: Removed trigger {config['psql_trigger']} and function {config['psql_trigger']}Fn."
        )

    except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
        logger.error(f"{connection_format(config)} Error: {e}")
        # Rollback the transaction (if needed)
        # conn.rollback()

    finally:
        # Close the cursor and connection
        if "cur" in locals():
            cur.close()
        if "conn" in locals():
            conn.close()


def format_config(cfg):
    config = {
        "db": {
            "dbname": cfg.get("dbname"),
            "user": cfg.get("dbuser"),
            "password": cfg.get("dbpassword"),
            "host": cfg.get("dbhost", 'localhost'),
            "port": int(cfg.get("dbport", 5432)),
        },
        "tag_id": cfg.get("tag_id", glob_tag_id),
        "psql_trigger": event_trigger,
        "psql_channel": event_trigger,
        "dbtable": dbtbl_orders_table,
        "dbtbl_order_line": dbtbl_order_line,
        "dbtbl_payment": dbtbl_payment,
        "dbtbl_payment_method": dbtbl_payment_method,
        "dbtbl_product_tagging": dbtbl_product_tagging
    }
    return config


def stop_execution(signum, frame):
    # Function to handle stopping the execution gracefully
    logger.info("-" * 40)
    logger.info("Received Ctrl+C. Shutting down gracefully...")
    exit_flag.set()


def listen_all_databases(database_configs):
    try:
        executor = concurrent.futures.ThreadPoolExecutor()

        # Register the signal handler for Ctrl+C
        signal.signal(signal.SIGINT, stop_execution)

        modified_configs = [format_config(cfg) for cfg in database_configs]
        connection_listener = {
            executor.submit(listen, cfg): cfg for cfg in modified_configs
        }

        for listener_info in concurrent.futures.as_completed(connection_listener):
            config = connection_listener[listener_info]
            try:
                # Handle the result
                result = listener_info.result()
                if result:
                    logger.info(
                        f"{connection_format(config)} Listener result: {result}"
                    )

            except Exception as e:
                # Handle exceptions raised by the task
                logger.error(f"{connection_format(config)} Listener error: {e}")
                traceback.print_exc()
                pass

    except KeyboardInterrupt:
        pass

    finally:
        if "executor" in locals():
            executor.shutdown(wait=False)


def service_install():
    try:
        # Check if the system supports .service files (systemd)
        if not os.path.exists(service_dir):
            logger.info(
                "This system does not support systemd service files (.service)."
            )
            return

        # Check if the service unit file already exists
        service_unit_path = f"{service_dir}/{service_unit_name}"
        if os.path.exists(service_unit_path):
            logger.info(f"Service unit file '{service_unit_name}' already exists.")
            return

        # Define the service unit file contents
        service_contents = f"""[Unit]
Description={service_name}

[Service]
ExecStart={sys.executable} '{os.path.abspath(__file__)}'
WorkingDirectory={os.path.dirname(os.path.abspath(__file__))}
Restart=always
User={getusername()}

[Install]
WantedBy=multi-user.target
"""

        # Write the service unit file
        with open(service_unit_path, "w") as service_file:
            service_file.write(service_contents)
        logger.info("+" * 40)
        logger.info(f"Service unit file '{service_unit_name}' created.")
        logger.info("+" * 40)
        logger.info("To start the service and enable it for auto-start on reboot, run:")
        logger.info(f"  sudo systemctl start {service_unit_name}")
        logger.info(f"  sudo systemctl enable {service_unit_name}")
        logger.info("To reload the services:")
        logger.info(f"  sudo systemctl daemon-reload")

    except Exception as e:
        logger.error(f"Service Error: {e}")
        traceback.print_exc()


def service_uninstall():
    try:
        service_unit_path = f"{service_dir}/{service_unit_name}"
        # Check if the service unit file exists
        if os.path.exists(service_unit_path):
            # Disable the service if it's currently running
            os.system(f"systemctl disable {service_unit_name}")

            # Stop the service if it's currently running
            os.system(f"systemctl stop {service_unit_name}")

            # Remove the service unit file
            os.remove(service_unit_path)

            reload_systemctl = f"systemctl daemon-reload"
            os.system(reload_systemctl)

            logger.info("-" * 40)
            logger.info(f"Service '{service_unit_name}' uninstalled.")
            logger.info("-" * 40)
        else:
            logger.error(
                f"Service '{service_unit_name}' is not installed on this system."
            )

    except Exception as e:
        logger.error(f"Service Error: {e}")
        traceback.print_exc()


def check_sudo():
    if os.geteuid() != 0:
        return False
    return True


def main():
    if args.action == "listen":
        # The default; Start listener for PSQL events
        listen_all_databases(all_databases)

    elif args.action == "install":
        # Add trigger to PSQL
        for cfg in all_databases:
            config = format_config(cfg)
            add_trigger_fn(config)

    elif args.action == "uninstall":
        # Remove the trigger from PSQL
        for cfg in all_databases:
            config = format_config(cfg)
            remove_trigger_fn(config)

    elif args.action == "service_install":
        if check_sudo():
            # Install the service
            service_install()
        else:
            logger.error(
                "Please run this script with superuser privileges (e.g., using 'sudo') to install or uninstall the service."
            )

    elif args.action == "service_uninstall":
        if check_sudo():
            # Uninstall the service
            service_uninstall()
        else:
            logger.error(
                "Please run this script with superuser privileges (e.g., using 'sudo') to install or uninstall the service."
            )

    elif args.action == "service":
        if check_sudo():
            action = (
                input(
                    "Enter 'install' to install the service or 'uninstall' to uninstall it: "
                )
                .strip()
                .lower()
            )
            if action == "install":
                service_install()
            elif action == "uninstall":
                service_uninstall()
            else:
                logger.info("Invalid action. Please enter 'install' or 'uninstall'.")
        else:
            logger.info(
                "Please run this script with superuser privileges (e.g., using 'sudo') to install or uninstall the service."
            )

    else:
        # Handle invalid action
        logger.info("Error: Invalid action specified.")


if __name__ == "__main__":
    main()
