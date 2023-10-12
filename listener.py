import argparse
import concurrent.futures
import datetime
import json
import logging
import os
import signal
import sys
import threading

import psycopg2
import psycopg2.extensions
from dotenv import load_dotenv

exit_flag = threading.Event()

# Load environment variables from the .env file
load_dotenv()

# Argument parser
parser = argparse.ArgumentParser(description="Odoo Loyalty Program - Extender")

profile_keys = [
    "first_name",
    "middle_name",
    "last_name",
    "name",
    "display_name",
    "email",
    "phone",
    "mobile",
    "title",
    "tz",
    "lang",
    "website",
    "type",
    "street",
    "street2",
    "zip",
    "city",
    "state_id",
    "country_id",
    "industry_id",
    "vat",
]

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
    "--json_server_list_src",
    type=str,
    help="A json file holding the list of servers to sync",
)
parser.add_argument(
    "--json_db_list_src",
    type=str,
    help="A json file holding the list of database configs to listen",
)
parser.add_argument(
    "--loyalty_program_id", type=int, help="Required! The loyalty program id: default=1"
)
parser.add_argument("--dbtable", type=str, help="Database table for the trigger")
parser.add_argument("--dbname", type=str, help="Database name")
parser.add_argument("--dbuser", type=str, help="Database user")
parser.add_argument("--dbpassword", type=str, help="Database user's password")
parser.add_argument("--dbhost", type=str, help="Database hostname or ip")
parser.add_argument("--dbport", type=int, help="Database port")
parser.add_argument(
    "--channel", type=str, help="PostgresSQL channel name to broadcast and listen"
)
parser.add_argument(
    "--webhook", type=str, help="Webhook url to send post request on trigger"
)
parser.add_argument(
    "--psql_trigger", type=str, help="Customized the trigger and function name"
)
parser.add_argument(
    "--dbtbl_partner", type=str, help="Table for partners; default: res_partner"
)
parser.add_argument(
    "--dbtbl_barcode", type=str, help="Table for barcode; default: ir_property"
)
parser.add_argument(
    "--psql_trigger_account",
    type=str,
    help="Customized the trigger and function name for account update",
)
parser.add_argument(
    "--psql_trigger_account_channel",
    type=str,
    help="Customized the trigger channel for account update",
)
parser.add_argument(
    "--psql_trigger_barcode",
    type=str,
    help="Customized the trigger and function name for barcode update",
)
parser.add_argument(
    "--psql_trigger_barcode_channel",
    type=str,
    help="Customized the trigger channel for barcode update",
)
parser.add_argument(
    "--service_unit_name",
    type=str,
    help="Service unit name to be created",
)
parser.add_argument(
    "--log_directory",
    type=str,
    help="Log directory storage",
)
parser.add_argument(
    "--with_console",
    type=str,
    default="yes",
    help="'yes' to Enable or 'no' to Disable console logs",
)

args = parser.parse_args()

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
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=logging_handlers,
)
logger = logging.getLogger(__name__)


#
# Service Information
#
service_name = "Odoo Service Listener by Accountador"
service_unit_name = (
    args.service_unit_name
    if args.service_unit_name is not None
    else os.environ.get("service_unit_name")
) or "accountador_listener.service"
service_dir = "/etc/systemd/system"


#
# Global Fallbacks
#
glob_dbname = args.dbname if args.dbname is not None else os.environ.get("dbname")
glob_dbuser = args.dbuser if args.dbuser is not None else os.environ.get("dbuser")
glob_dbpassword = (
    args.dbpassword if args.dbpassword is not None else os.environ.get("dbpassword")
)
glob_dbhost = args.dbhost if args.dbhost is not None else os.environ.get("dbhost")
glob_dbport = int(
    args.dbport if args.dbport is not None else os.environ.get("dbport", 0)
)

# Loyalty Program ID
glob_loyalty_program_id = (
    args.loyalty_program_id
    if args.loyalty_program_id is not None
    else os.environ.get("loyalty_program_id")
)
# PosgresSQL Notification Channel Name
glob_psql_channel = (
    args.channel if args.channel is not None else os.environ.get("channel")
)
# Name of the trigger
glob_trigger_name = (
    args.psql_trigger
    if args.psql_trigger is not None
    else os.environ.get("psql_trigger")
)
# Name of the trigger for account update
glob_trigger_account = (
    args.psql_trigger_account
    if args.psql_trigger_account is not None
    else os.environ.get("psql_trigger_account")
)
# Name of the trigger channel for account update
glob_trigger_account_channel = (
    args.psql_trigger_account_channel
    if args.psql_trigger_account_channel is not None
    else os.environ.get("psql_trigger_account_channel")
)
# Name of the trigger for barcode update
glob_trigger_barcode = (
    args.psql_trigger_barcode
    if args.psql_trigger_barcode is not None
    else os.environ.get("psql_trigger_barcode")
)
# Name of the trigger channel for account update
glob_trigger_barcode_channel = (
    args.psql_trigger_barcode_channel
    if args.psql_trigger_barcode_channel is not None
    else os.environ.get("psql_trigger_barcode_channel")
)
# Table for the trigger
glob_dbtable = args.dbtable if args.dbtable is not None else os.environ.get("dbtable")
# Webhook url
glob_webhook_url = (
    args.webhook if args.webhook is not None else os.environ.get("webhook")
)

# Tables might change
glob_dbtbl_partner = (
    args.dbtbl_partner
    if args.dbtbl_partner is not None
    else os.environ.get("dbtbl_partner")
)
glob_dbtbl_barcode = (
    args.dbtbl_barcode
    if args.dbtbl_barcode is not None
    else os.environ.get("dbtbl_barcode")
)

# JSON server List
json_server_list_src = (
    args.json_server_list_src
    if args.json_server_list_src is not None
    else os.environ.get("json_server_list_src", "servers.json")
)
try:
    if json_server_list_src:
        with open(json_server_list_src, "r") as json_file:
            all_servers = json.load(json_file)
    else:
        logger.error(
            "Error: A json file holding the list of servers to sync is required."
        )
        sys.exit(1)
except FileNotFoundError:
    logger.error(f"The file '{json_server_list_src}' was not found.")
    sys.exit(1)
except json.JSONDecodeError as e:
    logger.error(f"Error decoding JSON: {e}")
    sys.exit(1)
except Exception as e:
    logger.error(f"Error: {str(e)}")

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


# Custom JSON encoder for datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super().default(obj)


def update_loyalty_card(
    config,
    server,
    customer_barcode,
    new_customer_points,
    parsed_payload,
    customer_barcode_old="",
):
    try:
        logger.info("-" * 40)

        import xmlrpc.client

        # Odoo API
        odoo_common = xmlrpc.client.ServerProxy(
            "{}/xmlrpc/2/common".format(server["url"])
        )
        logger.info(f"{connection_format(config)} Odoo Server: {odoo_common.version()}")

        odoo_uid = odoo_common.authenticate(
            server["database"], server["user"], server["password"], {}
        )
        logger.info(f"{connection_format(config)} Odoo UID: {odoo_uid}")
        odoo_models = xmlrpc.client.ServerProxy(
            "{}/xmlrpc/2/object".format(server["url"])
        )

        query_barcode = customer_barcode
        if customer_barcode_old:
            query_barcode = customer_barcode_old

        # Search the barcode from table `ir_property` and get local customer identity
        logger.info(
            f"{connection_format(config)} Searching for customer with barcode: {query_barcode}"
        )
        tbl_customer_barcode = odoo_models.execute_kw(
            server["database"],
            odoo_uid,
            server["password"],
            "ir.property",
            "search_read",
            [[["value_text", "=", query_barcode], ["name", "=", "barcode"]]],
            {"fields": ["res_id"], "limit": 1},
        )

        if tbl_customer_barcode:
            # Update existing user
            tbl_customer_id = int(tbl_customer_barcode[0]["res_id"].split(",")[1])
            logger.info(
                f"{connection_format(config)} Local customer id found: {tbl_customer_id}"
            )

            odoo_update_fn(
                config,
                odoo_models,
                tbl_customer_id,
                odoo_uid,
                server,
                new_customer_points,
                customer_barcode,
                customer_barcode_old,
            )
        else:
            # Create a new user
            odoo_create_fn(
                config,
                odoo_models,
                odoo_uid,
                server,
                customer_barcode,
                new_customer_points,
                parsed_payload,
            )

    except Exception as e:
        logger.error(f"{connection_format(config)} Error: {str(e)}")


def profile_payload_format(profile_info):
    profile_payload = {}
    for key in profile_keys:
        profile_payload[key] = profile_info[key] or ""
    return profile_payload


def profile_payload_compare(old_profile, new_profile):
    has_changed = False
    for key in profile_keys:
        if old_profile[key] != new_profile[key]:
            has_changed = True
            break
    return has_changed


def odoo_update_customer_fn(config, server, customer_barcode, parsed_payload):
    try:
        logger.info("-" * 40)

        import xmlrpc.client

        # Odoo API
        odoo_common = xmlrpc.client.ServerProxy(
            "{}/xmlrpc/2/common".format(server["url"])
        )
        logger.info(f"{connection_format(config)} Odoo Server: {odoo_common.version()}")

        odoo_uid = odoo_common.authenticate(
            server["database"], server["user"], server["password"], {}
        )
        logger.info(f"{connection_format(config)} Odoo UID: {odoo_uid}")
        odoo_models = xmlrpc.client.ServerProxy(
            "{}/xmlrpc/2/object".format(server["url"])
        )

        # Search the barcode from table `ir_property` and get local customer identity
        logger.info(
            f"{connection_format(config)} Searching for customer with barcode: {customer_barcode}"
        )

        tbl_customer_barcode = odoo_models.execute_kw(
            server["database"],
            odoo_uid,
            server["password"],
            "ir.property",
            "search_read",
            [[["value_text", "=", customer_barcode], ["name", "=", "barcode"]]],
            {"fields": ["res_id"], "limit": 1},
        )

        if tbl_customer_barcode:
            # Update existing user
            tbl_customer_id = int(tbl_customer_barcode[0]["res_id"].split(",")[1])
            logger.info(
                f"{connection_format(config)} Local customer id found: {tbl_customer_id}"
            )

            # Update res_partner
            profile_payload = profile_payload_format(parsed_payload["new"])
            odoo_models.execute_kw(
                server["database"],
                odoo_uid,
                server["password"],
                "res.partner",
                "write",
                [[tbl_customer_id], profile_payload],
            )
            logger.info(
                f"{connection_format(config)} Updated customer's profile - {tbl_customer_id}"
            )
        else:
            customer_points = (
                parsed_payload["profile"]["loyalty_card"].get("points") or 0
            )
            # Create customers profile; res_partner
            odoo_create_fn(
                config,
                odoo_models,
                odoo_uid,
                server,
                customer_barcode,
                customer_points,
                parsed_payload,
            )
    except Exception as e:
        logger.error(
            f"{connection_format(config)} Create or Update customer error: {e}"
        )


def odoo_create_fn(
    config,
    odoo_models,
    odoo_uid,
    server,
    customer_barcode,
    new_customer_points,
    parsed_payload,
):
    logger.info("-" * 40)
    # Create res_partner
    profile_payload = profile_payload_format(parsed_payload["profile"]["partner"])
    logger.info(f"{connection_format(config)} Creating NEW customer. {profile_payload}")
    tbl_customer_id = odoo_models.execute_kw(
        server["database"],
        odoo_uid,
        server["password"],
        "res.partner",
        "create",
        [profile_payload],
    )
    logger.info(
        f"{connection_format(config)} Created customer's profile - {tbl_customer_id}"
    )

    # Create ir_property
    barcode = parsed_payload["profile"]["barcode"]
    profile_barcode = {
        "res_id": "res.partner," + str(tbl_customer_id),
        "value_text": customer_barcode or "",
        "fields_id": barcode.get("fields_id") or "",
        "name": barcode.get("name") or "",
        "type": barcode.get("type") or "",
    }
    odoo_models.execute_kw(
        server["database"],
        odoo_uid,
        server["password"],
        "ir.property",
        "create",
        [profile_barcode],
    )
    logger.info(
        f"{connection_format(config)} Created customer's barcode details - ({profile_barcode})"
    )

    # Create loyalty_card
    profile_loyalty = {
        "partner_id": tbl_customer_id,
        "points": new_customer_points,
        "program_id": config["loyalty_program_id"],
    }
    odoo_models.execute_kw(
        server["database"],
        odoo_uid,
        server["password"],
        "loyalty.card",
        "create",
        [profile_loyalty],
    )
    logger.info(
        f"{connection_format(config)} Created customer's loyalty details - ({profile_loyalty})"
    )


def odoo_update_fn(
    config,
    odoo_models,
    tbl_customer_id,
    odoo_uid,
    server,
    new_customer_points,
    customer_barcode,
    customer_barcode_old,
):
    logger.info("-" * 40)
    logger.info(f"{connection_format(config)} Updating existing customer.")
    # Search for the id in loyalty card table
    tbl_loyalty_card_ids = odoo_models.execute_kw(
        server["database"],
        odoo_uid,
        server["password"],
        "loyalty.card",
        "search",
        [[["partner_id", "=", tbl_customer_id]]],
        {"limit": 1},
    )
    logger.info(
        f"{connection_format(config)} Local customer's loyalty card id found: {tbl_loyalty_card_ids}"
    )

    # Update loyalty points
    loyalty_card_payload = {"points": new_customer_points}
    odoo_models.execute_kw(
        server["database"],
        odoo_uid,
        server["password"],
        "loyalty.card",
        "write",
        [tbl_loyalty_card_ids, loyalty_card_payload],
    )
    logger.info(
        f"{connection_format(config)} Local customer's loyalty points updated: {new_customer_points}"
    )

    if customer_barcode_old and customer_barcode != customer_barcode_old:
        logger.info(
            f"{connection_format(config)} Got a new barcode: {customer_barcode_old} to {customer_barcode}"
        )
        # Update customer barcode
        odoo_models.execute_kw(
            server["database"],
            odoo_uid,
            server["password"],
            "ir.property",
            "write",
            [tbl_loyalty_card_ids, {"value_text": customer_barcode}],
        )
        logger.info(
            f"{connection_format(config)} Local customer's barcode updated: {new_customer_points}"
        )


def get_customer_profile(config, user_id):
    try:
        # Connect to the database
        conn = psycopg2.connect(**config["db"])
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        profile = {
            "barcode": {},
            "partner": {},
            "loyalty_card": {},
        }

        # Unique customer from barcode table
        cur.execute(
            f"SELECT * FROM {config['dbtbl_barcode']} WHERE res_id='res.partner,{user_id}' AND name = 'barcode'"
        )
        barcode_data = cur.fetchone()
        if barcode_data:
            column_names = [desc[0] for desc in cur.description]
            json_result = dict(zip(column_names, barcode_data))
            profile["barcode"] = json_result

        # Profile from partner table
        cur.execute(f"SELECT * FROM {config['dbtbl_partner']} WHERE id={user_id}")
        partner_data = cur.fetchone()
        if partner_data:
            column_names = [desc[0] for desc in cur.description]
            json_result = dict(zip(column_names, partner_data))
            profile["partner"] = json_result

        # Profile from loyalty_card table
        cur.execute(f"SELECT * FROM {config['dbtable']} WHERE partner_id={user_id}")
        loyalty_card = cur.fetchone()
        if loyalty_card:
            column_names = [desc[0] for desc in cur.description]
            json_result = dict(zip(column_names, loyalty_card))
            profile["loyalty_card"] = json_result

        return profile

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
                event_name = notify.channel

                if event_name == config["psql_channel"]:
                    # Parsed JSON payload
                    parsed_payload = json.loads(notify.payload)
                    parsed_payload["profile"] = get_customer_profile(
                        config, parsed_payload["new"].get("partner_id")
                    )

                    customer_barcode = (
                        parsed_payload["profile"].get("barcode", {}).get("value_text")
                    )
                    customer_points_old = parsed_payload["old"].get("points")
                    customer_points_new = parsed_payload["new"].get("points")

                    # Check if the event made changes on the loyalty points
                    if customer_points_new != customer_points_old:
                        # logger.info(f"Payload: {parsed_payload}")
                        logger.info("+" * 40)
                        logger.info(
                            f"New Loyalty Points Event {connection_format(config)}"
                        )
                        logger.info("+" * 40)
                        logger.info(
                            f"{connection_format(config)} Points have been updated from {customer_points_old} to {customer_points_new}."
                        )
                        # Update all Odoo servers using odoo API
                        for server in all_servers:
                            logger.info(
                                f"{connection_format(config)} Connecting to server: {server['name']} - ({server['url']})"
                            )
                            update_loyalty_card(
                                config,
                                server,
                                customer_barcode,
                                customer_points_new,
                                parsed_payload,
                            )

                        # Send a POST request to the webhook with the event payload
                        if config["webhook_url"]:
                            logger.info("-" * 40)
                            import requests

                            headers = {"Content-Type": "application/json"}
                            response = requests.post(
                                config["webhook_url"],
                                data=json.dumps(parsed_payload, cls=DateTimeEncoder),
                                headers=headers,
                            )

                            if response.status_code == 200:
                                logger.info(
                                    f"{connection_format(config)} POST request to webhook sent successfully. - {config['webhook_url']}"
                                )
                            else:
                                logger.info(
                                    f"{connection_format(config)} Failed to send POST request to webhook. Status code: {response.status_code}"
                                )

                elif event_name == config["psql_trigger_account_channel"]:
                    # Parsed JSON payload
                    parsed_payload = json.loads(notify.payload)
                    if profile_payload_compare(
                        parsed_payload["old"], parsed_payload["new"]
                    ):
                        parsed_payload["profile"] = get_customer_profile(
                            config, parsed_payload["new"]["id"]
                        )

                        customer_barcode = parsed_payload["profile"]["barcode"][
                            "value_text"
                        ]
                        if customer_barcode:
                            # logger.info(f"Payload: {parsed_payload}")
                            logger.info("-" * 40)
                            logger.info(
                                f"{connection_format(config)} Event: Create or Update Account"
                            )
                            # Update all Odoo servers using odoo API
                            for server in all_servers:
                                logger.info(
                                    f"{connection_format(config)} Connecting to server: {server['name']} - ({server['url']})"
                                )
                                odoo_update_customer_fn(
                                    config, server, customer_barcode, parsed_payload
                                )
                        else:
                            logger.info(
                                f"{connection_format(config)} Event: Account Update has no Barcode"
                            )

                elif event_name == config["psql_trigger_barcode_channel"]:
                    # Parsed JSON payload
                    parsed_payload = json.loads(notify.payload)
                    barcode_old = parsed_payload["old"]["value_text"]
                    barcode_new = parsed_payload["new"]["value_text"]
                    if barcode_old == barcode_new:
                        logger.info(
                            f"{connection_format(config)} Barcode Changes: from {barcode_old} to {barcode_new}"
                        )
                        # Update all Odoo servers using odoo API
                        for server in all_servers:
                            logger.info(
                                f"{connection_format(config)} Connecting to server: {server['name']} - ({server['url']})"
                            )
                            # Update remote branch customer with old barcode to new barcode
                            update_loyalty_card(
                                config,
                                server,
                                barcode_new,
                                customer_points_new,
                                parsed_payload,
                                barcode_old,
                            )

                else:
                    logger.error(
                        f"{connection_format(config)} Ignored event with name: {event_name}"
                    )

            except Exception as e:
                logger.error(f"{connection_format(config)} Error: {str(e)}")

        # Connect to the database
        conn = psycopg2.connect(**config["db"])
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Set up a LISTENER for loyalty points
        cur.execute(f"LISTEN {config['psql_channel']}")
        logger.info(
            f"{connection_format(config)} Transaction Points: Listening for {config['psql_channel']} events..."
        )

        # Set up a LISTENER for profile update
        cur.execute(f"LISTEN {config['psql_trigger_account_channel']}")
        logger.info(
            f"{connection_format(config)} Account: Listening for {config['psql_trigger_account_channel']} events..."
        )

        # Set up a LISTENER for barcode update
        cur.execute(f"LISTEN {config['psql_trigger_barcode_channel']}")
        logger.info(
            f"{connection_format(config)} Barcode: Listening for {config['psql_trigger_barcode_channel']} events..."
        )

        if config["webhook_url"]:
            logger.info(f"{connection_format(config)} Webhook: {config['webhook_url']}")

        while not exit_flag.is_set():
            conn.poll()
            while conn.notifies:
                notification = conn.notifies.pop()
                notification_handler(notification)

    except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
        # Rollback the transaction (if needed)
        # conn.rollback()
        logger.error(f"{connection_format(config)} Error: {e}")

    except Exception as e:
        logger.error(f"{connection_format(config)} Error: {e}")

    finally:
        # Close the cursor and connection
        if "cur" in locals():
            cur.close()
        if "conn" in locals():
            conn.close()


def connection_format(config):
    return f"""({config['db']['dbname']}-{config['db']['user']}@{config['db']['host']}:{config['db']['port']})"""


def sqlcmd_create_trigger(
    dbtable,
    trigger_name,
    channel_name,
    trigger_on="INSERT OR UPDATE",
    sql_payload="""'{"old": ' || row_to_json(OLD) || ', "new": ' || row_to_json(NEW) || '}'""",
):
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
        sqlcmd_listen_loyalty_points = sqlcmd_create_trigger(
            config["dbtable"], config["psql_trigger"], config["psql_channel"]
        )
        # Execute the command
        cur.execute(f"{sqlcmd_listen_loyalty_points}")
        logger.info(
            f"{connection_format(config)} Success: Added trigger {config['psql_trigger']} and function {config['psql_trigger']}Fn."
        )

        # The command to perform for account update listener
        sqlcmd_listen_profile_update = sqlcmd_create_trigger(
            config["dbtbl_partner"],
            config["psql_trigger_account"],
            config["psql_trigger_account_channel"],
            "UPDATE",
        )
        # Execute the command
        cur.execute(f"{sqlcmd_listen_profile_update}")
        logger.info(
            f"{connection_format(config)} Success: Added trigger {config['psql_trigger_account']} and function {config['psql_trigger_account']}Fn."
        )

        # The command to perform for barcode update
        sqlcmd_listen_barcode_update = sqlcmd_create_trigger(
            config["dbtbl_barcode"],
            config["psql_trigger_barcode"],
            config["psql_trigger_barcode_channel"],
            "UPDATE",
        )
        # Execute the command
        cur.execute(f"{sqlcmd_listen_barcode_update}")
        logger.info(
            f"{connection_format(config)} Success: Added trigger {config['psql_trigger_barcode']} and function {config['psql_trigger_barcode']}Fn."
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

        # The command to perform profile update
        sqlcmd_unlisten_profile_update = sqlcmd_remove_trigger(
            config["dbtbl_partner"], config["psql_trigger_account"]
        )
        # Execute the command
        cur.execute(f"{sqlcmd_unlisten_profile_update}")
        logger.info(
            f"{connection_format(config)} Success: Removed trigger {config['psql_trigger_account']} and function {config['psql_trigger_account']}Fn."
        )

        # The command to perform barcode update
        sqlcmd_unlisten_barcode_update = sqlcmd_remove_trigger(
            config["dbtbl_barcode"], config["psql_trigger_barcode"]
        )
        # Execute the command
        cur.execute(f"{sqlcmd_unlisten_barcode_update}")
        logger.info(
            f"{connection_format(config)} Success: Removed trigger {config['psql_trigger_barcode']} and function {config['psql_trigger_barcode']}Fn."
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
        "loyalty_program_id": int(
            cfg.get("loyalty_program_id", glob_loyalty_program_id or 1)
        ),
        "db": {
            "dbname": cfg.get("dbname", glob_dbname),
            "user": cfg.get("dbuser", glob_dbuser),
            "password": cfg.get("dbpassword", glob_dbpassword),
            "host": cfg.get("dbhost", glob_dbhost) or "localhost",
            "port": int(cfg.get("dbport", glob_dbport) or 5432),
        },
        "dbtable": cfg.get("dbtable", glob_dbtable) or "loyalty_card",
        "webhook_url": cfg.get("webhook", glob_webhook_url),
        "psql_channel": cfg.get("channel", glob_psql_channel)
        or "accountador_loyalty_card_update",
        "psql_trigger": cfg.get("psql_trigger", glob_trigger_name)
        or "accountador_loyalty_card_update",
        "dbtbl_partner": cfg.get("dbtbl_partner", glob_dbtbl_partner) or "res_partner",
        "dbtbl_barcode": cfg.get("dbtbl_barcode", glob_dbtbl_barcode) or "ir_property",
        "psql_trigger_account": cfg.get("psql_trigger_account", glob_trigger_account)
        or "accountador_account_update",
        "psql_trigger_account_channel": cfg.get(
            "psql_trigger_account_channel", glob_trigger_account_channel
        )
        or "accountador_account_update",
        "psql_trigger_barcode": cfg.get("psql_trigger_barcode", glob_trigger_barcode)
        or "accountador_barcode_update",
        "psql_trigger_barcode_channel": cfg.get(
            "psql_trigger_barcode_channel", glob_trigger_barcode_channel
        )
        or "accountador_barcode_update",
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
