import argparse
import concurrent.futures
import json
import os
import signal
import sys
import threading
from datetime import datetime

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
    choices=["listen", "install", "uninstall"],
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

args = parser.parse_args()

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
        print("Error: A json file holding the list of servers to sync is required.")
        sys.exit(1)
except FileNotFoundError:
    print(f"The file '{json_server_list_src}' was not found.")
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"Error decoding JSON: {e}")
    sys.exit(1)
except Exception as e:
    print(f"Error: {str(e)}")

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
        print("Error: A json file holding the list of databases to listen is required.")
        sys.exit(1)
except FileNotFoundError:
    print(f"The file '{json_db_list_src}' was not found.")
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"Error decoding JSON: {e}")
    sys.exit(1)
except Exception as e:
    print(f"Error: {str(e)}")


# Custom JSON encoder for datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def update_loyalty_points(
    config, server, customer_barcode, new_customer_points, parsed_payload
):
    try:
        print("-" * 40)

        import xmlrpc.client

        # Odoo API
        odoo_common = xmlrpc.client.ServerProxy(
            "{}/xmlrpc/2/common".format(server["url"])
        )
        print(
            f"({connection_format(config)}) {server['name']}: Odoo Server: {odoo_common.version()}"
        )

        odoo_uid = odoo_common.authenticate(
            server["database"], server["user"], server["password"], {}
        )
        print(f"({connection_format(config)}) {server['name']}: Odoo UID: {odoo_uid}")
        odoo_models = xmlrpc.client.ServerProxy(
            "{}/xmlrpc/2/object".format(server["url"])
        )

        # Search the barcode from table `ir_property` and get local customer identity
        print(
            f"({connection_format(config)}) {server['name']}: Searching for customer with barcode: {customer_barcode}"
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
            print(
                f"({connection_format(config)}) {server['name']}: Local customer id found: {tbl_customer_id}"
            )

            odoo_update_fn(
                config,
                odoo_models,
                tbl_customer_id,
                odoo_uid,
                server,
                new_customer_points,
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
        print(f"({connection_format(config)}) {server['name']}: Error: {str(e)}")


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
    print("-" * 40)

    import xmlrpc.client

    # Odoo API
    odoo_common = xmlrpc.client.ServerProxy("{}/xmlrpc/2/common".format(server["url"]))
    print(
        f"({connection_format(config)}) {server['name']}: Odoo Server: {odoo_common.version()}"
    )

    odoo_uid = odoo_common.authenticate(
        server["database"], server["user"], server["password"], {}
    )
    print(f"({connection_format(config)}) {server['name']}: Odoo UID: {odoo_uid}")
    odoo_models = xmlrpc.client.ServerProxy("{}/xmlrpc/2/object".format(server["url"]))

    # Search the barcode from table `ir_property` and get local customer identity
    print(
        f"({connection_format(config)}) {server['name']}: Searching for customer with barcode: {customer_barcode}"
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
        print(
            f"({connection_format(config)}) {server['name']}: Local customer id found: {tbl_customer_id}"
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
        print(
            f"({connection_format(config)}) {server['name']}: Updated customer's profile - {tbl_customer_id}"
        )
    else:
        customer_points = parsed_payload["new"]["loyalty_card"]["points"]
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


def odoo_create_fn(
    config,
    odoo_models,
    odoo_uid,
    server,
    customer_barcode,
    new_customer_points,
    parsed_payload,
):
    print("-" * 40)
    # Create res_partner
    profile_payload = profile_payload_format(parsed_payload["profile"]["partner"])
    print(
        f"({connection_format(config)}) {server['name']}: Creating NEW customer. {profile_payload}"
    )
    tbl_customer_id = odoo_models.execute_kw(
        server["database"],
        odoo_uid,
        server["password"],
        "res.partner",
        "create",
        [profile_payload],
    )
    print(
        f"({connection_format(config)}) {server['name']}: Created customer's profile - {tbl_customer_id}"
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
    print(
        f"({connection_format(config)}) {server['name']}: Created customer's barcode details - ({profile_barcode})"
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
    print(
        f"({connection_format(config)}) {server['name']}: Created customer's loyalty details - ({profile_loyalty})"
    )


def odoo_update_fn(
    config,
    odoo_models,
    tbl_customer_id,
    odoo_uid,
    server,
    new_customer_points,
):
    print("-" * 40)
    print(
        f"({connection_format(config)}) {server['name']}: Updating existing customer."
    )
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
    print(
        f"({connection_format(config)}) {server['name']}: Local customer's loyalty card id found: {tbl_loyalty_card_ids}"
    )

    # Update loyalty points
    odoo_models.execute_kw(
        server["database"],
        odoo_uid,
        server["password"],
        "loyalty.card",
        "write",
        [tbl_loyalty_card_ids, {"points": new_customer_points}],
    )
    print(
        f"({connection_format(config)}) {server['name']}: Local customer's loyalty point updated: {new_customer_points}"
    )


def get_customer_profile(config, user_id):
    try:
        # Connect to the database
        conn = psycopg2.connect(**config["db"])
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Unique customer from barcode table
        cur.execute(
            f"SELECT * FROM {config['dbtbl_barcode']} WHERE res_id='res.partner,{user_id}' AND name = 'barcode'"
        )
        barcode_data = cur.fetchone()
        if barcode_data:
            column_names = [desc[0] for desc in cur.description]
            json_result = dict(zip(column_names, barcode_data))
            profile = {"barcode": json_result}
        else:
            profile = {}

        # Profile from partner table
        cur.execute(f"SELECT * FROM {config['dbtbl_partner']} WHERE id={user_id}")
        partner_data = cur.fetchone()
        if partner_data:
            column_names = [desc[0] for desc in cur.description]
            json_result = dict(zip(column_names, partner_data))
            profile["partner"] = json_result

        # Profile from partner table
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
        print(f"({connection_format(config)}) Error: {e}")
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
                        config, parsed_payload["new"]["partner_id"]
                    )

                    customer_barcode = parsed_payload["profile"]["barcode"][
                        "value_text"
                    ]
                    customer_points_old = parsed_payload["old"]["points"]
                    customer_points_new = parsed_payload["new"]["points"]

                    # Check if the event made changes on the loyalty points
                    if customer_points_new != customer_points_old:
                        # print(f"Payload: {parsed_payload}")
                        print("-" * 40)
                        print(f"New Event {connection_format(config)}")
                        print(
                            f"({connection_format(config)}) Points have been updated from {customer_points_old} to {customer_points_new}."
                        )
                        # Update all Odoo servers using odoo API
                        for server in all_servers:
                            print(
                                f"({connection_format(config)}) Connecting to server: {server['name']} - ({server['url']})"
                            )
                            update_loyalty_points(
                                config,
                                server,
                                customer_barcode,
                                customer_points_new,
                                parsed_payload,
                            )

                        # Send a POST request to the webhook with the event payload
                        if config["webhook_url"]:
                            print("-" * 40)
                            import requests

                            headers = {"Content-Type": "application/json"}
                            response = requests.post(
                                config["webhook_url"],
                                data=json.dumps(parsed_payload, cls=DateTimeEncoder),
                                headers=headers,
                            )

                            if response.status_code == 200:
                                print(
                                    f"({connection_format(config)}) POST request to webhook sent successfully. - {config['webhook_url']}"
                                )
                            else:
                                print(
                                    f"({connection_format(config)}) Failed to send POST request to webhook. Status code: {response.status_code}"
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
                            # print(f"Payload: {parsed_payload}")
                            print("-" * 40)
                            print(
                                f"({connection_format(config)}) Event: Account Update"
                            )
                            # Update all Odoo servers using odoo API
                            for server in all_servers:
                                print(
                                    f"({connection_format(config)}) Connecting to server: {server['name']} - ({server['url']})"
                                )
                                odoo_update_customer_fn(
                                    config, server, customer_barcode, parsed_payload
                                )
                        else:
                            print(
                                f"({connection_format(config)}) Event: Account Update has no Barcode"
                            )

                else:
                    print(
                        f"({connection_format(config)}) Ignored event with name: {event_name}"
                    )

            except Exception as e:
                print(f"({connection_format(config)} Error: {str(e)}")

        # Connect to the database
        conn = psycopg2.connect(**config["db"])
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Set up a LISTENER for loyalty points
        cur.execute(f"LISTEN {config['psql_channel']}")
        print(
            f"({connection_format(config)}) Listening for {config['psql_channel']} events..."
        )

        # Set up a LISTENER for profile update
        cur.execute(f"LISTEN {config['psql_trigger_account_channel']}")
        print(
            f"({connection_format(config)}) Listening for {config['psql_trigger_account_channel']} events..."
        )

        if config["webhook_url"]:
            print(f"({connection_format(config)}) Webhook: {config['webhook_url']}")

        while not exit_flag.is_set():
            conn.poll()
            while conn.notifies:
                notification = conn.notifies.pop()
                notification_handler(notification)

    except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
        # Rollback the transaction (if needed)
        # conn.rollback()
        print(f"({connection_format(config)}) Error: {e}")

    except Exception as e:
        print(f"({connection_format(config)}) Error: {e}")

    finally:
        # Close the cursor and connection
        if "cur" in locals():
            cur.close()
        if "conn" in locals():
            conn.close()


def connection_format(config):
    return f"""{config['db']['dbname']}-{config['db']['user']}@{config['db']['host']}:{config['db']['port']}"""


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
        print(
            f"({connection_format(config)}) Success: Added trigger {config['psql_trigger']} and function {config['psql_trigger']}Fn."
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
        print(
            f"({connection_format(config)}) Success: Added trigger {config['psql_trigger_account']} and function {config['psql_trigger_account']}Fn."
        )

    except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
        # Rollback the transaction (if needed)
        # conn.rollback()
        print(f"({connection_format(config)}) Error: {e}")

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
        print(
            f"({connection_format(config)}) Success: Removed trigger {config['psql_trigger']} and function {config['psql_trigger']}Fn."
        )

        # The command to perform profile update
        sqlcmd_unlisten_profile_update = sqlcmd_remove_trigger(
            config["dbtbl_partner"], config["psql_trigger_account"]
        )
        # Execute the command
        cur.execute(f"{sqlcmd_unlisten_profile_update}")
        print(
            f"({connection_format(config)}) Success: Removed trigger {config['psql_trigger_account']} and function {config['psql_trigger_account']}Fn."
        )

    except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
        print(f"({connection_format(config)}) Error: {e}")
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
    }
    return config


def stop_execution(signum, frame):
    # Function to handle stopping the execution gracefully
    print("-" * 40)
    print("Received Ctrl+C. Shutting down gracefully...")
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
                    print(f"({connection_format(config)}) Listener result: {result}")

            except Exception as e:
                # Handle exceptions raised by the task
                print(f"({connection_format(config)}) Listener error: {e}")
                pass

    except KeyboardInterrupt:
        pass

    finally:
        if "executor" in locals():
            executor.shutdown(wait=False)


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

    else:
        # Handle invalid action
        print("Error: Invalid action specified.")


if __name__ == "__main__":
    main()
