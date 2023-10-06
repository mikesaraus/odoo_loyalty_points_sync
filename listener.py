import os
import sys
import json
import argparse
import xmlrpc.client
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import psycopg2.extensions
import requests

# Load environment variables from the .env file
load_dotenv()

parser = argparse.ArgumentParser(description="Odoo Loyalty Program - Extender")

# Add command-line arguments
parser.add_argument("action", choices=["listen", "addtrigger", "removetrigger"], help="Specify the action to execute")
parser.add_argument("--webhook", type=str, help="Webhook url to send post request on trigger")
parser.add_argument("--channel", type=str, help="PostgresSQL channel name to broadcast and listen")
parser.add_argument("--dbtable", type=str, help="Database table for the trigger")
parser.add_argument("--dbname", type=str, help="Database name")
parser.add_argument("--dbuser", type=str, help="Database user")
parser.add_argument("--dbpassword", type=str, help="Database user's password")
parser.add_argument("--dbhost", type=str, help="Database hostname or ip")
parser.add_argument("--dbport", type=int, help="Database port")
parser.add_argument("--psql_trigger", type=str, help="Customized the trigger and function name")
parser.add_argument("--dbtbl_partner", type=str, help="Table for partners; default: res_partner")
parser.add_argument("--dbtbl_barcode", type=str, help="Table for barcode; default: ir_property")
parser.add_argument("--json_server_list_src", type=str, help="A json file holding the list of servers to sync")

args = parser.parse_args()

# JSON server List
json_server_list_src = args.json_server_list_src if args.json_server_list_src is not None else os.environ.get('json_server_list_src', 'servers.json')
try:
    if json_server_list_src:
        with open(json_server_list_src, 'r') as json_file:
            all_server = json.load(json_file)
    else:
        print("Error: A json file holding the list of servers to sync is required.")
        sys.exit(1)
except FileNotFoundError:
    print(f"The file '{json_server_list_src}' was not found.")
    sys.exit(1) 
except json.JSONDecodeError as e:
    print(f"Error decoding JSON: {e}")
    sys.exit(1)


# Webhook url
webhook_url = args.webhook if args.webhook is not None else os.environ.get('webhook')
# PosgresSQL Notification Channel Name
psql_channel = args.channel if args.channel is not None else os.environ.get('channel', 'loyalty_card_update')
# Table for the trigger
dbtable = args.dbtable if args.dbtable is not None else os.environ.get('dbtable', 'loyalty_card')
# Name of the trigger
trigger_name = args.psql_trigger if args.psql_trigger is not None else os.environ.get('psql_trigger', 'odooLoyaltyUpdate')

# Database credentials
dbname = args.dbname if args.dbname is not None else os.environ.get('dbname')
dbuser = args.dbuser if args.dbuser is not None else os.environ.get('dbuser')
dbpassword = args.dbpassword if args.dbpassword is not None else os.environ.get('dbpassword')
dbhost = args.dbhost if args.dbhost is not None else os.environ.get('dbhost', 'localhost')
dbport = int(args.dbport if args.dbport is not None else os.environ.get('dbport', 5432))

# Tables might change
dbtbl_partner = args.dbtbl_partner if args.dbtbl_partner is not None else os.environ.get('dbtbl_partner', 'res_partner')
dbtbl_barcode = args.dbtbl_barcode if args.dbtbl_barcode is not None else os.environ.get('dbtbl_barcode', 'ir_property')

try:
    db_connection = psycopg2.connect(
        dbname = dbname,
        user = dbuser,
        password = dbpassword,
        host = dbhost,
        port = dbport
    )
except Exception as e:
    print(f"Error: {str(e)}")


# Custom JSON encoder for datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)
    
def updateLoyaltyPoints(server_name, odoo, db, customer_barcode, new_customer_points, parsed_payload):
    try:
        # Odoo API
        odoo_common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(odoo["url"]))
        print(f"{server_name}: Odoo Server: {odoo_common.version()}")

        odoo_uid = odoo_common.authenticate(db["name"], odoo["user"], odoo["password"], {})
        print(f"{server_name}: Odoo UID: {odoo_uid}")
        odoo_models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(odoo["url"]))

        # Search the barcode from table `ir_property` and get local customer identity
        print(f"{server_name}: Searching for customer with barcode: {customer_barcode}")
        tbl_customer_barcode = odoo_models.execute_kw(db["name"], odoo_uid, odoo["password"], 'ir.property', 'search_read', [[['value_text', '=', customer_barcode],['name', '=', 'barcode']]], {'fields': ['res_id'], 'limit': 1})
        
        if tbl_customer_barcode:
            # Update existing user
            tbl_customer_id = int(tbl_customer_barcode[0]["res_id"].split(',')[1])
            print(f"{server_name}: Local customer id found: {tbl_customer_id}")
            
            odoo_updateFn(odoo_models, tbl_customer_id, odoo_uid, server_name, odoo, db, customer_barcode, new_customer_points)
        else:
            # Create a new user
            odoo_createFn(odoo_models, odoo_uid, server_name, odoo, db, customer_barcode, new_customer_points, parsed_payload)

    except Exception as e:
        print(f"Error: {str(e)}")
        
def odoo_createFn(odoo_models, odoo_uid, server_name, odoo, db, customer_barcode, new_customer_points, parsed_payload):
    # Create res_partner
    profile_payload = parsed_payload["profile"]["partner"]
    del profile_payload["id"]
    tbl_customer_id = odoo_models.execute_kw(db["name"], odoo_uid, odoo["password"], 'res.partner', 'create', [profile_payload])

    # Create ir_property
    profile_barcode = {
        "res_id": tbl_customer_id,
        "name": parsed_payload["profile"]["barcode"]["name"],
        "type": parsed_payload["profile"]["barcode"]["type"],
        "value_text":parsed_payload["profile"]["barcode"]["value_text"] 
    }
    odoo_models.execute_kw(db["name"], odoo_uid, odoo["password"], 'ir.property', 'create', [profile_barcode])

    # Create loyalty_card
    profile_loyalty = {
        "partner_id": tbl_customer_id,
        "points": parsed_payload["new"]["points"]
    }
    odoo_models.execute_kw(db["name"], odoo_uid, odoo["password"], 'loyalty_card', 'create', [profile_loyalty])
    
    odoo_updateFn(odoo_models, tbl_customer_id, odoo_uid, server_name, odoo, db, customer_barcode, new_customer_points)

def odoo_updateFn(odoo_models, tbl_customer_id, odoo_uid, server_name, odoo, db, customer_barcode, new_customer_points):
    # Search for the id in loyalty card table
    tbl_loyalty_card_ids = odoo_models.execute_kw(db["name"], odoo_uid, odoo["password"], 'loyalty.card', 'search', [[['partner_id', '=', tbl_customer_id]]], {'limit': 1})
    print(f"{server_name}: Local customer's loyalty card id found: {tbl_loyalty_card_ids}")
    
    # Update loyalty points
    odoo_models.execute_kw(db["name"], odoo_uid, odoo["password"], 'loyalty.card', 'write', [tbl_loyalty_card_ids, {'points': new_customer_points}])
    print(f"{server_name}: Local customer's loyalty point updated: {new_customer_points}")

def getCustomerProfile(id):
    try:
        local_db_connection = psycopg2.connect(
            dbname=dbname,
            user=dbuser,
            password=dbpassword,
            host=dbhost,
            port=dbport
        )
        # Connect to the database
        conn = local_db_connection
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Unique customer from barcode table
        cur.execute(f"SELECT * FROM {dbtbl_barcode} WHERE res_id='res.partner,{id}' AND name = 'barcode'")
        barcode_data = cur.fetchone()
        if barcode_data:
            column_names = [desc[0] for desc in cur.description]
            json_result = dict(zip(column_names, barcode_data))
            profile = {
                "barcode": json_result
            }
        else:
            profile = {}
        # Profile from partner table
        cur.execute(f"SELECT * FROM {dbtbl_partner} WHERE id={id}")
        partner_data = cur.fetchone()

        if partner_data:
            column_names = [desc[0] for desc in cur.description]
            json_result = dict(zip(column_names, partner_data))
            profile["partner"] = json_result
        return profile

    except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
        print(f"Error: {e}")
        return 0
        # Rollback the transaction (if needed)
        # conn.rollback()

    finally:
        # Close the cursor and connection
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


def listen():
    try:
        # Set up a notification handler
        def notification_handler(notify):
            try:
                event_name = notify.channel

                if event_name == psql_channel:
                    # Parsed json payload
                    parsed_payload = json.loads(notify.payload)
                    profile = getCustomerProfile(parsed_payload["new"]["partner_id"])
                    parsed_payload["profile"] = profile
                    
                    string_payload = json.dumps(parsed_payload, cls=DateTimeEncoder)

                    customer_barcode = parsed_payload["profile"]["barcode"]["value_text"]
                    customer_points_old = parsed_payload["old"]["points"]
                    customer_points_new = parsed_payload["new"]["points"]
                    
                    # Check if the event made changes on the loyalty points
                    if customer_points_new != customer_points_old:
                        print(f"Payload: {parsed_payload}")
                        print(f"Points have been updated from {customer_points_old} to {customer_points_new}.")
                        # Update all Odoo servers using odoo API
                        for server in all_server:
                            print(f"Connecting to server: {server['name']} - ({server['url']})")
                            odoo = { 
                                "url": server["url"],
                                "user": server["user"],
                                "password": server["password"]
                            }
                            db = { 
                                "name": server["database"],
                            }
                            updateLoyaltyPoints(server["name"], odoo, db, customer_barcode, customer_points_new, parsed_payload)
                            
                        # Send a POST request to the webhook with the event payload
                        if webhook_url:
                            headers = {"Content-Type": "application/json"}
                            response = requests.post(webhook_url, data=string_payload, headers=headers)
                            
                            if response.status_code == 200:
                                print("POST request to webhook sent successfully.")
                            else:
                                print(f"Failed to send POST request to webhook. Status code: {response.status_code}")
                else:
                    print(f"Ignored event with name: {event_name}")

            except Exception as e:
                print(f"Error: {str(e)}")

        # Connect to the database
        conn = db_connection
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Set up a LISTEN command for the channel
        cur.execute(f"LISTEN {psql_channel}")

        if webhook_url:
            print(f"Webhook: {webhook_url}")
        print(f"Listening for {psql_channel} events...")

        while True:
            conn.poll()
            while conn.notifies:
                notification = conn.notifies.pop()
                notification_handler(notification)

    except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
        print(f"Error: {e}")
        # Rollback the transaction (if needed)
        # conn.rollback()

    finally:
        # Close the cursor and connection
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def addtrigger():
    try:
        # The command to perform
        sql_payload = """'{"old": ' || row_to_json(OLD) || ', "new": ' || row_to_json(NEW) || '}'"""
        sql_command = f"""
-- Create a function to send a pg_notify event
CREATE OR REPLACE FUNCTION {trigger_name}Fn()
RETURNS TRIGGER AS $$
DECLARE
    psql_channel TEXT = '{psql_channel}'; -- Name of the channel for notification
BEGIN
    -- Send a pg_notify event with a custom channel and payload
    PERFORM pg_notify(psql_channel, {sql_payload});
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger that calls the function
CREATE TRIGGER {trigger_name}
AFTER INSERT OR UPDATE
ON {dbtable}
FOR EACH ROW
EXECUTE FUNCTION {trigger_name}Fn();
"""
        # Connect to the database
        conn = db_connection
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Execute the command
        cur.execute(f"{sql_command}")
        print(f"Success: Added trigger {trigger_name} and function {trigger_name}Fn.")

    except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
        print(f"Error: {e}")
        # Rollback the transaction (if needed)
        # conn.rollback()

    finally:
        # Close the cursor and connection
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
    

def removetrigger():
    try:
        # The command to perform
        sql_command = f"""
-- To drop the trigger and the function
DROP TRIGGER {trigger_name} ON loyalty_card;
DROP FUNCTION {trigger_name}Fn;
"""
        # Connect to the database
        conn = db_connection
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Execute the command
        cur.execute(f"{sql_command}")
        print(f"Success: Removed trigger {trigger_name} and function {trigger_name}Fn.")

    except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
        print(f"Error: {e}")
        # Rollback the transaction (if needed)
        # conn.rollback()

    finally:
        # Close the cursor and connection
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def main():
    if args.action == "listen":
        listen()

    elif args.action == "addtrigger":
        addtrigger()

    elif args.action == "removetrigger":
        removetrigger()

    else:
        # Handle invalid action
        print("Error: Invalid action specified.")

if __name__ == "__main__":
    main()
