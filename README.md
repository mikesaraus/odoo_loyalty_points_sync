# Odoo Loyalty Program - Extender

## Sync Loyalty Programs from multiple Odoo instances using the unique customer's barcode

### Requirements

- Odoo v16
- Python
- PostresSQL Database

### Install Python and the required modules

```bash
# Note: `sudo` maybe required
# Python
apt-get install python3

# Modules
# using pip
pip3 install dotenv
pip3 install psycopg2
pip3 install requests
# or using apt package manager
apt-get install python3-dotenv
apt-get install python3-psycopg2
apt-get install python3-requests
```

### Configure `.env`

```bash
# copy .env.example to .env
cp .env.example .env
# then update the values accordingly
```

### Important .env keys are:

- `loyalty_program_id` - <b>Important!</b> <b>Required!</b> The local id of the loyalty program; default = 1
- `dbname` - the database name
- `dbuser` - the database user
- `dbpassword` - the database password of the user
- `dbhost` - the database host; default = localhost
- `dbport` - the database port; default = 5432

### Configure servers list `servers.json`

```bash
# copy servers.sample.json to servers.json
cp servers.sample.json servers.json
# then update the list accordingly
```

### Display help information

```bash
python3 listener.py -h
or
python3 listener.py --help
```

#### by [mike](https://github.com/mikesaraus)@[accountador.com](https://accountador.com)
