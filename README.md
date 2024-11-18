# Odoo Loyalty Program - Extender

## Sync Loyalty Programs from multiple Odoo instances using the unique customer's barcode

### Requirements:

- Python
- Odoo v16
- PostresSQL Database

### Notes:

You must configure Odoo to enable the `"Discounts, Loyalty & Gift Card"`.

Configure customer to have a unique `barcode` as it is used to identify unique customers.

### Install Python and the required modules

```bash
# Python
sudo apt install python3
# Pip
sudo apt install python3-pip

# Modules
# using pip
pip3 install -r requirements.txt
# or
pip3 install dotenv
pip3 install psycopg2
pip3 install requests
# or using apt package manager for system wide (service)
sudo apt install python3-dotenv python3-psycopg2 python3-requests
```

### Configure `.env`

```bash
# copy .env.example to .env
cp .env.example .env
# then update the values accordingly
nano .env
```

### Important .env keys

- `loyalty_program_id` - <b>Important!</b> The local id of the loyalty program; default = 1
- `dbname` - the database name
- `dbuser` - the database user
- `dbpassword` - the database password of the user
- `dbhost` - the database host; default = localhost
- `dbport` - the database port; default = 5432

### Configure db.config.json

```bash
# copy db.config.sample.json to db.config.json
cp db.config.sample.json db.config.json
# then update the list accordingly
nano db.config.json
```

### Configure servers list `servers.json`

```bash
# copy servers.sample.json to servers.json
cp servers.sample.json servers.json
# then update the list accordingly
nano servers.json
```

### Display help information

```bash
python3 listener.py -h
# or
python3 listener.py --help
```

#### by [mikee](https://github.com/mikesaraus)@[accountador.com](https://accountador.com)
