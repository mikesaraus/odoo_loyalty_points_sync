# Odoo Loyalty Program - Extender

## Sync Loyalty Programs from multiple database using unique customers barcode

### Requirements

- Odoo v16
- Python
- PostresSQL Database

### Install the required module (must have python)

```bash
# Note: `sudo` maybe required
apt-get install python3

# using pip
pip3 install dotenv
pip3 install psycopg2
pip3 install requests

# or using apt package manager
apt-get install python3-dotenv
apt-get install python3-psycopg2
apt-get install python3-requests
```

### Configure servers list `servers.json`

```bash
# rename servers.sample.json to servers.json
mv servers.sample.json servers.json
# then update the list accordingly
```

### Display help information

```bash
python3 listener.py -h
or
python3 listener.py --help
```
