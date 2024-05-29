# How to run task2

1. Make sure you have PostgreSQL installed, if not get it from: https://www.postgresql.org/download/
during the download you will be asked to provide a password, note it down as it will be used in script.

(1.5) If you wish, you can create virtual environment before instaling libraries.

2. Script uses packages beyond the standard library, install them by simply running command 
`pip install -r requirements.txt` in your terminal.

3. Code will be run on your local PostgreSQL server and data from final view will be returned in the terminal.
However if you wish to preview the created tables and the view itself, you can use a gui like 
for ex. pgAdmin from https://www.pgadmin.org/download/

4. To run the code open terminal in the folder where the file is located and run `python email_marketing_program.py` (might be `python3 ...` depending on set up).