import psycopg2

try:
    connection = psycopg2.connect(
        user='postgres',
        password='xxxx',
        host='localhost',
        port='5432',
        database='postgres'
    )

    cursor = connection.cursor()
    create_tables_properties_and_values = '''
        DROP TABLE IF EXISTS project_properties;

        CREATE TABLE project_properties
        (id INT,
        project_id INT,
        label TEXT);
   
        DROP TABLE IF EXISTS project_properties_values;

        CREATE TABLE project_properties_values
        (id INT,
        customer_id INT,
        property_id INT,
        value TEXT,
        craete_dte TIMESTAMP);
    '''
    cursor.execute(create_tables_properties_and_values)
    connection.commit()
    print("Empty tables created succesfully")

    with open('task2\data\project_properties_values.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'project_properties_values', sep=',')
    connection.commit()

    with open('task2\data\project_properties.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'project_properties', sep=',')
    connection.commit()
    print("Tables populated succesfully")

except (Exception, psycopg2.DatabaseError) as error:
    print("Error while creating PostgreSQL table", error)
finally:
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection closed")