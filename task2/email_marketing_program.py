import psycopg2

try:
    # connecting to local PostgreSQL server
    connection = psycopg2.connect(
        user='postgres',
        password='xxxx',
        host='localhost',
        port='5432',
        database='postgres'
    )

    cursor = connection.cursor()
    # creating empty tables for the given data
    create_tables_properties_and_values = '''
        DROP VIEW IF EXISTS projects_for_marketing;

        DROP TABLE IF EXISTS project_properties;

        CREATE TABLE project_properties
        (id INT,
        project_id INT,
        label VARCHAR(100));
   
        DROP TABLE IF EXISTS project_properties_values;

        CREATE TABLE project_properties_values
        (id INT,
        customer_id INT,
        property_id INT,
        value VARCHAR(1000),
        create_dte TIMESTAMP);
    '''
    cursor.execute(create_tables_properties_and_values)
    connection.commit()
    print("Empty tables created succesfully")

    # populating the tables
    with open('task2\data\project_properties_values.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'project_properties_values', sep=',')
    connection.commit()

    with open('task2\data\project_properties.csv', 'r') as file:
        next(file)
        cursor.copy_from(file, 'project_properties', sep=',')
    connection.commit()
    print("Tables populated succesfully")

    # view in question
    projects_for_marketing_view = '''
        CREATE OR REPLACE VIEW projects_for_marketing AS
        WITH properties_with_values AS (
            SELECT
                p.project_id,
                v.customer_id,
                v.value,
                p.label,
                ROW_NUMBER() OVER(PARTITION BY p.project_id, v.customer_id, p.label ORDER BY v.create_dte DESC) AS rn
            FROM public.project_properties_values AS v
            JOIN public.project_properties AS p ON v.property_id = p.id
        ),
        aggregated_newest_properties AS (
            SELECT 
                project_id,
                customer_id,
                MAX(CASE WHEN label IN ('e-mail', 'email') THEN value WHEN label = 'email_contents' THEN SUBSTRING(value FROM 'FROM:(.+?) CONTENTS') END) AS customer_email,
                MAX(CASE WHEN label = 'avg_message_volume' THEN CAST(value  AS DECIMAL(10,2)) END) AS avg_message_volume,
                MAX(CASE WHEN label = 'estimated_client_volume_usd' THEN CAST(value  AS DECIMAL(10,2)) END) AS estimated_client_volume_usd,
                MAX(CASE WHEN label = 'plan' THEN value END) AS plan,
                MAX(CASE WHEN label = 'interested_in_product' THEN value END) AS interested_in_product
            FROM properties_with_values
            WHERE rn = 1
            GROUP BY 
                project_id,
                customer_id
        )
            
        SELECT
            CAST(project_id AS INT),
            CAST(customer_id AS INT),
            CAST(customer_email AS VARCHAR(100)),
            CAST(avg_message_volume AS DECIMAL(10,2)),
            CAST(estimated_client_volume_usd AS DECIMAL(10,2)),
            CAST(plan AS VARCHAR(100)),
            CAST(interested_in_product AS VARCHAR(100))
        FROM
            aggregated_newest_properties
        WHERE
            avg_message_volume > 5000 AND
            estimated_client_volume_usd > 1000 AND
            plan = 'Free' AND
            interested_in_product = 'YES'
    '''
    cursor.execute(projects_for_marketing_view)
    connection.commit()
    print("View with client information for marketing program created succesfully.")

    # printing the results
    cursor.execute("SELECT * FROM projects_for_marketing")
    print('----------------------------------------')
    rows = cursor.fetchall()
    for row in rows:
        print(f"{row} \n----------------------------------------")

except Exception as ex:
    print(f"Error while creating PostgreSQL tables, message: {str(ex)}")
finally:
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection closed")