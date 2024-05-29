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
        create_dte TIMESTAMP);
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
            project_id,
            customer_id,
            customer_email,
            avg_message_volume,
            estimated_client_volume_usd,
            plan,
            interested_in_product
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

except (Exception, psycopg2.DatabaseError) as error:
    print("Error while creating PostgreSQL table", error)
finally:
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection closed")