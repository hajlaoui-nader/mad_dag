#!/bin/bash

# Database credentials
DB_USER="rshift"
DB_PASSWORD="rshift"
DB_NAME="analytics"
DB_HOST="localhost"
DB_PORT="4333"  # Change if your port is different

# Parameter to determine which query to run
query=$1

# Function to run number of events query
number_of_events() {
    docker exec -it redshift psql -U $DB_USER -d $DB_NAME -c \
    "
    -- number of events per day/month/year
    SELECT 
        EXTRACT(YEAR FROM timestamp) AS year,
        EXTRACT(MONTH FROM timestamp) AS month,
        EXTRACT(DAY FROM timestamp) AS day,
        COUNT(*) AS event_count
    FROM 
        events
    GROUP BY 
        year, month, day
    ORDER BY 
        year, month, day;
    "
}

# Function to run most active users query
most_active_users() {
    docker exec -it redshift psql -U $DB_USER -d $DB_NAME -c \
    "
    SELECT 
        email,
        COUNT(*) AS event_count
    FROM 
        events
    GROUP BY 
        email
    ORDER BY 
        event_count DESC
    LIMIT 10;
    "
}

# Function to run most active companies query
most_active_companies() {
    docker exec -it redshift psql -U $DB_USER -d $DB_NAME -c \
    "
    SELECT 
        company,
        COUNT(*) AS event_count
    FROM 
        events
    GROUP BY 
        company
    ORDER BY 
        event_count DESC
    LIMIT 10;
    "
}

# Check the parameter and run the corresponding query
case $query in
    number_of_events)
        number_of_events
        ;;
    most_active_users)
        most_active_users
        ;;
    most_active_companies)
        most_active_companies
        ;;
    *)
        echo "Invalid query parameter. Use one of: number_of_events, most_active_users, most_active_companies"
        ;;
esac
