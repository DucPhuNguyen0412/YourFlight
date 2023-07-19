#!/bin/bash

# Ask for input
echo "Enter the departure locations (separated by commas): "
read departures

echo "Enter the destinations (separated by commas): "
read destinations

echo "Enter the departure dates (YYYY-MM-DD, separated by commas): "
read depart_dates

echo "Enter the return dates (YYYY-MM-DD, separated by commas): "
read return_dates

echo "Enter the numbers of travelers (separated by commas): "
read travelers

echo "Enter the cabin classes (separated by commas): "
read cabin_classes

echo "Enter the flight types (separated by commas): "
read flight_types

# Split each input by comma into an array
IFS=',' read -ra departure_array <<< "$departures"
IFS=',' read -ra destination_array <<< "$destinations"
IFS=',' read -ra depart_date_array <<< "$depart_dates"
IFS=',' read -ra return_date_array <<< "$return_dates"
IFS=',' read -ra travelers_array <<< "$travelers"
IFS=',' read -ra cabin_class_array <<< "$cabin_classes"
IFS=',' read -ra flight_type_array <<< "$flight_types"

# Get the length of any of the arrays
length=${#departure_array[@]}

# Set the bucket name
bucket="bestpricenphu"

# Loop over each index in the array
for (( i=0; i<$length; i++ )); do
    # Run the python script with each set of parameters
    python3 /Users/macbook/Documents/Documents_MacBook_Pro/ISTT/AirflowTutorial/src/scripts/web_scraping/kayak_scraping.py "${departure_array[$i]}" "${destination_array[$i]}" "${depart_date_array[$i]}" "${return_date_array[$i]}" "${travelers_array[$i]}" "${cabin_class_array[$i]}" "${flight_type_array[$i]}" "$bucket" >> script.log 2>&1
done
