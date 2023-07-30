import subprocess

# Ask the user for the type of flight
flight_type = int(input("Enter the type of flight (0 for One-way, 1 for Round-trip): "))

if flight_type == 0:
    # Call the one-way script
    subprocess.call(["python", "src/scripts/web_scraping/one_way_kayak_scraping.py"])
elif flight_type == 1:
    # Call the round-trip script
    subprocess.call(["python", "src/scripts/web_scraping/round_trip_kayak_scraping.py"])
else:
    print("Invalid input. Please enter 0 for One-way or 1 for Round-trip.")
