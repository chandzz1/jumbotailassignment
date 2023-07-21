import threading
import time
from collections import deque
import random
import psycopg2
from flask import Flask, request

app = Flask(__name__)

class EcommerceUserThread(threading.Thread):
    def __init__(self, user_id, webhook_url, event_queue, event_repository):
        super().__init__()
        self.user_id = user_id
        self.webhook_url = webhook_url
        self.event_queue = event_queue
        self.event_repository = event_repository

    def run(self):
        events = self.generate_events()
        self.push_events_to_queue(events)

    def generate_events(self):
        events = []
        cities = ["chennai", "delhi", "hyderabad", "kolkata", "mumbai", "pune"]
        num_events = random.randint(1, 10)
        for _ in range(num_events):
            event = {
                "user_id": self.user_id,
                "event_type": random.choice(["click", "purchase", "view", "selects product", "adds to cart"]),
                "timestamp": time.time(),
                "stage": random.randint(1, 6),  # Random stage number from 1 to 6
                "city": random.choice(cities),
            }
            events.append(event)
        return events

    def push_events_to_queue(self, events):
        for event in events:
            self.event_queue.append(event)
            print(f"Event pushed to the queue for user {self.user_id}")

    def send_batch_to_webhook(self, batch):
        import requests
        try:
            response = requests.post(self.webhook_url, json=batch)
            if response.status_code == 200:
                print(f"Events sent successfully for user {self.user_id}")
            else:
                print(f"Failed to send events for user {self.user_id}. Error: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Error occurred while sending events for user {self.user_id}. {str(e)}")

# Number of users to simulate
NUM_USERS = 10

# Webhook URL to send events
WEBHOOK_URL = "http://localhost:8888/webhook"

# Create an in-memory event queue using deque
event_queue = deque(maxlen=100)  # Adjust the maxlen based on your requirements

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="user1",
    user="postgres",
    password="1234"
)

# Create a cursor for database operations
cursor = conn.cursor()

# Create a sample event repository with PostgreSQL integration
class EventRepository:
    def insert(self, event):
        query = "INSERT INTO events (user_id, event_type, timestamp, stage, city) VALUES (%s, %s, %s, %s, %s)"
        values = (
            event["user_id"],
            event["event_type"],
            event["timestamp"],
            event["stage"],
            event["city"],
        )
        cursor.execute(query, values)
        conn.commit()
        print(f"Event inserted into the database: {event}")

    def count_users_in_each_stage(self):
        query = "SELECT event_type, COUNT(*) FROM events GROUP BY event_type"
        cursor.execute(query)
        result = cursor.fetchall()
        stage_counts = {row[0]: row[1] for row in result}
        return stage_counts

    def count_users_in_each_city(self):
        query = "SELECT city, COUNT(DISTINCT user_id) FROM events GROUP BY city"
        cursor.execute(query)
        result = cursor.fetchall()
        city_counts = {row[0]: row[1] for row in result}
        return city_counts

    def calculate_percentage_users_in_each_stage(self):
        stage_counts = self.count_users_in_each_stage()
        total_users = sum(stage_counts.values())
        percentage_users = {stage: (count / total_users) * 100 for stage, count in stage_counts.items()}
        return percentage_users



    def get_user_activities(self):
        query = "SELECT * FROM events"
        cursor.execute(query)
        result = cursor.fetchall()
        return result

# Create the event repository instance
event_repository = EventRepository()

# Create and start the user threads
user_threads = []
for i in range(NUM_USERS):
    user_thread = EcommerceUserThread(i+1, WEBHOOK_URL, event_queue, event_repository)
    user_threads.append(user_thread)
    user_thread.start()

# Start the Flask server to receive events
@app.route("/webhook", methods=["POST", "GET"])
def webhook():
    if request.method == "POST":
        event = request.json
        event_repository.insert(event)
        return "Event received"
    else:
        return "This endpoint only supports POST requests."

# Run the Flask server in a separate thread
server_thread = threading.Thread(target=app.run, kwargs={"host": "localhost", "port": 8888})
server_thread.start()

# Wait for all threads to complete
for user_thread in user_threads:
    user_thread.join()

# Calculate percentages of users in each stage
stage_percentages = event_repository.calculate_percentage_users_in_each_stage()

# Calculate the number of users in each city
city_counts = event_repository.count_users_in_each_city()

# Fetch user activities
user_activities = event_repository.get_user_activities()

# Print the results
print("Simulating user events completed.\n")
print("User activities:")
for activity in user_activities:
    print(activity)

print("\nPercentage of users in each stage:")
for event_type, percentage in stage_percentages.items():
    print(f"{event_type.capitalize()}: {percentage:.2f}%")

print("\nNumber of users in each city:")
for city, count in city_counts.items():
    print(f"{city}: {count}")

# Stop the Flask server
server_thread.join()

# Close the database connection
cursor.close()
conn.close()

print("\nAll users have completed their actions.")
