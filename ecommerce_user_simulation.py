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
        # Simulate user behavior and generate events
        events = self.generate_events()

        # Push events to the in-memory queue with retry mechanism
        self.push_events_to_queue(events)

    def generate_events(self):
        # Implement event generation logic based on user behavior
        # Consider the ratio of events, order of events, stages of engagement, and user dropouts
        events = []

        # Example event generation logic (modify based on your requirements)
        num_events = random.randint(1, 10)  # Generate a random number of events
        for _ in range(num_events):
            event = {
                "user_id": self.user_id,
                "event_type": random.choice(["click", "purchase", "view"]),
                "timestamp": time.time(),
                # Add other necessary attributes for your events
            }
            events.append(event)

        return events

    def push_events_to_queue(self, events):
        max_retries = 3  # Maximum number of retry attempts

        for event in events:
            success = False
            retries = 0

            while not success and retries < max_retries:
                try:
                    self.event_queue.append(event)
                    success = True
                    print(f"Event pushed to the queue for user {self.user_id}")
                except IndexError:
                    retries += 1
                    print(f"Queue is full. Retrying to push event for user {self.user_id}. Retry attempt {retries} of {max_retries}")
                    time.sleep(1)  # Wait for some time before retrying

            if not success:
                print(f"Failed to push event to the queue for user {self.user_id} after {max_retries} retries")

    def send_batch_to_webhook(self, batch):
        # Implement the logic to send the event batch to the webhook
        # You can use appropriate libraries or modules to make HTTP requests

        # Example code using requests library
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
WEBHOOK_URL = "http://localhost:8888/webhook"  # Replace with your actual webhook URL

# Create an in-memory event queue using deque
event_queue = deque(maxlen=100)  # Adjust the maxlen based on your requirements

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host="your_host",
    database="your_database",
    user="your_user",
    password="your_password"
)

# Create a cursor for database operations
cursor = conn.cursor()

# Create a sample event repository with PostgreSQL integration
class EventRepository:
    def insert(self, event):
        # Insert event into the PostgreSQL database
        query = "INSERT INTO events (user_id, event_type, timestamp) VALUES (%s, %s, %s)"
        values = (event["user_id"], event["event_type"], event["timestamp"])
        cursor.execute(query, values)
        conn.commit()
        print(f"Event inserted into the database: {event}")

    def count_users_in_each_stage(self):
        # Perform the necessary query to count the number of users in each stage
        # Example using SQL query
        query = "SELECT stage, COUNT(*) FROM events GROUP BY stage"
        cursor.execute(query)
        result = cursor.fetchall()
        stage_counts = {row[0]: row[1] for row in result}
        return stage_counts

    def count_users_in_each_city(self):
        # Perform the necessary query to count the number of users in each city
        # Example using SQL query
        query = "SELECT city, COUNT(*) FROM events GROUP BY city"
        cursor.execute(query)
        result = cursor.fetchall()
        city_counts = {row[0]: row[1] for row in result}
        return city_counts

    def calculate_percentage_users_in_each_stage(self):
        stage_counts = self.count_users_in_each_stage()
        total_users = sum(stage_counts.values())
        percentage_users = {stage: (count / total_users) * 100 for stage, count in stage_counts.items()}
        return percentage_users

    def calculate_percentage_users_in_each_city(self):
        city_counts = self.count_users_in_each_city()
        total_users = sum(city_counts.values())
        percentage_users = {city: (count / total_users) * 100 for city, count in city_counts.items()}
        return percentage_users

# Create the event repository instance
event_repository = EventRepository()

# Create and start the user threads
user_threads = []
for i in range(NUM_USERS):
    user_thread = EcommerceUserThread(i+1, WEBHOOK_URL, event_queue, event_repository)
    user_threads.append(user_thread)
    user_thread.start()

# Start the Flask server to receive events
@app.route("/webhook", methods=["POST"])
def webhook():
    event = request.json
    event_repository.insert(event)
    return "Event received"

# Run the Flask server in a separate thread
server_thread = threading.Thread(target=app.run, kwargs={"host": "localhost", "port": 8888})
server_thread.start()

# Wait for all threads to complete
for user_thread in user_threads:
    user_thread.join()

# Calculate percentages of users in each stage
stage_percentages = event_repository.calculate_percentage_users_in_each_stage()

# Calculate percentages of users in each city
city_percentages = event_repository.calculate_percentage_users_in_each_city()

# Print the results
print("Percentage of users in each stage of the user journey:")
for stage, percentage in stage_percentages.items():
    print(f"{stage}: {percentage}%")

print("\nEvaluation of the performance of different cities:")
for city, percentage in city_percentages.items():
    print(f"{city}: {percentage}%")

# Stop the Flask server
server_thread.join()

# Close the database connection
cursor.close()
conn.close()

print("All users have completed their actions.")
