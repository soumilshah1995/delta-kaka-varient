from kafka import KafkaProducer
from faker import Faker
import json
from time import sleep
import uuid

# Initialize Kafka Producer
producer = KafkaProducer(bootstrap_servers='localhost:7092')
faker = Faker()

class DataGenerator:
    @staticmethod
    def get_data():
        return [
            uuid.uuid4().__str__(),
            faker.name(),
            faker.random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
            faker.random_element(elements=('CA', 'NY', 'TX', 'FL', 'IL', 'RJ')),
            faker.random_int(min=10000, max=150000),
            faker.random_int(min=18, max=60),
            faker.random_int(min=0, max=100000),
            faker.unix_time(),
        ]

    @staticmethod
    def get_properties():
        """Generate nested JSON for the 'properties' column."""
        return {
            "address": {
                "street": faker.street_name(),
                "city": faker.city(),
                "zip": faker.zipcode(),
            },
            "employment": {
                "position": faker.job(),
                "experience_years": faker.random_int(min=1, max=40),
                "is_manager": faker.boolean(),
            },
            "contacts": {
                "email": faker.email(),
                "phone": faker.phone_number(),
            },
            "preferences": {
                "newsletter_subscribed": faker.boolean(),
                "preferred_language": faker.random_element(elements=["English", "Spanish", "French"]),
            },
        }

# Kafka Topic
topic_name = "customers"

# Columns for the main record
columns = ["emp_id", "employee_name", "department", "state", "salary", "age", "bonus", "ts", "properties"]

# Generate and send messages to Kafka
for _ in range(20):
    for i in range(1, 20):
        # Generate flat data and nested properties
        data_list = DataGenerator.get_data()
        nested_properties = DataGenerator.get_properties()

        # Include 'properties' as the last column
        data_list.append(nested_properties)

        # Create JSON payload
        json_data = dict(zip(columns, data_list))
        _payload = json.dumps(json_data).encode("utf-8")

        # Send to Kafka topic
        response = producer.send(topic_name, _payload)
        print(f"Sent to Kafka: {_payload}")
        sleep(5)
