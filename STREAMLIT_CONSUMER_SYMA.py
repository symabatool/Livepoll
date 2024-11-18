import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json
import threading

# Initialize Kafka consumer
consumer = KafkaConsumer('live_poll_topic',bootstrap_servers='localhost:9092', value_deserializer=lambda x: json.loads(x.decode('utf-8')),auto_offset_reset='earliest',
enable_auto_commit=True)

# Global variable to hold poll responses
poll_responses = []

def consume_poll_responses():
    global poll_responses
    for message in consumer:
        poll_responses.append(message.value)
        print(f"Received: {message.value}")  # Debug: Show received message

# Start the consumer in a separate thread
threading.Thread(target=consume_poll_responses, daemon=True).start()

# Streamlit app layout
st.title("Live Poll App")
st.subheader("Live Poll Results")

# KPI: Total Responses Received
st.metric("Total Responses Received", len(poll_responses))

# Create a DataFrame from poll responses
df = pd.DataFrame(poll_responses)

# Debug: Show the DataFrame and its columns
st.write("DataFrame:", df)
st.write("DataFrame Columns:", df.columns.tolist())  # Show the columns

# Display bar charts for each question
if not df.empty and 'answer_array' in df.columns:
    # Iterate over each response in the DataFrame
    for index, row in df.iterrows():
        st.subheader(f"Response ID: {row['answer_id']}")
        # Iterate over each question-answer pair in the answer_array
        for answer in row['answer_array']:
            for question, response in answer.items():
                st.write(f"{question}: {response}")
                
    # Optionally, create a summary of responses for each question
    question_summary = {}
    for index, row in df.iterrows():
        for answer in row['answer_array']:
            for question, response in answer.items():
                if question not in question_summary:
                    question_summary[question] = []
                question_summary[question].append(response)

    # Display bar charts for each question based on summary
    for question, responses in question_summary.items():
        st.subheader(question)
        st.bar_chart(pd.Series(responses).value_counts())

    # Display DataFrame of Responses
    st.subheader("Poll Responses DataFrame")
    st.dataframe(df)
else:
    st.write("No responses received yet or 'answer_array' column is missing.")

# Footer
st.markdown("---")
st.markdown("App Created By Saima Batool")