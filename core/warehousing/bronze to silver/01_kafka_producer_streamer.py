import pandas as pd
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
import os
import random

class VegetableDataStreamer:
    def __init__(self):
        # Kafka configuration
        self.bootstrap_servers = 'redpanda:9092'
        self.topic_name = 'vegetable_data'
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        
        # Streaming configuration
        self.events_per_minute = 10  # Control streaming speed
        self.days_to_compress = 365  # Compress 1 year data into how many days of streaming
        
    def load_and_transform_data(self, csv_path):
        """Load CSV and transform 2024 data to appear as current data"""
        print("📊 Loading and transforming data...")
        df = pd.read_csv(csv_path)
        
        # Convert Report_Date to datetime
        df['Report_Date'] = pd.to_datetime(df['Report_Date'])
        
        # Get the date range in original data
        original_start = df['Report_Date'].min()
        original_end = df['Report_Date'].max()
        original_days = (original_end - original_start).days
        
        print(f"Original data range: {original_start} to {original_end} ({original_days} days)")
        
        # Calculate new date range (compress into shorter period)
        streaming_end = datetime.now()
        streaming_start = streaming_end - timedelta(days=self.days_to_compress)
        
        print(f"Streaming as: {streaming_start} to {streaming_end}")
        
        # Transform dates to appear current
        df = self.transform_dates(df, original_start, original_end, streaming_start, streaming_end)
        
        return df
    
    def transform_dates(self, df, orig_start, orig_end, stream_start, stream_end):
        """Transform dates from 2024 to appear as current dates"""
        orig_range = (orig_end - orig_start).days
        stream_range = (stream_end - stream_start).days
        
        def map_date(original_date):
            days_from_start = (original_date - orig_start).days
            new_days = int((days_from_start / orig_range) * stream_range)
            new_date = stream_start + timedelta(days=new_days)
            return new_date
        
        df['Streaming_Date'] = df['Report_Date'].apply(map_date)
        df['Original_Date'] = df['Report_Date']
        df['Report_Date'] = df['Streaming_Date']  # Use new date for reporting
        
        return df
    
    def stream_data(self, df):
        """Stream data in real-time simulation"""
        print("🔄 Starting real-time data streaming...")
        
        # Sort by new streaming date
        df = df.sort_values('Streaming_Date')
        
        current_stream_date = None
        records_sent = 0
        
        for index, row in df.iterrows():
            record = row.to_dict()
            
            # Convert numpy types to Python native types
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None
                elif hasattr(value, 'item'):
                    record[key] = value.item()
            
            # Add streaming metadata
            record['kafka_timestamp'] = datetime.now().isoformat()
            record['streaming_batch_id'] = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            record['is_simulated_live'] = True
            
            # Send to Kafka
            self.producer.send(self.topic_name, value=record)
            records_sent += 1
            
            # Print progress
            stream_date = record['Streaming_Date']
            if current_stream_date != stream_date:
                current_stream_date = stream_date
                print(f"📅 Streaming data for: {stream_date.strftime('%Y-%m-%d')}")
            
            # Control streaming speed
            if records_sent % 5 == 0:
                time.sleep(60 / self.events_per_minute)  # Control events per minute
            
            if records_sent % 50 == 0:
                print(f"✅ Sent {records_sent} records...")
        
        # Flush and close
        self.producer.flush()
        self.producer.close()
        
        print(f"🎉 Streaming completed! Total records sent: {records_sent}")
    
    def run(self, csv_path):
        """Main method to run the streaming simulation"""
        try:
            print("🥦 Vegetable Data Streamer Started")
            print("=" * 50)
            
            df = self.load_and_transform_data(csv_path)
            self.stream_data(df)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    streamer = VegetableDataStreamer()
    streamer.run('/app/data/TN_wastage_data.csv')
