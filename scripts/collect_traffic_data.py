import os
import json
import requests
import pandas as pd
from datetime import datetime
import time
from pathlib import Path

# Configuration
TOMTOM_API_KEY = os.getenv('TOMTOM_API_KEY')
ROUTES_CONFIG_FILE = 'routes_config.json'
DATA_DIR = Path('data/raw')

# TomTom API endpoint
TOMTOM_BASE_URL = "https://api.tomtom.com/routing/1/calculateRoute"


def load_routes():
    """Load routes from configuration file"""
    with open(ROUTES_CONFIG_FILE, 'r') as f:
        config = json.load(f)
    return config['routes']


def get_traffic_data(origin_lat, origin_lon, dest_lat, dest_lon, api_key, max_retries=3):
    """
    Fetch traffic data from TomTom API with retry logic
    """
    route = f"{origin_lat},{origin_lon}:{dest_lat},{dest_lon}"
    
    params = {
        'key': api_key,
        'traffic': 'true',
        'travelMode': 'car',
        'departAt': 'now'
    }
    
    url = f"{TOMTOM_BASE_URL}/{route}/json"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'routes' in data and len(data['routes']) > 0:
                    route_info = data['routes'][0]
                    summary = route_info['summary']
                    
                    return {
                        'distance_km': round(summary['lengthInMeters'] / 1000, 2),
                        'duration_minutes': round(summary['travelTimeInSeconds'] / 60, 1),
                        'traffic_delay_minutes': round(summary.get('trafficDelayInSeconds', 0) / 60, 1),
                        'status': 'success'
                    }
                else:
                    print(f"⚠️  No route data in response")
                    return None
                    
            elif response.status_code == 429:
                print(f"⚠️  Rate limit hit, waiting 60 seconds...")
                time.sleep(60)
                continue
                
            else:
                print(f"⚠️  API error: {response.status_code}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                continue
                
        except requests.exceptions.RequestException as e:
            print(f"⚠️  Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(5)
            continue
    
    return None


def collect_all_routes():
    """Collect traffic data for all routes"""
    
    if not TOMTOM_API_KEY:
        print("❌ ERROR: TOMTOM_API_KEY environment variable not set!")
        return
    
    print(f"🚗 Starting traffic data collection at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    routes = load_routes()
    print(f"📍 Loaded {len(routes)} routes")
    
    now = datetime.now()
    timestamp_str = now.strftime('%Y-%m-%d %H:%M:%S')
    date_str = now.strftime('%Y%m%d')
    
    collected_data = []
    
    for route in routes:
        print(f"\n📊 Collecting data for: {route['name']}")
        
        traffic_data = get_traffic_data(
            route['origin']['lat'],
            route['origin']['lon'],
            route['destination']['lat'],
            route['destination']['lon'],
            TOMTOM_API_KEY
        )
        
        if traffic_data:
            traffic_data.update({
                'timestamp': timestamp_str,
                'route_name': route['name'],
                'route_id': route['id'],
                'origin': route['origin']['name'],
                'destination': route['destination']['name'],
                'hour': now.hour,
                'day_of_week': now.strftime('%A'),
                'is_weekend': 1 if now.weekday() >= 5 else 0
            })
            
            collected_data.append(traffic_data)
            print(f"   ✅ Success: {traffic_data['duration_minutes']} min, {traffic_data['traffic_delay_minutes']} min delay")
        else:
            print(f"   ❌ Failed to collect data")
            collected_data.append({
                'timestamp': timestamp_str,
                'route_name': route['name'],
                'route_id': route['id'],
                'origin': route['origin']['name'],
                'destination': route['destination']['name'],
                'hour': now.hour,
                'day_of_week': now.strftime('%A'),
                'is_weekend': 1 if now.weekday() >= 5 else 0,
                'distance_km': None,
                'duration_minutes': None,
                'traffic_delay_minutes': None,
                'status': 'failed'
            })
        
        time.sleep(1)
    
    if collected_data:
        df = pd.DataFrame(collected_data)
        
        column_order = [
            'timestamp', 'route_name', 'distance_km', 'duration_minutes', 
            'traffic_delay_minutes', 'status', 'route_id', 'origin', 
            'destination', 'hour', 'day_of_week', 'is_weekend'
        ]
        df = df[column_order]
        
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        
        csv_file = DATA_DIR / f'traffic_data_{date_str}.csv'
        
        if csv_file.exists():
            df.to_csv(csv_file, mode='a', header=False, index=False)
            print(f"\n✅ Data appended to: {csv_file}")
        else:
            df.to_csv(csv_file, index=False)
            print(f"\n✅ New file created: {csv_file}")
        
        success_count = df[df['status'] == 'success'].shape[0]
        print(f"📈 Summary: {success_count}/{len(routes)} routes collected successfully")
        
        print(f"\n📋 Sample data:")
        print(df[['route_name', 'duration_minutes', 'traffic_delay_minutes']].to_string(index=False))
    else:
        print("\n❌ No data collected")
    
    print(f"\n🏁 Collection completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    collect_all_routes()
