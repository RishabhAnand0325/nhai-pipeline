"""Geospatial Analyzer for distance and location calculations"""
import math
import googlemaps
import pandas as pd
import geopy.distance


class GeospatialAnalyzer:
    """Handles geospatial operations like distance calculation and geocoding"""

    def __init__(self, config):
        self.config = config
        self.gmaps = googlemaps.Client(key=config.gmaps_api_key)

    def calculate_distance(self, lat1, lon1, lat2, lon2):
        """Calculate distance between two coordinates using Haversine formula (in kilometers)"""
        R = 6371  # Earth radius in kilometers
        dLat = (lat2 - lat1) * (math.pi / 180)
        dLon = (lon2 - lon1) * (math.pi / 180)
        a = (
            math.sin(dLat / 2) ** 2 +
            math.cos(lat1 * (math.pi / 180)) * math.cos(lat2 * (math.pi / 180)) *
            math.sin(dLon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = R * c
        return distance

    def calculate_distance_meters(self, lat1, lon1, lat2, lon2):
        """Calculate distance between two coordinates in meters"""
        return geopy.distance.geodesic((lat1, lon1), (lat2, lon2)).meters

    def calculate_total_distance(self, frame_list_data):
        """Calculate total distance from frame data"""
        total_distance = 0
        distances = []

        for i in range(1, len(frame_list_data)):
            prev_frame = frame_list_data[i - 1]
            current_frame = frame_list_data[i]

            # Check if latitude and longitude keys exist
            if not all(key in prev_frame for key in ['latitude', 'longitude']):
                print(f"Warning: Missing latitude/longitude in frame {i-1}")
                distances.append(0)
                continue

            if not all(key in current_frame for key in ['latitude', 'longitude']):
                print(f"Warning: Missing latitude/longitude in frame {i}")
                distances.append(0)
                continue

            if (prev_frame['latitude'] == current_frame['latitude'] and
                prev_frame['longitude'] == current_frame['longitude']):
                distance = 0
            else:
                distance = self.calculate_distance(
                    prev_frame['latitude'],
                    prev_frame['longitude'],
                    current_frame['latitude'],
                    current_frame['longitude']
                )

            total_distance += distance
            distances.append(distance)

        return total_distance, distances

    def calculate_chainage(self, df, distances):
        """Calculate chainage for each frame"""
        df['distance'] = [0] + distances
        chainage = 0
        chainages = []
        chainage_distance = 0

        for distance in df['distance']:
            chainage_distance += distance * 1000
            chainages.append(chainage)

            if chainage_distance >= 100:
                chainage += 1
                chainage_distance -= 100

        df['chainage'] = chainages
        return df

    def get_address(self, latitude, longitude):
        """Get address from coordinates using reverse geocoding"""
        try:
            result = self.gmaps.reverse_geocode((latitude, longitude))
            if not result:
                return 'Locality road'

            address_components = result[0]['address_components']
            formatted_address = result[0]['formatted_address']

            # Remove premises or plus code from address
            for component in address_components:
                if 'premise' in component['types'] or 'plus_code' in component['types']:
                    premise_or_plus_code = component['long_name']
                    formatted_address = formatted_address.replace(
                        premise_or_plus_code, ''
                    ).replace(',,', ',').strip(', ')

            return formatted_address

        except Exception as e:
            print(f"Error getting address for {latitude}, {longitude}: {e}")
            return 'Address not found'
