#!/usr/bin/env python3
"""
Script to generate synthetic database for two products (CineVibe and EventGo)
with a shared billing system.

Requirements:
1. Two products: CineVibe (subscription) and EventGo (pay-per-event)
2. Time period: 01.01.2025 to 31.12.2026
3. 20% overlap in user phone numbers
4. Realistic data without sharp metric jumps
5. Shared billing system
"""

import random
import datetime
from datetime import timedelta
import os
from dotenv import load_dotenv
import hashlib
import requests

# Load environment variables
load_dotenv()

# Database connection parameters
CLICKHOUSE_HOSTS = os.getenv('CLICKHOUSE_HOSTS', 'localhost:8123')
CLICKHOUSE_SECURE = os.getenv('CLICKHOUSE_SECURE', 'false').lower() == 'true'
DB_HOST = CLICKHOUSE_HOSTS.split(':')[0]
DB_PORT = int(CLICKHOUSE_HOSTS.split(':')[1]) if ':' in CLICKHOUSE_HOSTS else 8123
DB_USER = os.getenv('DB_USER', 'click_ro')
DB_PASSWORD = os.getenv('DB_PASSWORD', '457DfeoiwdW')
DB_NAME = os.getenv('CLICKHOUSE_DB', 'company-stat')

class ClickHouseHTTPClient:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        # Use HTTPS if CLICKHOUSE_SECURE is true
        protocol = "https" if CLICKHOUSE_SECURE else "http"
        self.url = f"{protocol}://{host}:{port}"
        self.base_url = self.url

    def execute(self, query, data=None):
        """Execute a query and return results"""
        headers = {
            'Content-Type': 'text/plain; charset=utf-8',
        }

        # Add authentication
        auth = (self.user, self.password) if self.user and self.password else None

        # Add database parameter to URL
        url_with_db = f"{self.url}?database={self.database}"

        # Process query to ensure proper database qualification
        processed_query = self._qualify_table_names(query)

        # Debug output
        # print(f"DEBUG: URL: {url_with_db}")
        # print(f"DEBUG: Query: {processed_query}")

        try:
            if data:
                full_query = processed_query
                if isinstance(data, list) and len(data) > 0:
                    if processed_query.strip().upper().endswith('VALUES'):
                        values_lines = []
                        for row in data:
                            if isinstance(row, (tuple, list)):
                                formatted_values = []
                                for val in row:
                                    # Добавляем обработку datetime.date и datetime.datetime
                                    if isinstance(val, (datetime.date, datetime.datetime)):
                                        # Преобразуем дату в строку и заключаем в кавычки
                                        date_str = val.strftime('%Y-%m-%d')
                                        formatted_values.append(f"'{date_str}'")
                                    elif isinstance(val, str):
                                        escaped_val = val.replace("'", "''")
                                        formatted_values.append(f"'{escaped_val}'")
                                    elif val is None:
                                        formatted_values.append('NULL')
                                    else:
                                        formatted_values.append(str(val))
                                values_lines.append(f"({', '.join(formatted_values)})")
                        full_query = processed_query + " " + ", ".join(values_lines)
                    else:
                        full_query = processed_query

                response = requests.post(
                    url_with_db,
                    headers=headers,
                    auth=auth,
                    data=full_query.encode('utf-8'),
                    timeout=30
                )
            else:
                response = requests.post(
                    url_with_db,
                    headers=headers,
                    auth=auth,
                    data=processed_query.encode('utf-8'),
                    timeout=30
                )

            # Debug output
            # print(f"DEBUG: Response status: {response.status_code}")
            # print(f"DEBUG: Response text: {response.text}")

            response.raise_for_status()

            # For SELECT queries, return parsed results
            if processed_query.strip().upper().startswith(('SELECT', 'WITH')):
                if response.text.strip():
                    # Parse TSV response
                    lines = response.text.strip().split('\n')
                    results = []
                    for line in lines:
                        if line.strip():
                            # Split by tab, handling escaped tabs if needed
                            values = line.split('\t')
                            # Convert types appropriately
                            converted_row = []
                            for val in values:
                                if val.lower() in ('null', ''):
                                    converted_row.append(None)
                                elif val.isdigit():
                                    converted_row.append(int(val))
                                else:
                                    # Try to convert to float if possible
                                    try:
                                        converted_row.append(float(val))
                                    except ValueError:
                                        converted_row.append(val)
                            if len(converted_row) >= 1:
                                if len(converted_row) == 1:
                                    results.append(converted_row[0])
                                else:
                                    results.append(tuple(converted_row))
                            else:
                                results.append(None)
                    return results
                return []
            else:
                # For other queries, return success
                return True

        except requests.exceptions.RequestException as e:
            # Debug output
            # print(f"DEBUG: RequestException: {e}")
            raise Exception(f"ClickHouse HTTP request failed: {e}")

    def _qualify_table_names(self, query):
        """Qualify table names with database name"""
        # List of table names that need to be qualified
        table_names = [
            'billing_log',
            'eventgo_ticket_purchases',
            'cinevibe_views_likes',
            'cinevibe_subscriptions_history',
            'eventgo_events',
            'cinevibe_movies',
            'eventgo_users',
            'cinevibe_users'
        ]

        qualified_query = query
        # Escape database name if it contains special characters like hyphens
        escaped_database = self.database
        if '-' in self.database:
            escaped_database = f'`{self.database}`'

        for table_name in table_names:
            # Replace table names with qualified names (database.table)
            # Handle common SQL patterns
            patterns = [
                f' {table_name} ',
                f' {table_name}\n',
                f' {table_name}\t',
                f' {table_name},',
                f' {table_name}\)',
                f'\n{table_name} ',
                f'\n{table_name}\n',
                f'\n{table_name}\t',
                f'\n{table_name},',
                f'\n{table_name}\)',
                f'\t{table_name} ',
                f'\t{table_name}\n',
                f'\t{table_name}\t',
                f'\t{table_name},',
                f'\t{table_name}\)',
                f',{table_name} ',
                f',{table_name}\n',
                f',{table_name}\t',
                f',{table_name},',
                f',{table_name}\)',
                f'\({table_name} ',
                f'\({table_name}\n',
                f'\({table_name}\t',
                f'\({table_name},',
                f'\({table_name}\)',
                f'`{table_name}`',
            ]

            for pattern in patterns:
                replacement = pattern.replace(table_name, f'{escaped_database}.{table_name}')
                qualified_query = qualified_query.replace(pattern, replacement)

        return qualified_query

    def insert(self, query, data):
        """Insert data into ClickHouse"""
        return self.execute(query, data)

def get_db_client():
    """Create and return a ClickHouse database client"""
    return ClickHouseHTTPClient(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def drop_existing_tables(client):
    """Drop all existing tables to start fresh"""
    print("🗑️  Dropping existing tables...")

    tables_to_drop = [
        'billing_log',
        'eventgo_ticket_purchases',
        'cinevibe_views_likes',
        'cinevibe_subscriptions_history',
        'eventgo_events',
        'cinevibe_movies',
        'eventgo_users',
        'cinevibe_users'
    ]

    for table in tables_to_drop:
        try:
            client.execute(f"DROP TABLE IF EXISTS {table}")
            print(f"  ✅ Dropped table: {table}")
        except Exception as e:
            print(f"  ⚠️  Error dropping table {table}: {e}")

def create_database_schema(client):
    """Create all required tables with proper schema and indexes"""
    print("🏗️  Creating database schema...")

    # Create cinevibe_users table
    client.execute("""
        CREATE TABLE IF NOT EXISTS cinevibe_users (
            user_id UInt32,
            msisdn String,
            reg_date Date
        ) ENGINE = MergeTree()
        ORDER BY (user_id)
        SETTINGS index_granularity = 8192
    """)
    print("  ✅ Created cinevibe_users table")

    # Create eventgo_users table
    client.execute("""
        CREATE TABLE IF NOT EXISTS eventgo_users (
            user_id UInt32,
            msisdn String,
            reg_date Date
        ) ENGINE = MergeTree()
        ORDER BY (user_id)
        SETTINGS index_granularity = 8192
    """)
    print("  ✅ Created eventgo_users table")

    # Create cinevibe_movies table
    client.execute("""
        CREATE TABLE IF NOT EXISTS cinevibe_movies (
            movie_id UInt32,
            title String,
            release_date Date,
            genre String,
            duration UInt16
        ) ENGINE = MergeTree()
        ORDER BY (movie_id)
        SETTINGS index_granularity = 8192
    """)
    print("  ✅ Created cinevibe_movies table")

    # Create eventgo_events table
    client.execute("""
        CREATE TABLE IF NOT EXISTS eventgo_events (
            event_id UInt32,
            title String,
            event_date Date,
            category String,
            base_price Decimal32(2),
            venue String
        ) ENGINE = MergeTree()
        ORDER BY (event_id)
        SETTINGS index_granularity = 8192
    """)
    print("  ✅ Created eventgo_events table")

    # Create cinevibe_subscriptions_history table
    client.execute("""
        CREATE TABLE IF NOT EXISTS cinevibe_subscriptions_history (
            user_id UInt32,
            subscription_type String,
            start_date Date,
            end_date Nullable(Date),
            payment_amount Decimal32(2)
        ) ENGINE = MergeTree()
        ORDER BY (user_id, start_date)
        SETTINGS index_granularity = 8192
    """)
    print("  ✅ Created cinevibe_subscriptions_history table")

    # Create cinevibe_views_likes table
    client.execute("""
        CREATE TABLE IF NOT EXISTS cinevibe_views_likes (
            user_id UInt32,
            movie_id UInt32,
            view_date Date,
            like_status String DEFAULT ''
        ) ENGINE = MergeTree()
        ORDER BY (user_id, movie_id, view_date)
        SETTINGS index_granularity = 8192
    """)
    print("  ✅ Created cinevibe_views_likes table")

    # Create eventgo_ticket_purchases table
    client.execute("""
        CREATE TABLE IF NOT EXISTS eventgo_ticket_purchases (
            user_id UInt32,
            event_id UInt32,
            purchase_date Date,
            amount Decimal32(2),
            ticket_quantity UInt8
        ) ENGINE = MergeTree()
        ORDER BY (user_id, event_id, purchase_date)
        SETTINGS index_granularity = 8192
    """)
    print("  ✅ Created eventgo_ticket_purchases table")

    # Create billing_log table
    client.execute("""
        CREATE TABLE IF NOT EXISTS billing_log (
            transaction_id String,
            user_id UInt32,
            product_name String,
            transaction_date Date,
            amount Decimal32(2),
            description String
        ) ENGINE = MergeTree()
        ORDER BY (transaction_date, user_id)
        SETTINGS index_granularity = 8192
    """)
    print("  ✅ Created billing_log table")

def generate_phone_numbers(count, existing_phones=None):
    """Generate unique phone numbers in format 7XXXXXXXXXX"""
    phones = set()
    if existing_phones:
        phones.update(existing_phones)

    while len(phones) < count:
        phone = "7" + "".join([str(random.randint(0, 9)) for _ in range(10)])
        phones.add(phone)

    return list(phones)[:count]

def generate_cinevibe_users(client):
    """Generate 1000 CineVibe users with 20% overlap with EventGo users"""
    print("👤 Generating CineVibe users...")

    # Generate 1000 unique phone numbers for CineVibe users
    cinevibe_phones = generate_phone_numbers(1000)

    # Select 200 phones for overlap with EventGo (20% of 1000)
    overlapping_phones = random.sample(cinevibe_phones, 200)

    # Generate user data
    users_data = []
    start_date = datetime.date(2025, 1, 1)
    end_date = datetime.date(2026, 12, 31)
    total_days = (end_date - start_date).days

    for i in range(1000):
        user_id = i + 1
        phone = cinevibe_phones[i]

        # Registration date should be before any activity
        # Distribute registrations more evenly across the entire period
        reg_days = int((i / 1000) * total_days)
        reg_date = start_date + timedelta(days=reg_days)

        users_data.append((user_id, phone, reg_date))

    # Insert data into database
    client.execute("INSERT INTO cinevibe_users VALUES", users_data)
    print(f"  ✅ Generated {len(users_data)} CineVibe users")

    return overlapping_phones

def generate_eventgo_users(client, overlapping_phones):
    """Generate 1000 EventGo users with 20% overlap with CineVibe users"""
    print("👤 Generating EventGo users...")

    # Generate 800 unique phone numbers for non-overlapping EventGo users
    non_overlapping_phones = generate_phone_numbers(800)

    # Combine overlapping and non-overlapping phones
    eventgo_phones = overlapping_phones + non_overlapping_phones

    # Generate user data
    users_data = []
    start_date = datetime.date(2025, 1, 1)
    end_date = datetime.date(2026, 12, 31)
    total_days = (end_date - start_date).days

    for i in range(1000):
        user_id = i + 1
        phone = eventgo_phones[i]

        # Registration date should be before any activity
        # Distribute registrations more evenly across the entire period
        reg_days = int((i / 1000) * total_days)
        reg_date = start_date + timedelta(days=reg_days)

        users_data.append((user_id, phone, reg_date))

    # Insert data into database
    client.execute("INSERT INTO eventgo_users VALUES", users_data)
    print(f"  ✅ Generated {len(users_data)} EventGo users")

def generate_cinevibe_movies(client):
    """Generate 100 movies with 1-2 word titles released evenly over the period"""
    print("🎬 Generating CineVibe movies...")

    movies_data = []
    start_date = datetime.date(2025, 1, 1)
    end_date = datetime.date(2026, 12, 31)
    total_days = (end_date - start_date).days

    # Sample 1-2 word movie titles
    movie_titles = [
        "Dark Knight", "Iron Man", "Ocean Deep", "Summer Days", "Winter Tale",
        "City Lights", "Star Wars", "Lost World", "Golden Gate", "Silver Screen",
        "Blue Moon", "Red Planet", "Green Mile", "Black Pearl", "White Noise",
        "Fast Lane", "Slow Burn", "High Tide", "Low Earth", "Big Sky",
        "Small Town", "Grand Prix", "Royal Flush", "Ace Ventura", "King Kong",
        "Queen Bee", "Jack Sparrow", "Rosebud", "Sunset Blvd", "Sunrise",
        "Midnight", "Noon Day", "Morning Glory", "Evening Star", "Night Shift",
        "Day Break", "Twilight", "Dawn Patrol", "Dusk Till", "Storm Front",
        "Rain Dance", "Snow Fall", "Wind Chill", "Fire Storm", "Ice Age",
        "Earth Rise", "Mars Attacks", "Jupiter", "Saturn", "Neptune",
        "Mercury", "Venus", "Pluto", "Galaxy Quest", "Cosmic Rays",
        "Solar Wind", "Lunar Eclipse", "Solar Flare", "Meteor Shower", "Comet Tail",
        "Black Hole", "White Dwarf", "Red Giant", "Blue Star", "Yellow Sun",
        "Magic Mike", "Wonder Woman", "Spider Man", "Super Mario", "Sonic Boom",
        "Matrix Reloaded", "Avatar", "Titanic", "Gladiator", "Casablanca",
        "Vertigo", "Psycho", "Forrest Gump", "Goodfellas", "Pulp Fiction",
        "Fight Club", "Inception", "Interstellar", "Parasite", "Moonlight",
        "La La Land", "Whiplash", "Get Out", "Her", "Arrival",
        "Blade Runner", "Alien", "Predator", "Terminator", "Robocop",
        "Ghostbusters", "E.T.", "Jaws", "Jurassic Park", "Back to Future",
        "Home Alone", "Braveheart", "Schindler List", "Shawshank", "Godfather",
        "Scarface", "Heat", "Training Day", "Malcolm X", "Dozens", "Men Black",
        "Independence Day", "Armageddon", "Deep Impact", "Volcano", "Dante Peak",
        "Twister", "Tornado", "Hurricane", "Blizzard", "Avalanche",
        "Earthquake", "Tsunami", "Wildfire", "Flood Tide", "Drought Season"
    ]

    genres = ['Action', 'Comedy', 'Drama', 'Horror', 'Romance', 'Sci-Fi', 'Thriller', 'Documentary']

    # Shuffle the movie titles to ensure randomness
    shuffled_titles = movie_titles.copy()
    random.shuffle(shuffled_titles)

    # Ensure we have 100 unique titles by extending the list if needed
    while len(shuffled_titles) < 100:
        # Add variations of existing titles to reach 100
        base_title = random.choice(movie_titles)
        variation = f"{base_title} {random.randint(2, 5)}"
        shuffled_titles.append(variation)

    for i in range(100):
        movie_id = i + 1
        title = shuffled_titles[i]  # Use each title uniquely

        # Distribute releases evenly over the period
        release_day = int((i / 100) * total_days)
        release_date = start_date + timedelta(days=release_day)

        genre = random.choice(genres)
        duration = random.randint(90, 180)  # 90-180 minutes

        movies_data.append((movie_id, title, release_date, genre, duration))

    # Insert data into database
    client.execute("INSERT INTO cinevibe_movies VALUES", movies_data)
    print(f"  ✅ Generated {len(movies_data)} CineVibe movies")

def generate_eventgo_events(client):
    """Generate 100 events with 1-2 word titles announced and occurring evenly over the period"""
    print("🎫 Generating EventGo events...")

    events_data = []
    start_date = datetime.date(2025, 1, 1)
    end_date = datetime.date(2026, 12, 31)
    total_days = (end_date - start_date).days

    # Sample 1-2 word event titles
    event_titles = [
        "Rock Festival", "Jazz Night", "Classical Concert", "Pop Showcase", "Indie Gig",
        "Broadway Show", "Drama Play", "Comedy Night", "Opera Performance", "Ballet Dance",
        "Football Match", "Basketball Game", "Tennis Tournament", "Soccer Cup", "Hockey League",
        "Art Exhibition", "Photo Gallery", "Sculpture Display", "Modern Art", "Vintage Collection",
        "Tech Conference", "Business Summit", "Startup Pitch", "Investor Meet", "Networking Event",
        "Food Festival", "Wine Tasting", "Cooking Class", "Chef Battle", "Culinary Tour",
        "Book Reading", "Poetry Slam", "Literary Talk", "Author Signing", "Writing Workshop",
        "Film Screening", "Movie Premiere", "Director Talk", "Behind Scenes", "Making Of",
        "Charity Gala", "Fundraising", "Community Fair", "Local Market", "Cultural Fest",
        "Dance Party", "Club Night", "DJ Set", "Live Music", "Karaoke Evening",
        "Science Fair", "Robotics Show", "Coding Workshop", "Hackathon", "Tech Demo",
        "Yoga Session", "Fitness Class", "Meditation", "Wellness Day", "Health Seminar",
        "Car Show", "Bike Rally", "Auto Expo", "Motor Race", "Driving Test",
        "Fashion Week", "Model Runway", "Design Show", "Style Talk", "Beauty Pageant",
        "Gaming Tournament", "Esports Championship", "VR Experience", "Console Battle", "Arcade Fun",
        "Magic Show", "Circus Act", "Standup Comedy", "Improvisation", "Sketch Performance",
        "Wrestling Match", "Boxing Fight", "Martial Arts", "Karate Demo", "Taekwondo",
        "Graduation", "Award Ceremony", "Recognition", "Achievement", "Celebration",
        "Wedding Reception", "Anniversary", "Birthday Party", "Engagement", "Baby Shower",
        "Corporate Meeting", "Board Session", "Team Building", "Workshop", "Seminar",
        "Trade Show", "Product Launch", "Innovation Fair", "Industry Meet", "Conference",
        "Political Rally", "Debate Night", "Campaign Event", "Town Hall", "Public Forum"
    ]

    categories = ['Concert', 'Theater', 'Sports', 'Cinema', 'Exhibition']
    venues = ['Main Hall', 'Concert Arena', 'Sports Stadium', 'Theater Hall', 'Gallery']

    # Shuffle the event titles to ensure randomness
    shuffled_titles = event_titles.copy()
    random.shuffle(shuffled_titles)

    # Ensure we have 100 unique titles by extending the list if needed
    while len(shuffled_titles) < 100:
        # Add variations of existing titles to reach 100
        base_title = random.choice(event_titles)
        variation = f"{base_title} {random.randint(2, 5)}"
        shuffled_titles.append(variation)

    for i in range(100):
        event_id = i + 1
        title = shuffled_titles[i]  # Use each title uniquely

        # Events are announced evenly over time
        announcement_day = int((i / 100) * total_days * 0.8)  # Announced in first 80% of period
        announcement_date = start_date + timedelta(days=announcement_day)

        # Event date is after announcement (but within the period)
        days_after_announcement = random.randint(7, min(180, total_days - announcement_day))
        event_date = announcement_date + timedelta(days=days_after_announcement)

        # Ensure event date doesn't exceed the end date
        if event_date > end_date:
            event_date = end_date

        category = random.choice(categories)
        base_price = round(random.uniform(500, 5000), 2)  # 500-5000 rubles
        venue = random.choice(venues)

        events_data.append((event_id, title, event_date, category, base_price, venue))

    # Insert data into database
    client.execute("INSERT INTO eventgo_events VALUES", events_data)
    print(f"  ✅ Generated {len(events_data)} EventGo events")

def generate_cinevibe_subscriptions(client):
    """Generate subscription history for CineVibe users"""
    print("💳 Generating CineVibe subscriptions...")

    subscriptions_data = []
    billing_data = []

    start_date = datetime.date(2025, 1, 1)
    end_date = datetime.date(2026, 12, 31)

    subscription_types = [
        {'type': 'basic', 'price': 100},
        {'type': 'premium', 'price': 200}
    ]

    for user_id in range(1, 1001):  # 1000 users
        # Each user may have 1-5 subscription periods
        num_periods = random.randint(1, 5)
        user_reg_date_result = client.execute("SELECT reg_date FROM cinevibe_users WHERE user_id = {}".format(user_id))
        user_reg_date_str = user_reg_date_result[0] if isinstance(user_reg_date_result, list) and len(user_reg_date_result) > 0 else user_reg_date_result
        # Convert string date to datetime.date object with more robust parsing
        if isinstance(user_reg_date_str, str):
            # Handle different date formats that might be returned by ClickHouse
            try:
                user_reg_date = datetime.datetime.strptime(user_reg_date_str, '%Y-%m-%d').date()
            except ValueError:
                # If parsing fails, try to extract date components
                import re
                date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', str(user_reg_date_str))
                if date_match:
                    year, month, day = map(int, date_match.groups())
                    user_reg_date = datetime.date(year, month, day)
                else:
                    # Fallback to a default date in the correct range
                    user_reg_date = datetime.date(2025, 1, 1)
        else:
            user_reg_date = user_reg_date_str

        current_date = user_reg_date + timedelta(days=random.randint(1, 30))  # Start subscription after registration

        for _ in range(num_periods):
            if current_date > end_date:
                break

            # Choose subscription type
            sub_type = random.choice(subscription_types)

            # Subscription start date
            start_date_sub = current_date

            # Subscription duration (30-365 days)
            duration = random.randint(30, 365)
            end_date_sub = start_date_sub + timedelta(days=duration)

            # Ensure end date doesn't exceed the overall period
            if end_date_sub > end_date:
                end_date_sub = None  # Still active

            # Add to subscriptions history
            subscriptions_data.append((user_id, sub_type['type'], start_date_sub, end_date_sub, sub_type['price']))

            # Add to billing log (monthly payments while subscription is active)
            billing_current = start_date_sub
            while billing_current <= (end_date_sub or end_date):
                transaction_id = hashlib.md5(f"{user_id}_{billing_current}_sub".encode()).hexdigest()
                description = f"Subscription CineVibe - {sub_type['type']}"
                billing_data.append((transaction_id, user_id, 'CineVibe', billing_current, sub_type['price'], description))

                # Move to next month
                next_month = billing_current.month + 1
                next_year = billing_current.year
                if next_month > 12:
                    next_month = 1
                    next_year += 1
                try:
                    billing_current = billing_current.replace(month=next_month, year=next_year)
                except ValueError:
                    # Handle day overflow (e.g., Jan 31 -> Feb 28/29)
                    billing_current = datetime.date(next_year, next_month, 1)

            # Set next subscription start date
            next_start_base = end_date_sub or end_date
            if next_start_base is not None:
                current_date = next_start_base + timedelta(days=random.randint(1, 30))
            else:
                # If end_date_sub is None (still active), we shouldn't create another subscription
                break

    # Insert subscription data
    client.execute("INSERT INTO cinevibe_subscriptions_history VALUES", subscriptions_data)
    print(f"  ✅ Generated {len(subscriptions_data)} CineVibe subscription records")

    # Insert billing data
    client.execute("INSERT INTO billing_log VALUES", billing_data)
    print(f"  ✅ Generated {len(billing_data)} CineVibe billing records")

def generate_cinevibe_views(client):
    """Generate movie views and likes for CineVibe users"""
    print("👁️  Generating CineVibe views and likes...")

    views_data = []

    start_date = datetime.date(2025, 1, 1)
    end_date = datetime.date(2026, 12, 31)

    # Get all movies with their release dates
    movies = client.execute("SELECT movie_id, release_date FROM cinevibe_movies")
    movies_dict = {}
    for movie in movies:
        movie_id, release_date_str = movie
        # Convert string date to datetime.date object
        release_date = datetime.datetime.strptime(str(release_date_str), '%Y-%m-%d').date()
        movies_dict[movie_id] = release_date

    # Get all users with their registration dates
    users = client.execute("SELECT user_id, reg_date FROM cinevibe_users")
    users_dict = {}
    for user in users:
        user_id, reg_date_str = user
        # Convert string date to datetime.date object with more robust parsing
        if isinstance(reg_date_str, str):
            # Handle different date formats that might be returned by ClickHouse
            try:
                reg_date = datetime.datetime.strptime(reg_date_str, '%Y-%m-%d').date()
            except ValueError:
                # If parsing fails, try to extract date components
                import re
                date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', str(reg_date_str))
                if date_match:
                    year, month, day = map(int, date_match.groups())
                    reg_date = datetime.date(year, month, day)
                else:
                    # Fallback to a default date in the correct range
                    reg_date = datetime.date(2025, 1, 1)
        else:
            reg_date = reg_date_str
        users_dict[user_id] = reg_date

    like_statuses = ['like', 'dislike']  # Only like or dislike, no empty values

    # Create a more even distribution by dividing the time period into months
    # and ensuring each user's views are distributed across specific months
    total_months = 24  # 24 months from Jan 2025 to Dec 2026

    for user_id, user_reg_date in users_dict.items():
        # Each user watches 10-100 movies
        num_views = random.randint(10, 100)

        # Assign user to a group based on their ID for even distribution across months
        user_group = (user_id - 1) % total_months

        for _ in range(num_views):
            # Choose a random movie
            movie_id = random.randint(1, 100)
            movie_release_date = movies_dict[movie_id]

            # View date must be after both user registration and movie release
            earliest_date = max(user_reg_date, movie_release_date)

            # Determine which month this user should primarily view content in
            # We'll create a window of 6 months centered around their assigned month
            assigned_month_offset = user_group
            window_start_offset = max(0, assigned_month_offset - 3)
            window_end_offset = min(total_months - 1, assigned_month_offset + 3)

            # Convert offsets to actual dates
            window_start = start_date + timedelta(days=window_start_offset * 30)
            window_end = start_date + timedelta(days=window_end_offset * 30 + 30)

            # Ensure the window is within bounds
            actual_window_start = max(earliest_date, window_start)
            actual_window_end = min(end_date, window_end)

            # If the window is valid for this user
            if actual_window_start < actual_window_end:
                # Generate a random date within the window
                window_days = (actual_window_end - actual_window_start).days
                if window_days > 0:
                    days_after = random.randint(0, window_days)
                    view_date = actual_window_start + timedelta(days=days_after)
                else:
                    view_date = actual_window_start
            else:
                # Fallback to original logic if window is not valid
                time_window_days = (end_date - earliest_date).days
                if time_window_days > 0:
                    days_after = random.randint(0, time_window_days)
                    view_date = earliest_date + timedelta(days=days_after)
                else:
                    view_date = earliest_date

            # Random like status (ensure every view has a status)
            like_status = random.choices(like_statuses, weights=[0.8, 0.2])[0]  # 80% like, 20% dislike

            views_data.append((user_id, movie_id, view_date, like_status))

    # Insert views data
    client.execute("INSERT INTO cinevibe_views_likes VALUES", views_data)
    print(f"  ✅ Generated {len(views_data)} CineVibe view records")

def generate_eventgo_purchases(client):
    """Generate ticket purchases for EventGo users"""
    print("🎟️  Generating EventGo ticket purchases...")

    purchases_data = []
    billing_data = []

    start_date = datetime.date(2025, 1, 1)
    end_date = datetime.date(2026, 12, 31)

    # Get all events with their dates and prices
    events = client.execute("SELECT event_id, event_date, base_price FROM eventgo_events")
    events_dict = {}
    for event in events:
        event_id, event_date_str, base_price = event
        # Convert string date to datetime.date object
        event_date = datetime.datetime.strptime(str(event_date_str), '%Y-%m-%d').date()
        events_dict[event_id] = (event_date, base_price)

    # Get all users with their registration dates
    users = client.execute("SELECT user_id, reg_date FROM eventgo_users")
    users_dict = {}
    for user in users:
        user_id, reg_date_str = user
        # Convert string date to datetime.date object
        reg_date = datetime.datetime.strptime(str(reg_date_str), '%Y-%m-%d').date()
        users_dict[user_id] = reg_date

    # Create a more even distribution by dividing the time period into months
    # and ensuring each user's purchases are distributed across specific months
    total_months = 24  # 24 months from Jan 2025 to Dec 2026

    for user_id, user_reg_date in users_dict.items():
        # Each user buys 1-20 tickets
        num_purchases = random.randint(1, 20)

        # Assign user to a group based on their ID for even distribution across months
        user_group = (user_id - 1) % total_months

        for _ in range(num_purchases):
            # Choose a random event
            event_id = random.randint(1, 100)
            event_date, base_price = events_dict[event_id]

            # Purchase date must be before event date and after registration
            if event_date <= user_reg_date:
                continue  # Skip if user registered after event date

            # Determine which month this user should primarily make purchases in
            # We'll create a window of 6 months centered around their assigned month
            assigned_month_offset = user_group
            window_start_offset = max(0, assigned_month_offset - 3)
            window_end_offset = min(total_months - 1, assigned_month_offset + 3)

            # Convert offsets to actual dates
            window_start = start_date + timedelta(days=window_start_offset * 30)
            window_end = start_date + timedelta(days=window_end_offset * 30 + 30)

            # Ensure the window is within bounds and after registration
            actual_window_start = max(user_reg_date, window_start)
            actual_window_end = min(event_date, window_end)

            # If the window is valid for this user and event
            if actual_window_start < actual_window_end:
                # Generate a random date within the window
                window_days = (actual_window_end - actual_window_start).days
                if window_days > 0:
                    days_after_reg = random.randint(0, window_days)
                    purchase_date = actual_window_start + timedelta(days=days_after_reg)
                else:
                    purchase_date = actual_window_start
            else:
                # Fallback to original logic if window is not valid
                days_before_event = (event_date - user_reg_date).days
                if days_before_event > 0:
                    days_after_reg = random.randint(0, min(days_before_event, (end_date - user_reg_date).days))
                    purchase_date = user_reg_date + timedelta(days=days_after_reg)
                else:
                    purchase_date = user_reg_date

            # Number of tickets (1-5)
            ticket_quantity = random.randint(1, 5)

            # Total amount
            amount = round(base_price * ticket_quantity, 2)

            # Add to purchases
            purchases_data.append((user_id, event_id, purchase_date, amount, ticket_quantity))

            # Add to billing log
            transaction_id = hashlib.md5(f"{user_id}_{event_id}_{purchase_date}_ticket".encode()).hexdigest()
            description = f"Ticket purchase for EventGo event {event_id}"
            billing_data.append((transaction_id, user_id, 'EventGo', purchase_date, amount, description))

    # Insert purchases data
    client.execute("INSERT INTO eventgo_ticket_purchases VALUES", purchases_data)
    print(f"  ✅ Generated {len(purchases_data)} EventGo purchase records")

    # Insert billing data
    client.execute("INSERT INTO billing_log VALUES", billing_data)
    print(f"  ✅ Generated {len(billing_data)} EventGo billing records")

def validate_data_consistency(client):
    """Validate that generated data meets all requirements"""
    print("🔍 Validating data consistency...")

    # Check 1: 20% phone number overlap
    overlap_query = """
        SELECT count(*)
        FROM (
            SELECT msisdn FROM cinevibe_users
            INTERSECT
            SELECT msisdn FROM eventgo_users
        )
    """
    overlap_result = client.execute(overlap_query)
    overlap_count = overlap_result[0] if isinstance(overlap_result, list) and len(overlap_result) > 0 else overlap_result
    expected_overlap = 200  # 20% of 1000
    print(f"  📱 Phone number overlap: {overlap_count}/{expected_overlap} (expected)")

    # Check 2: No actions before registration
    # For CineVibe views
    invalid_views_query = """
        SELECT count(*)
        FROM cinevibe_views_likes v
        JOIN cinevibe_users u ON v.user_id = u.user_id
        WHERE v.view_date < u.reg_date
    """
    invalid_views_result = client.execute(invalid_views_query)
    invalid_views = invalid_views_result[0] if isinstance(invalid_views_result, list) and len(invalid_views_result) > 0 else invalid_views_result
    print(f"  👁️  Invalid views (before registration): {invalid_views} (should be 0)")

    # For CineVibe subscriptions
    invalid_subs_query = """
        SELECT count(*)
        FROM cinevibe_subscriptions_history s
        JOIN cinevibe_users u ON s.user_id = u.user_id
        WHERE s.start_date < u.reg_date
    """
    invalid_subs_result = client.execute(invalid_subs_query)
    invalid_subs = invalid_subs_result[0] if isinstance(invalid_subs_result, list) and len(invalid_subs_result) > 0 else invalid_subs_result
    print(f"  💳 Invalid subscriptions (before registration): {invalid_subs} (should be 0)")

    # For EventGo purchases
    invalid_purchases_query = """
        SELECT count(*)
        FROM eventgo_ticket_purchases p
        JOIN eventgo_users u ON p.user_id = u.user_id
        WHERE p.purchase_date < u.reg_date
    """
    invalid_purchases_result = client.execute(invalid_purchases_query)
    invalid_purchases = invalid_purchases_result[0] if isinstance(invalid_purchases_result, list) and len(invalid_purchases_result) > 0 else invalid_purchases_result
    print(f"  🎟️  Invalid purchases (before registration): {invalid_purchases} (should be 0)")

    # Check 3: No views before movie release
    invalid_movie_views_query = """
        SELECT count(*)
        FROM cinevibe_views_likes v
        JOIN cinevibe_movies m ON v.movie_id = m.movie_id
        WHERE v.view_date < m.release_date
    """
    invalid_movie_views_result = client.execute(invalid_movie_views_query)
    invalid_movie_views = invalid_movie_views_result[0] if isinstance(invalid_movie_views_result, list) and len(invalid_movie_views_result) > 0 else invalid_movie_views_result
    print(f"  🎬 Invalid movie views (before release): {invalid_movie_views} (should be 0)")

    # Check 4: All views have valid like_status
    invalid_like_status_query = """
        SELECT count(*)
        FROM cinevibe_views_likes
        WHERE like_status NOT IN ('like', 'dislike') OR like_status IS NULL OR like_status = ''
    """
    invalid_like_status_result = client.execute(invalid_like_status_query)
    invalid_like_status = invalid_like_status_result[0] if isinstance(invalid_like_status_result, list) and len(invalid_like_status_result) > 0 else invalid_like_status_result
    print(f"  ❤️  Invalid like_status values: {invalid_like_status} (should be 0)")

    # Check 5: No purchases after event date
    invalid_event_purchases_query = """
        SELECT count(*)
        FROM eventgo_ticket_purchases p
        JOIN eventgo_events e ON p.event_id = e.event_id
        WHERE p.purchase_date > e.event_date
    """
    invalid_event_purchases_result = client.execute(invalid_event_purchases_query)
    invalid_event_purchases = invalid_event_purchases_result[0] if isinstance(invalid_event_purchases_result, list) and len(invalid_event_purchases_result) > 0 else invalid_event_purchases_result
    print(f"  📅 Invalid event purchases (after event date): {invalid_event_purchases} (should be 0)")

    print("  ✅ Data validation completed")

def main():
    """Main function to generate the synthetic database"""
    print("🚀 Starting synthetic database generation...")

    try:
        # Connect to database
        client = get_db_client()
        print(f"🔗 Connected to ClickHouse at {DB_HOST}:{DB_PORT}")

        # Drop existing tables
        drop_existing_tables(client)

        # Create database schema
        create_database_schema(client)

        # Generate users with 20% overlap
        overlapping_phones = generate_cinevibe_users(client)
        generate_eventgo_users(client, overlapping_phones)

        # Generate content
        generate_cinevibe_movies(client)
        generate_eventgo_events(client)

        # Generate user activities
        generate_cinevibe_subscriptions(client)
        generate_cinevibe_views(client)
        generate_eventgo_purchases(client)

        # Validate data consistency
        validate_data_consistency(client)

        print("🎉 Synthetic database generation completed successfully!")

    except Exception as e:
        print(f"❌ Error during database generation: {e}")
        raise

if __name__ == "__main__":
    main()