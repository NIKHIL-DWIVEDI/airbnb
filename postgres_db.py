import psycopg2
import json
import os
import glob
from datetime import datetime

## database connection parameters
DB_CONFIG = {
    "dbname": "airbnb_db",
    "user": "nikhil",
    "password": "nikhil123",
    "host": "localhost",
    "port": "5432"  
}

def test_database_connection():
    """Test database connection and basic operations"""
    print("Testing database connection...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Test basic query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✓ PostgreSQL version: {version[0]}")
        
        # Test database existence
        cursor.execute("SELECT current_database();")
        db_name = cursor.fetchone()
        print(f"✓ Connected to database: {db_name[0]}")
        
        # Test table creation permissions
        cursor.execute("CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, test_col TEXT);")
        cursor.execute("INSERT INTO test_table (test_col) VALUES ('test_value');")
        cursor.execute("SELECT COUNT(*) FROM test_table;")
        count = cursor.fetchone()
        print(f"✓ Test table operations successful. Test records: {count[0]}")
        
        # Clean up test table
        cursor.execute("DROP TABLE test_table;")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✓ Database connection test passed!")
        return True
        
    except Exception as e:
        print(f"✗ Database connection test failed: {e}")
        return False

def connect_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Database connection successful!")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def find_json_files(folder_path):
    """Find all JSON files in the specified folder"""
    if not os.path.exists(folder_path):
        print(f"Folder does not exist: {folder_path}")
        return []
    
    # Support multiple JSON file patterns
    patterns = [
        os.path.join(folder_path, "*.json"),
        os.path.join(folder_path, "**/*.json"),  # Recursive search
    ]
    
    json_files = []
    for pattern in patterns:
        json_files.extend(glob.glob(pattern, recursive=True))
    
    # Remove duplicates and sort
    json_files = sorted(list(set(json_files)))
    
    print(f"Found {len(json_files)} JSON files in {folder_path}")
    for file in json_files:
        print(f"  - {file}")
    
    return json_files

def load_json_file(file_path):
    """Load and validate JSON file"""
    try:
        print(f"Loading file: {file_path}")
        file_size = os.path.getsize(file_path)
        print(f"File size: {file_size:,} bytes")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        # Validate data structure
        if not isinstance(json_data, list):
            print(f"Warning: {file_path} - Expected list, got {type(json_data)}")
            return None
        
        print(f"✓ Successfully loaded {len(json_data)} records from {file_path}")
        return json_data
        
    except FileNotFoundError:
        print(f"✗ File not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON in {file_path}: {e}")
        return None
    except Exception as e:
        print(f"✗ Error loading {file_path}: {e}")
        return None

def process_json_files(conn, folder_path):
    """Process all JSON files in the folder"""
    json_files = find_json_files(folder_path)
    
    if not json_files:
        print("No JSON files found to process.")
        return
    
    total_records = 0
    successful_files = 0
    failed_files = []
    
    print(f"\n{'='*60}")
    print(f"PROCESSING {len(json_files)} JSON FILES")
    print(f"{'='*60}")
    
    for idx, file_path in enumerate(json_files, 1):
        print(f"\n[{idx}/{len(json_files)}] Processing: {os.path.basename(file_path)}")
        print("-" * 50)
        
        try:
            # Load JSON data
            json_data = load_json_file(file_path)
            if json_data is None:
                failed_files.append(file_path)
                continue
            
            # Insert data into database
            records_before = get_record_count(conn)
            insert_json_data(conn, json_data, file_path)
            records_after = get_record_count(conn)
            
            records_added = records_after - records_before
            total_records += len(json_data)
            successful_files += 1
            
            print(f"✓ File processed successfully!")
            print(f"  Records in file: {len(json_data)}")
            print(f"  Records added to DB: {records_added}")
            print(f"  Total DB records: {records_after}")
            
        except Exception as e:
            print(f"✗ Error processing {file_path}: {e}")
            failed_files.append(file_path)
            continue
    
    # Summary
    print(f"\n{'='*60}")
    print("PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Total files found: {len(json_files)}")
    print(f"Successfully processed: {successful_files}")
    print(f"Failed files: {len(failed_files)}")
    print(f"Total records processed: {total_records}")
    
    if failed_files:
        print(f"\nFailed files:")
        for file in failed_files:
            print(f"  - {file}")

def get_record_count(conn):
    """Get current record count in airbnb_rooms table"""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM airbnb_rooms")
        count = cursor.fetchone()[0]
        return count
    except:
        return 0
    finally:
        cursor.close()

def insert_json_data(conn, json_data, source_file=None):
    cursor = conn.cursor()
    create_tables(conn)
    
    # Debug: Check if json_data is loaded correctly
    print(f"JSON data type: {type(json_data)}")
    print(f"JSON data length: {len(json_data) if isinstance(json_data, list) else 'Not a list'}")
    
    if source_file:
        print(f"Source file: {source_file}")
    
    # Print first item to see structure
    if isinstance(json_data, list) and len(json_data) > 0:
        print(f"First item keys: {json_data[0].keys() if isinstance(json_data[0], dict) else 'Not a dict'}")
        print(f"First item sample: {str(json_data[0])[:500]}...")
    
    insert_room_data(conn, json_data)
    cursor.close()

def create_tables(conn):
    """Create tables for Airbnb data - only if they don't exist"""
    cursor = conn.cursor()
    
    try:
        # FIXED: Changed latitude and longitude precision
        # From DECIMAL(10,8) to DECIMAL(11,8) - allows for 3 digits before decimal
        # This supports coordinates like -180.12345678 to 180.12345678
        create_rooms_table = """
        CREATE TABLE IF NOT EXISTS airbnb_rooms (
            id SERIAL PRIMARY KEY,
            room_id BIGINT UNIQUE NOT NULL,
            category VARCHAR(100),
            kind VARCHAR(50),
            name TEXT,
            title TEXT,
            type VARCHAR(50),
            rating_value DECIMAL(3,2),
            rating_review_count INTEGER,
            price_amount DECIMAL(10,2),
            price_qualifier VARCHAR(100),
            price_currency_symbol VARCHAR(10),
            latitude DECIMAL(11,8),
            longitude DECIMAL(11,8),
            badges TEXT[],
            raw_data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Images table (normalized)
        create_images_table = """
        CREATE TABLE IF NOT EXISTS room_images (
            id SERIAL PRIMARY KEY,
            room_id BIGINT REFERENCES airbnb_rooms(room_id),
            image_url TEXT NOT NULL,
            image_order INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Price breakdown table (normalized)
        create_price_breakdown_table = """
        CREATE TABLE IF NOT EXISTS price_breakdown (
            id SERIAL PRIMARY KEY,
            room_id BIGINT REFERENCES airbnb_rooms(room_id),
            description TEXT,
            amount DECIMAL(10,2),
            currency VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # File processing log table
        create_file_log_table = """
        CREATE TABLE IF NOT EXISTS file_processing_log (
            id SERIAL PRIMARY KEY,
            file_path TEXT NOT NULL,
            file_name TEXT NOT NULL,
            file_size BIGINT,
            records_count INTEGER,
            processing_status VARCHAR(20),
            error_message TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        cursor.execute(create_rooms_table)
        cursor.execute(create_images_table)
        cursor.execute(create_price_breakdown_table)
        cursor.execute(create_file_log_table)
        
        # Add missing columns to existing tables if they don't exist
        add_missing_columns(conn)
        
        conn.commit()
        print("✓ All tables created/verified successfully")
        
    except Exception as e:
        print(f"✗ Error creating tables: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

def add_missing_columns(conn):
    """Add missing columns to existing tables"""
    cursor = conn.cursor()
    
    # List of columns to add if they don't exist
    columns_to_add = [
        {
            'table': 'airbnb_rooms',
            'column': 'source_file',
            'definition': 'TEXT'
        },
        {
            'table': 'airbnb_rooms', 
            'column': 'updated_at',
            'definition': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        }
    ]
    
    for col_info in columns_to_add:
        try:
            # Check if column exists
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND column_name = %s
            """, (col_info['table'], col_info['column']))
            
            if not cursor.fetchone():
                # Column doesn't exist, add it
                alter_query = f"ALTER TABLE {col_info['table']} ADD COLUMN {col_info['column']} {col_info['definition']}"
                cursor.execute(alter_query)
                print(f"✓ Added column {col_info['column']} to {col_info['table']}")
            else:
                print(f"✓ Column {col_info['column']} already exists in {col_info['table']}")
                
        except Exception as e:
            print(f"Warning: Could not add column {col_info['column']} to {col_info['table']}: {e}")
    
    cursor.close()

def log_file_processing(conn, file_path, records_count, status, error_message=None):
    """Log file processing status"""
    cursor = conn.cursor()
    try:
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        
        cursor.execute("""
            INSERT INTO file_processing_log 
            (file_path, file_name, file_size, records_count, processing_status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (file_path, file_name, file_size, records_count, status, error_message))
        
        conn.commit()
    except Exception as e:
        print(f"Warning: Could not log file processing: {e}")
    finally:
        cursor.close()

def insert_room_data(conn, rooms_data):
    """Insert room data into the database"""
    cursor = conn.cursor()
    
    # Check if rooms_data is actually a list
    if not isinstance(rooms_data, list):
        print(f"Expected list, got {type(rooms_data)}")
        return
    
    print(f"Processing {len(rooms_data)} rooms...")
    successful_inserts = 0
    
    for idx, room in enumerate(rooms_data):
        try:
            if idx % 50 == 0:  # Progress indicator
                print(f"Processing room {idx + 1}/{len(rooms_data)}")
            
            # Extract main room data
            room_id = room.get('room_id')
            if not room_id:
                print(f"Skipping room {idx}: No room_id found")
                continue
            
            category = room.get('category')
            kind = room.get('kind')
            name = room.get('name')
            title = room.get('title')
            room_type = room.get('type')
            
            # Extract rating data
            rating = room.get('rating', {})
            rating_value = rating.get('value')
            rating_review_count = None
            if rating.get('reviewCount'):
                try:
                    rating_review_count = int(rating.get('reviewCount'))
                except:
                    rating_review_count = None
            
            # Extract price data
            price = room.get('price', {})
            unit_price = price.get('unit', {})
            price_amount = unit_price.get('amount')
            price_qualifier = unit_price.get('qualifier')
            price_currency = unit_price.get('currency_symbol', unit_price.get('curency_symbol', ''))
            
            # Extract coordinates
            coordinates = room.get('coordinates', {})
            latitude = coordinates.get('latitude')
            longitude = coordinates.get('longitude', coordinates.get('longitud'))
            
            # Extract badges
            badges = room.get('badges', [])
            
            # Check if updated_at column exists
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'airbnb_rooms' AND column_name = 'updated_at'
            """)
            has_updated_at = cursor.fetchone() is not None
            
            # Insert main room record - with or without updated_at column
            if has_updated_at:
                insert_room_query = """
                INSERT INTO airbnb_rooms (
                    room_id, category, kind, name, title, type,
                    rating_value, rating_review_count, price_amount, 
                    price_qualifier, price_currency_symbol, latitude, longitude,
                    badges, raw_data, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (room_id) DO UPDATE SET
                    category = EXCLUDED.category,
                    kind = EXCLUDED.kind,
                    name = EXCLUDED.name,
                    title = EXCLUDED.title,
                    type = EXCLUDED.type,
                    rating_value = EXCLUDED.rating_value,
                    rating_review_count = EXCLUDED.rating_review_count,
                    price_amount = EXCLUDED.price_amount,
                    price_qualifier = EXCLUDED.price_qualifier,
                    price_currency_symbol = EXCLUDED.price_currency_symbol,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    badges = EXCLUDED.badges,
                    raw_data = EXCLUDED.raw_data,
                    updated_at = EXCLUDED.updated_at
                """
                
                cursor.execute(insert_room_query, (
                    room_id, category, kind, name, title, room_type,
                    rating_value, rating_review_count, price_amount,
                    price_qualifier, price_currency, latitude, longitude,
                    badges, json.dumps(room), datetime.now()
                ))
            else:
                # Fallback for existing schema without updated_at column
                insert_room_query = """
                INSERT INTO airbnb_rooms (
                    room_id, category, kind, name, title, type,
                    rating_value, rating_review_count, price_amount, 
                    price_qualifier, price_currency_symbol, latitude, longitude,
                    badges, raw_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (room_id) DO UPDATE SET
                    category = EXCLUDED.category,
                    kind = EXCLUDED.kind,
                    name = EXCLUDED.name,
                    title = EXCLUDED.title,
                    type = EXCLUDED.type,
                    rating_value = EXCLUDED.rating_value,
                    rating_review_count = EXCLUDED.rating_review_count,
                    price_amount = EXCLUDED.price_amount,
                    price_qualifier = EXCLUDED.price_qualifier,
                    price_currency_symbol = EXCLUDED.price_currency_symbol,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    badges = EXCLUDED.badges,
                    raw_data = EXCLUDED.raw_data
                """
                
                cursor.execute(insert_room_query, (
                    room_id, category, kind, name, title, room_type,
                    rating_value, rating_review_count, price_amount,
                    price_qualifier, price_currency, latitude, longitude,
                    badges, json.dumps(room)
                ))
            
            # Delete existing images and price breakdowns for this room before inserting new ones
            cursor.execute("DELETE FROM room_images WHERE room_id = %s", (room_id,))
            cursor.execute("DELETE FROM price_breakdown WHERE room_id = %s", (room_id,))
            
            # Insert images
            images = room.get('images', [])
            for idx_img, image in enumerate(images):
                if isinstance(image, dict) and image.get('url'):
                    insert_image_query = """
                    INSERT INTO room_images (room_id, image_url, image_order)
                    VALUES (%s, %s, %s)
                    """
                    cursor.execute(insert_image_query, (room_id, image.get('url'), idx_img + 1))
            
            # Insert price breakdown
            price_breakdown = price.get('break_down', [])
            for breakdown in price_breakdown:
                if isinstance(breakdown, dict):
                    insert_breakdown_query = """
                    INSERT INTO price_breakdown (room_id, description, amount, currency)
                    VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(insert_breakdown_query, (
                        room_id,
                        breakdown.get('description'),
                        breakdown.get('amount'),
                        breakdown.get('currency')
                    ))
            
            successful_inserts += 1
            
        except Exception as e:
            print(f"✗ Error inserting room {room.get('room_id', f'index-{idx}')}: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    cursor.close()
    print(f"Successfully inserted {successful_inserts} out of {len(rooms_data)} rooms")

def verify_data(conn):
    """Verify that data was inserted"""
    cursor = conn.cursor()
    
    try:
        # Count total rooms
        cursor.execute("SELECT COUNT(*) FROM airbnb_rooms")
        room_count = cursor.fetchone()[0]
        print(f"Total rooms in database: {room_count}")
        
        # Count images
        cursor.execute("SELECT COUNT(*) FROM room_images")
        image_count = cursor.fetchone()[0]
        print(f"Total images in database: {image_count}")
        
        # Count price breakdowns
        cursor.execute("SELECT COUNT(*) FROM price_breakdown")
        breakdown_count = cursor.fetchone()[0]
        print(f"Total price breakdowns in database: {breakdown_count}")
        
        # Show file processing log
        cursor.execute("SELECT COUNT(*) FROM file_processing_log")
        log_count = cursor.fetchone()[0]
        print(f"Total files processed (logged): {log_count}")
        
        # Show sample data
        cursor.execute("SELECT room_id, name, price_amount, latitude, longitude, created_at FROM airbnb_rooms LIMIT 3")
        sample_rooms = cursor.fetchall()
        print(f"\nSample rooms:")
        for room in sample_rooms:
            print(f"  Room ID: {room[0]}, Name: {room[1][:30]}..., Price: {room[2]}, Coordinates: ({room[3]}, {room[4]})")
            
    except Exception as e:
        print(f"Error verifying data: {e}")
    finally:
        cursor.close()

def main():
    # Configuration
    JSON_FOLDER = "./results"  # Change this to your folder path
    
    print("=" * 60)
    print("AIRBNB JSON BULK PROCESSOR")
    print("=" * 60)
    print(f"Target folder: {JSON_FOLDER}")
    print(f"Database: {DB_CONFIG['dbname']}")
    
    # Test database connection first
    if not test_database_connection():
        print("Database connection failed. Please check your configuration.")
        return
    
    conn = connect_db()
    if conn is None:
        return
    
    try:
        # Create tables if they don't exist
        create_tables(conn)
        
        # Process all JSON files in the folder
        process_json_files(conn, JSON_FOLDER)
        
        # Final verification
        print(f"\n{'='*60}")
        print("FINAL DATABASE STATE")
        print(f"{'='*60}")
        verify_data(conn)
        
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    main()