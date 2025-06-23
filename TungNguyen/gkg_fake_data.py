import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
import random
import hashlib
import json
from pathlib import Path

class UnifiedGKGDataManager:
    def __init__(self):
        # Current system info - Updated to latest
        self.current_time = "2025-06-18 06:27:22"
        self.user_login = "strawberrymilktea0604"
        
        # Directories
        self.source_dir = f"{self.user_login}_april_june_2025"
        self.target_dirs = ["April", "May", "June"]
        
        # Date range: April 1, 2025 to June 10, 2025
        self.start_date = datetime(2025, 4, 1)
        self.end_date = datetime(2025, 6, 10)
        
        # File format: YYYYMMDD.gkg.csv
        self.file_extension = ".gkg.csv"
        
        # Duplicate detection columns
        self.key_columns = ['DATE', 'THEMES', 'LOCATIONS', 'PERSONS', 'ORGANIZATIONS', 'SOURCES']
        
        # Initialize realistic data pools based on the sample
        self.init_realistic_data_pools()
        
        # Statistics tracking
        self.stats = {
            'generation': {
                'total_files': 0,
                'total_rows': 0,
                'total_size_mb': 0
            },
            'merge': {
                'total_processed': 0,
                'total_added': 0,
                'total_duplicates_skipped': 0,
                'by_month': {}
            }
        }

    def init_realistic_data_pools(self):
        """Initialize realistic data pools based on the sample data format"""
        
        # Themes based on the sample - realistic GKG themes
        self.themes_pool = [
            "TRIAL;TAX_FNCACT;TAX_FNCACT_LAWYER;TAX_FNCACT_CANDIDATE;TAX_FNCACT_HAIRDRESSER;BAN;CRISISLEX_CRISISLEXREC;MEDIA_MSM;AFFECT;WB_698",
            "PORTSMEN_HOLIDAY;CRISISLEX_CRISISLEXREC;SOC_POINTSOFINTEREST;SOC_POINTSOFINTEREST_SCHOOL;WB_2936_GOLD;WB_507_ENERGY_AND_EXTRACTIVES;WB_895_MINING_SYSTEMS",
            "WB_135_TRANSPORT;WB_1803_TRANSPORT_INFRASTRUCTURE;WB_167;METAL_ORE_MINING;TAX_FNCACT;TAX_FNCACT_KING;WB_137_WATER;UNGP_FORESTS_RIVERS_OCEANS",
            "WB_1979_NATURAL_RESOURCE_MANAGEMENT;WB_435_AGRICULTURE_AND_FOOD_SECURITY;WB_1986_MOUNTAINS;SOC_POINTSOFINTEREST_AIRPORT;WB_1884_AIRPORTS_TOURISM",
            "WB_826_TOURISM;WB_1921_COMPETITIVE_AND_REAL_SECTORS;WB_INDUSTRY_POLICY;WB_471_ECONOMIC_GROWTH;WB_1078_DETERMINANTS_OF_GROWTH;WB_2670_JOBS",
            "ARREST;TAX_FNCACT;TAX_FNCACT_OFFICIALS;TRIAL;TAX_FNCACT_ATTORNEY;TAX_ETHNICITY;TAX_ETHNICITY_VENEZUELAN;CRIME;WB_ILLEGAL_DRUGS;SECURITY_SERVICES",
            "TAX_FNCACT_POLICE;SOC_POINTSOFINTEREST_PRISON;WB_2405_DETENTION_REFORM;WB_2470_PEACE_OPERATIONS_AND_CONFLICT_MANAGEMENT;WB_2490_NATIONAL_PROTECTION_AND_SECURITY",
            "TERROR;ARMEDCONFLICT;TAX_ETHNICITY_VENEZUELANS;USPEC_POLICY1;EPU_ECONOMY;EPU_ECONOMY_HISTORIC;TAX_ETHNICITY_SPANISH;TAX_WORLDLANGUAGES;TAX_WORLDLANGUAGES_SPANISH",
            "PUBLIC_TRANSPORT;WB_135_TRANSPORT;WB_1803_TRANSPORT_INFRASTRUCTURE;WB_166;EDUCATION;WB_470_EDUCATION;WB_1467_EDUCATION_FOR_ALL;WB_2131_EMPLOYABILITY_SKILLS",
            "HEALTH_NUTRITION_AND_POPULATION;WB_2456_DRUGS_AND_NARCOTICS;TAX_FNCACT;TAX_FNCACT_OFFICIALS;TRIAL;TAX_FNCACT_ATTORNEY;TAX_ETHNICITY"
        ]
        
        # Locations based on sample - realistic GKG location format
        self.locations_pool = [
            "4#Sydney, New South Wales, Australia#AS#AS02#-33.8683#151.217#-1603135",
            "4#Houston, Texas, United States#US#USTX#29.7633#-95.3633#1380948",
            "3#New Orleans, Louisiana, United States#US#USLA#29.9546#-90.0751#1629985",
            "4#Baton Rouge, Louisiana, United States#US#USLA#30.4507#-91.1545#1629914",
            "3#Hollywood, California, United States#US#USCA#34.0983#-118.327#1660757",
            "4#Brisbane, Queensland, Australia#AS#AS04#-27.5415#153.017#-1567128",
            "4#Melbourne, Victoria, Australia#AS#AS07#-37.8136#144.9631#-1581729",
            "4#Perth, Western Australia, Australia#AS#AS08#-31.9522#115.8614#-1607549",
            "3#Coffs Harbour, New South Wales, Australia#AS#AS02#-30.2963#153.114#-1566281",
            "4#Daintree, Queensland, Australia#AS#AS04#-16.254#145.317#-1568710",
            "4#Darwin, Northern Territory, Australia#AS#AS03#-12.4634#130.8456#-1643874",
            "4#Adelaide, South Australia, Australia#AS#AS05#-34.9285#138.6007#-2077963",
            "4#Hobart, Tasmania, Australia#AS#AS06#-42.8821#147.3272#-2147714",
            "3#Los Angeles, California, United States#US#USCA#34.0522#-118.2437#1840020",
            "4#Miami, Florida, United States#US#USFL#25.7617#-80.1918#1525116"
        ]
        
        # Persons - realistic names from sample
        self.persons_pool = [
            "nicolette boele;carla mascarenhas;paul fletcher;gisele kapterian",
            "kaley cuoco;tom pelphrey;sarah king;scott olson",
            "mike pence;donald trump;joe biden;kamala harris",
            "david wilson;mary johnson;robert smith;jennifer garcia",
            "michael davis;sarah brown;christopher martinez;amanda rodriguez",
            "matthew hernandez;emily moore;daniel jackson;ashley white",
            "joshua harris;nicole clark;andrew lewis;stephanie walker",
            "ryan hall;michelle young;kevin king;rebecca wright",
            "brandon lopez;rachel hill;jacob green;samantha adams",
            "tyler baker;lauren nelson;alexis murphy;jordan taylor"
        ]
        
        # Organizations - realistic from sample
        self.organizations_pool = [
            "facebook;nicolette boele;instagram;thenews.com.pk",
            "national college;health care;division titles",
            "facebook;goulburn post;accommodation",
            "houston police;venezuelan nationals;crime groups",
            "peace operations;conflict management;national protection",
            "ministry of health;department of education;world health organization",
            "chamber of commerce;trade union;professional association",
            "research institute;technology corporation;media group",
            "environmental agency;transport authority;tourism board",
            "university system;education board;academic council"
        ]
        
        # Sources - realistic domains from sample
        self.sources_pool = [
            "singletonargus.com.au", "goulburnpost.com.au", "710keel.com", "thenews.com.pk",
            "reuters.com", "bbc.com", "cnn.com", "guardian.com", "nytimes.com",
            "washingtonpost.com", "bloomberg.com", "wsj.com", "ap.org", "abc.com",
            "reuters.co.uk", "france24.com", "dw.com", "aljazeera.com", "sky.com",
            "independent.co.uk", "telegraph.co.uk", "economist.com", "forbes.com", "time.com"
        ]
        
        # Source URLs - realistic patterns from sample
        self.source_url_patterns = [
            "https://www.singletonargus.com.au/story/{}/teal-candidate-apologises-for-sexual-joke-at-sydney-salon/?cs={}",
            "https://www.goulburnpost.com.au/story/{}/where-you-can-find-cheap-accommodation-for-easter-school-holidays/?cs={}",
            "https://710keel.com/ixp/182/p/shreveport-most-unhealthy-city-louisiana-study/",
            "https://www.thenews.com.pk/latest/{}/",
            "https://www.reuters.com/world/article/{}.html",
            "https://www.bbc.com/news/world-{}-{}",
            "https://edition.cnn.com/{}/world/news-{}.html",
            "https://www.theguardian.com/world/{}/{}",
            "https://www.nytimes.com/{}/world/{}/"
        ]

    def get_filename(self, date_str):
        """Get filename in format YYYYMMDD.gkg.csv"""
        return f"{date_str}{self.file_extension}"

    def ensure_directory(self, directory):
        """Create directory if it doesn't exist"""
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"📁 Created directory: {directory}")

    def create_metadata_file(self, directory):
        """Create metadata file with generation info"""
        metadata_file = os.path.join(directory, "generation_metadata.txt")
        with open(metadata_file, "w", encoding="utf-8") as f:
            f.write(f"GKG News Dataset Generation Metadata\n")
            f.write(f"=" * 50 + "\n")
            f.write(f"Generated by: {self.user_login}\n")
            f.write(f"Generation time (UTC): {self.current_time}\n")
            f.write(f"Date range: April 1, 2025 - June 10, 2025\n")
            f.write(f"File format: YYYYMMDD.gkg.csv (GKG format)\n")
            f.write(f"NUMARTS: Fixed at 1 (as per sample specification)\n")
            f.write(f"Purpose: Expand Kaggle dataset from 7GB to 10GB\n")
            f.write(f"Original dataset: https://www.kaggle.com/datasets/nguynlmtng/topic-news\n")
            f.write(f"Output directory: {directory}\n")
            f.write(f"Data format: Follows GKG (Global Knowledge Graph) specification\n")

    # === GKG DATA GENERATION FUNCTIONS ===
    
    def generate_tone_values(self):
        """Generate realistic tone values (6 comma-separated floats)"""
        # Based on sample: -0.3134796238451,7.61755485893,4.07523510971787,7.83690959561129,20.6896551724138,1.56739811912226
        values = []
        values.append(round(random.uniform(-5, 5), 13))      # Tone 1: usually small positive/negative
        values.append(round(random.uniform(5, 15), 11))      # Tone 2: usually positive
        values.append(round(random.uniform(0, 10), 12))      # Tone 3: usually positive
        values.append(round(random.uniform(5, 15), 11))      # Tone 4: usually positive
        values.append(round(random.uniform(15, 30), 10))     # Tone 5: usually higher positive
        values.append(round(random.uniform(0, 5), 14))       # Tone 6: usually small positive
        
        return ",".join(map(str, values))

    def generate_cameo_event_ids(self):
        """Generate realistic CAMEO event IDs"""
        # Based on sample: multiple IDs separated by commas
        num_ids = random.randint(3, 8)
        ids = []
        for _ in range(num_ids):
            # Generate realistic CAMEO event IDs (10 digits starting with 123-125)
            base = random.choice([1234, 1235, 1236, 1237, 1238])
            event_id = f"{base}{random.randint(100000, 999999)}"
            ids.append(event_id)
        
        return ",".join(ids)

    def generate_source_url(self, source):
        """Generate realistic source URL based on patterns"""
        pattern = random.choice(self.source_url_patterns)
        
        if "story" in pattern:
            story_id = random.randint(8000000, 9999999)
            cs_id = random.randint(100, 9999)
            return pattern.format(story_id, cs_id)
        elif "ixp" in pattern:
            return pattern
        elif "latest" in pattern:
            article_id = random.randint(1000000, 9999999)
            return pattern.format(article_id)
        else:
            # For other patterns
            id1 = random.randint(100000, 999999)
            id2 = random.randint(1000, 9999)
            return pattern.format(id1, id2)

    def generate_counts_data(self):
        """Generate COUNTS column data in GKG format"""
        if random.random() < 0.4:  # 40% chance of having counts data
            # Format: ACTION#CODE#LOCATION;
            actions = ["ARREST", "TRIAL", "PORTSMEN_HOLIDAY", "AFFECT"]
            action = random.choice(actions)
            
            # Generate location-style data
            location_codes = [
                "alleged members#3#Houston, Texas, United States#US#USTX#29.7633#-95.3633#1380948",
                "health care#1#Sydney, New South Wales, Australia#AS#AS02#-33.8683#151.217#-1603135",
                "national college#2#California, United States#US#USCA#34.0983#-118.327#1660757"
            ]
            
            location = random.choice(location_codes)
            return f"{action}#{location};"
        return ""

    def generate_gkg_row(self, date):
        """Generate a single row of GKG data following the exact format"""
        return {
            'DATE': date,
            'NUMARTS': 1,  # Fixed at 1 as per sample specification
            'COUNTS': self.generate_counts_data(),
            'THEMES': random.choice(self.themes_pool),
            'LOCATIONS': ";".join(random.sample(self.locations_pool, random.randint(2, 6))),
            'PERSONS': random.choice(self.persons_pool),
            'ORGANIZATIONS': random.choice(self.organizations_pool),
            'TONE': self.generate_tone_values(),
            'CAMEOEVENTIDS': self.generate_cameo_event_ids(),
            'SOURCES': random.choice(self.sources_pool),
            'SOURCEURLS': self.generate_source_url(random.choice(self.sources_pool))
        }

    def get_date_list(self):
        """Get list of all dates in the range"""
        dates = []
        current_date = self.start_date
        
        while current_date <= self.end_date:
            dates.append(current_date.strftime("%Y%m%d"))
            current_date += timedelta(days=1)
        
        return dates

    def calculate_rows_for_target_size(self, target_gb=3):
        """Calculate rows needed to reach target size"""
        dates = self.get_date_list()
        num_days = len(dates)
        
        # GKG data is typically larger per row due to long text fields
        # Estimate: 2KB per row for GKG format
        target_mb = target_gb * 1024
        total_rows_needed = int((target_mb * 1024) / 2.0)  # 2KB per row
        rows_per_day = total_rows_needed // num_days
        
        print(f"📅 Date range: {self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}")
        print(f"📊 Total days: {num_days}")
        print(f"🎯 Target size: {target_gb} GB")
        print(f"📈 Estimated rows per day: {rows_per_day:,} (GKG format)")
        print(f"✅ NUMARTS: Fixed at 1 (as per specification)")
        
        return rows_per_day, num_days

    def generate_gkg_daily_files(self, target_gb=3):
        """Generate daily GKG .csv files for April-June 2025"""
        print(f"\n🚀 Starting GKG data generation...")
        print(f"👤 User: {self.user_login}")
        print(f"⏰ Current time (UTC): {self.current_time}")
        print(f"📁 File format: YYYYMMDD.gkg.csv (GKG specification)")
        print(f"✅ NUMARTS: Fixed at 1")
        
        # Ensure source directory exists
        self.ensure_directory(self.source_dir)
        self.create_metadata_file(self.source_dir)
        
        rows_per_day, num_days = self.calculate_rows_for_target_size(target_gb)
        dates = self.get_date_list()
        
        total_rows = 0
        total_size_mb = 0
        progress_log = []
        
        print(f"\n📝 Generating {num_days} daily GKG files...")
        
        for i, date_str in enumerate(dates):
            # Add daily variation (±15% for more realistic distribution)
            daily_rows = rows_per_day + random.randint(-int(rows_per_day*0.15), int(rows_per_day*0.15))
            
            # Generate GKG data for this day
            daily_data = []
            for _ in range(daily_rows):
                row = self.generate_gkg_row(date_str)
                daily_data.append(row)
            
            # Create DataFrame and save with .gkg.csv extension
            df_daily = pd.DataFrame(daily_data)
            filename = self.get_filename(date_str)
            output_file = os.path.join(self.source_dir, filename)
            
            # Save with tab separation to match GKG format better
            df_daily.to_csv(output_file, index=False, sep='\t')
            
            # Calculate file size
            file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
            total_rows += len(df_daily)
            total_size_mb += file_size_mb
            
            # Log progress
            progress_log.append({
                'date': date_str,
                'filename': filename,
                'rows': len(df_daily),
                'numarts_fixed': 1,  # Log that NUMARTS is fixed at 1
                'size_mb': file_size_mb,
                'cumulative_rows': total_rows,
                'cumulative_size_mb': total_size_mb
            })
            
            # Progress update every 7 days
            if (i + 1) % 7 == 0 or i == len(dates) - 1:
                progress = ((i + 1) / len(dates)) * 100
                print(f"⏳ Progress: {progress:.1f}% - Generated {i+1}/{len(dates)} GKG files")
                print(f"   Latest: {filename} ({len(df_daily):,} rows, NUMARTS=1, {file_size_mb:.2f} MB)")
        
        # Save progress log
        progress_df = pd.DataFrame(progress_log)
        progress_file = os.path.join(self.source_dir, "gkg_generation_progress.csv")
        progress_df.to_csv(progress_file, index=False)
        
        # Update stats
        self.stats['generation']['total_files'] = len(dates)
        self.stats['generation']['total_rows'] = total_rows
        self.stats['generation']['total_size_mb'] = total_size_mb
        
        print(f"\n✅ GKG Generation Complete!")
        print(f"📁 Files created: {len(dates)} .gkg.csv files")
        print(f"📊 Total rows: {total_rows:,}")
        print(f"✅ NUMARTS: Fixed at 1 for all rows")
        print(f"💾 Total size: {total_size_mb:.2f} MB ({total_size_mb/1024:.2f} GB)")
        print(f"📋 Format: GKG (Global Knowledge Graph) specification")
        
        return total_rows, total_size_mb

    # === DUPLICATE DETECTION AND MERGE FUNCTIONS ===
    
    def create_row_hash(self, row):
        """Create a hash for a row based on key columns for duplicate detection"""
        key_values = []
        for col in self.key_columns:
            if col in row and pd.notna(row[col]):
                key_values.append(str(row[col]).strip())
            else:
                key_values.append("")
        
        combined = "|".join(key_values)
        return hashlib.md5(combined.encode('utf-8')).hexdigest()

    def get_month_from_date(self, date_str):
        """Get month name from date string (YYYYMMDD)"""
        month_num = int(date_str[4:6])
        month_names = {4: "April", 5: "May", 6: "June"}
        return month_names.get(month_num, "Unknown")

    def load_existing_data(self, target_file):
        """Load existing data from target file and create hash set"""
        if os.path.exists(target_file):
            try:
                # Try both tab and comma separation
                try:
                    existing_df = pd.read_csv(target_file, sep='\t')
                except:
                    existing_df = pd.read_csv(target_file)
                    
                existing_hashes = set()
                
                for _, row in existing_df.iterrows():
                    row_hash = self.create_row_hash(row)
                    existing_hashes.add(row_hash)
                
                # Verify NUMARTS is 1
                if 'NUMARTS' in existing_df.columns:
                    numarts_values = existing_df['NUMARTS'].unique()
                    if not all(val == 1 for val in numarts_values):
                        print(f"⚠️  Warning: Found NUMARTS values other than 1: {numarts_values}")
                
                print(f"📂 Loaded {len(existing_df)} existing rows from {os.path.basename(target_file)}")
                return existing_df, existing_hashes
            except Exception as e:
                print(f"⚠️  Error loading {target_file}: {e}")
                return pd.DataFrame(), set()
        else:
            print(f"📄 Target file {os.path.basename(target_file)} doesn't exist - will create new")
            return pd.DataFrame(), set()

    def merge_gkg_file_intelligently(self, source_file, target_file):
        """Merge source GKG file into target file with duplicate detection"""
        print(f"\n🔄 Processing GKG file: {os.path.basename(source_file)}")
        
        # Load source data
        try:
            # Try tab separation first (GKG format)
            try:
                source_df = pd.read_csv(source_file, sep='\t')
            except:
                source_df = pd.read_csv(source_file)
                
            print(f"📊 Source rows: {len(source_df)}")
            
            # Verify NUMARTS is 1 in source
            if 'NUMARTS' in source_df.columns:
                numarts_values = source_df['NUMARTS'].unique()
                if not all(val == 1 for val in numarts_values):
                    print(f"⚠️  Warning: Source has NUMARTS values other than 1: {numarts_values}")
                else:
                    print(f"✅ Source NUMARTS verified: all values = 1")
                    
        except Exception as e:
            print(f"❌ Error loading source file: {e}")
            return 0, 0
        
        # Load existing target data
        existing_df, existing_hashes = self.load_existing_data(target_file)
        
        # Check for duplicates and filter
        new_rows = []
        duplicates_count = 0
        
        for _, row in source_df.iterrows():
            row_hash = self.create_row_hash(row)
            
            if row_hash not in existing_hashes:
                new_rows.append(row)
                existing_hashes.add(row_hash)
            else:
                duplicates_count += 1
        
        # Create merged dataframe
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            
            if len(existing_df) > 0:
                merged_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                merged_df = new_df
            
            # Sort by DATE
            merged_df = merged_df.sort_values('DATE').reset_index(drop=True)
            
            # Save merged data with tab separation (GKG format)
            merged_df.to_csv(target_file, index=False, sep='\t')
            
            print(f"✅ Added {len(new_rows)} new rows")
            print(f"🔄 Skipped {duplicates_count} duplicates")
            print(f"📊 Total rows in target: {len(merged_df)}")
            print(f"✅ All rows maintain NUMARTS = 1")
            
            return len(new_rows), duplicates_count
        else:
            print(f"⚠️  No new rows to add (all {duplicates_count} were duplicates)")
            return 0, duplicates_count

    def intelligent_merge_to_monthly_dirs(self):
        """Merge generated GKG files to monthly directories with deduplication"""
        print(f"\n🔗 Starting intelligent GKG merge to monthly directories...")
        print(f"👤 User: {self.user_login}")
        print(f"⏰ Time: {self.current_time}")
        print(f"📁 File format: YYYYMMDD.gkg.csv (GKG specification)")
        print(f"✅ NUMARTS: Maintaining fixed value of 1")
        
        # Ensure target directories exist
        for target_dir in self.target_dirs:
            self.ensure_directory(target_dir)
        
        # Initialize monthly stats
        for month in self.target_dirs:
            self.stats['merge']['by_month'][month] = {
                'files_processed': 0,
                'rows_added': 0,
                'duplicates_skipped': 0
            }
        
        # Get all .gkg.csv files from source directory
        source_files = []
        if os.path.exists(self.source_dir):
            for filename in os.listdir(self.source_dir):
                if filename.endswith(self.file_extension):
                    source_files.append(filename)
        
        source_files.sort()
        print(f"\n📋 Found {len(source_files)} GKG files to process")
        
        if not source_files:
            print(f"❌ No .gkg.csv files found in {self.source_dir}")
            return
        
        # Process each file
        for i, filename in enumerate(source_files):
            source_file = os.path.join(self.source_dir, filename)
            date_str = filename.replace(self.file_extension, '')
            
            # Determine target month
            month = self.get_month_from_date(date_str)
            if month not in self.target_dirs:
                print(f"⚠️  Skipping {filename} - month {month} not in target directories")
                continue
            
            # Target file path
            target_file = os.path.join(month, filename)
            
            # Merge with duplicate detection
            added_rows, skipped_duplicates = self.merge_gkg_file_intelligently(source_file, target_file)
            
            # Update statistics
            self.stats['merge']['total_processed'] += 1
            self.stats['merge']['total_added'] += added_rows
            self.stats['merge']['total_duplicates_skipped'] += skipped_duplicates
            
            self.stats['merge']['by_month'][month]['files_processed'] += 1
            self.stats['merge']['by_month'][month]['rows_added'] += added_rows
            self.stats['merge']['by_month'][month]['duplicates_skipped'] += skipped_duplicates
            
            # Progress update
            if (i + 1) % 7 == 0 or i == len(source_files) - 1:
                progress = ((i + 1) / len(source_files)) * 100
                print(f"📈 Progress: {progress:.1f}% ({i+1}/{len(source_files)} GKG files)")

    # === ANALYSIS AND REPORTING FUNCTIONS ===
    
    def analyze_gkg_directory_structure(self):
        """Analyze current GKG directory structure"""
        print(f"\n📂 GKG DIRECTORY STRUCTURE ANALYSIS")
        print(f"👤 User: {self.user_login}")
        print(f"⏰ Time: {self.current_time}")
        print(f"📁 Format: GKG (Global Knowledge Graph)")
        print(f"✅ NUMARTS: Fixed at 1")
        print("="*60)
        
        # Check source directory
        if os.path.exists(self.source_dir):
            source_files = [f for f in os.listdir(self.source_dir) if f.endswith(self.file_extension)]
            source_files.sort()
            
            print(f"📁 Source Directory: {self.source_dir}")
            print(f"   GKG files found: {len(source_files)}")
            
            if source_files:
                # Calculate total size
                total_size_mb = sum(os.path.getsize(os.path.join(self.source_dir, f)) 
                                  for f in source_files) / (1024 * 1024)
                
                # Sample first file to show structure and verify NUMARTS
                sample_file = os.path.join(self.source_dir, source_files[0])
                try:
                    try:
                        sample_df = pd.read_csv(sample_file, sep='\t', nrows=5)
                    except:
                        sample_df = pd.read_csv(sample_file, nrows=5)
                    
                    print(f"   Total size: {total_size_mb:.2f} MB")
                    print(f"   Sample columns: {list(sample_df.columns)}")
                    print(f"   Sample row count: {len(sample_df)}")
                    
                    # Verify NUMARTS
                    if 'NUMARTS' in sample_df.columns:
                        numarts_values = sample_df['NUMARTS'].unique()
                        print(f"   ✅ NUMARTS values in sample: {numarts_values} (should be [1])")
                    
                    # Monthly breakdown
                    monthly_count = {"April": 0, "May": 0, "June": 0}
                    for filename in source_files:
                        date_str = filename.replace(self.file_extension, '')
                        month = self.get_month_from_date(date_str)
                        if month in monthly_count:
                            monthly_count[month] += 1
                    
                    print(f"   April files: {monthly_count['April']}")
                    print(f"   May files: {monthly_count['May']}")
                    print(f"   June files: {monthly_count['June']}")
                    print(f"   First file: {source_files[0]}")
                    print(f"   Last file: {source_files[-1]}")
                    
                except Exception as e:
                    print(f"   ⚠️  Error reading sample: {e}")
        else:
            print(f"❌ Source directory not found: {self.source_dir}")
        
        # Check target directories
        print(f"\n📁 Target Directories:")
        for target_dir in self.target_dirs:
            if os.path.exists(target_dir):
                csv_files = [f for f in os.listdir(target_dir) if f.endswith(self.file_extension)]
                total_size_mb = sum(os.path.getsize(os.path.join(target_dir, f)) 
                                  for f in csv_files) / (1024 * 1024)
                print(f"   📂 {target_dir}/: {len(csv_files)} GKG files, {total_size_mb:.2f} MB")
            else:
                print(f"   📂 {target_dir}/: Directory not found")

    def validate_gkg_data_integrity(self):
        """Validate the integrity of GKG data across all directories"""
        print(f"\n🔍 GKG DATA INTEGRITY VALIDATION")
        print(f"✅ Special focus on NUMARTS = 1 validation")
        print("="*50)
        
        validation_results = {}
        
        # Check source directory
        if os.path.exists(self.source_dir):
            source_files = [f for f in os.listdir(self.source_dir) if f.endswith(self.file_extension)]
            validation_results['source'] = {
                'directory': self.source_dir,
                'files': len(source_files),
                'total_rows': 0,
                'format_issues': 0,
                'column_consistency': True,
                'numarts_valid': True,
                'numarts_issues': 0
            }
            
            expected_columns = ['DATE', 'NUMARTS', 'COUNTS', 'THEMES', 'LOCATIONS', 'PERSONS', 'ORGANIZATIONS', 'TONE', 'CAMEOEVENTIDS', 'SOURCES', 'SOURCEURLS']
            
            for filename in source_files[:5]:  # Check first 5 files
                file_path = os.path.join(self.source_dir, filename)
                try:
                    # Try both tab and comma separation
                    try:
                        df = pd.read_csv(file_path, sep='\t')
                    except:
                        df = pd.read_csv(file_path)
                    
                    validation_results['source']['total_rows'] += len(df)
                    
                    # Check column consistency
                    if list(df.columns) != expected_columns:
                        validation_results['source']['column_consistency'] = False
                        validation_results['source']['format_issues'] += 1
                    
                    # Check NUMARTS = 1
                    if 'NUMARTS' in df.columns:
                        numarts_values = df['NUMARTS'].unique()
                        non_one_values = [v for v in numarts_values if v != 1]
                        if non_one_values:
                            validation_results['source']['numarts_valid'] = False
                            validation_results['source']['numarts_issues'] += len(non_one_values)
                            print(f"⚠️  {filename}: Found NUMARTS values other than 1: {non_one_values}")
                        
                except Exception as e:
                    validation_results['source']['format_issues'] += 1
                    print(f"⚠️  Error validating {filename}: {e}")
        
        # Check target directories
        for target_dir in self.target_dirs:
            if os.path.exists(target_dir):
                csv_files = [f for f in os.listdir(target_dir) if f.endswith(self.file_extension)]
                
                validation_results[target_dir.lower()] = {
                    'directory': target_dir,
                    'files': len(csv_files),
                    'total_rows': 0,
                    'date_range': {'min': None, 'max': None},
                    'gkg_format_valid': True,
                    'numarts_valid': True,
                    'numarts_issues': 0
                }
                
                all_dates = []
                total_rows = 0
                
                for filename in csv_files:
                    file_path = os.path.join(target_dir, filename)
                    try:
                        # Try both tab and comma separation
                        try:
                            df = pd.read_csv(file_path, sep='\t')
                        except:
                            df = pd.read_csv(file_path)
                        
                        total_rows += len(df)
                        
                        if 'DATE' in df.columns:
                            all_dates.extend(df['DATE'].tolist())
                        
                        # Validate GKG format specifics
                        if 'TONE' in df.columns:
                            # Check if TONE has comma-separated values
                            sample_tone = df['TONE'].iloc[0] if len(df) > 0 else ""
                            if ',' not in str(sample_tone):
                                validation_results[target_dir.lower()]['gkg_format_valid'] = False
                        
                        # Check NUMARTS = 1
                        if 'NUMARTS' in df.columns:
                            numarts_values = df['NUMARTS'].unique()
                            non_one_values = [v for v in numarts_values if v != 1]
                            if non_one_values:
                                validation_results[target_dir.lower()]['numarts_valid'] = False
                                validation_results[target_dir.lower()]['numarts_issues'] += len(non_one_values)
                                print(f"⚠️  {target_dir}/{filename}: Found NUMARTS values other than 1: {non_one_values}")
                            
                    except Exception as e:
                        print(f"⚠️  Error validating {filename}: {e}")
                        validation_results[target_dir.lower()]['gkg_format_valid'] = False
                
                validation_results[target_dir.lower()]['total_rows'] = total_rows
                
                if all_dates:
                    validation_results[target_dir.lower()]['date_range']['min'] = min(all_dates)
                    validation_results[target_dir.lower()]['date_range']['max'] = max(all_dates)
        
        # Print validation results
        print(f"✅ GKG Validation Results:")
        for key, result in validation_results.items():
            print(f"📂 {result['directory']}:")
            print(f"   Files: {result['files']}")
            print(f"   Total rows: {result['total_rows']:,}")
            
            if 'date_range' in result and result['date_range']['min']:
                print(f"   Date range: {result['date_range']['min']} to {result['date_range']['max']}")
            
            if 'format_issues' in result:
                print(f"   Format issues: {result['format_issues']}")
                print(f"   Column consistency: {'✅' if result['column_consistency'] else '❌'}")
            
            if 'gkg_format_valid' in result:
                print(f"   GKG format valid: {'✅' if result['gkg_format_valid'] else '❌'}")
            
            if 'numarts_valid' in result:
                print(f"   ✅ NUMARTS = 1: {'✅' if result['numarts_valid'] else '❌'}")
                if result['numarts_issues'] > 0:
                    print(f"   ⚠️  NUMARTS issues found: {result['numarts_issues']}")
        
        return validation_results

    def save_comprehensive_gkg_report(self):
        """Save comprehensive report of all GKG operations"""
        report = {
            'metadata': {
                'user': self.user_login,
                'timestamp': self.current_time,
                'file_format': 'YYYYMMDD.gkg.csv (GKG specification)',
                'numarts_specification': 'Fixed at 1 (as per sample data)',
                'date_range': f"{self.start_date.strftime('%Y-%m-%d')} to {self.end_date.strftime('%Y-%m-%d')}",
                'source_directory': self.source_dir,
                'target_directories': self.target_dirs,
                'gkg_columns': ['DATE', 'NUMARTS', 'COUNTS', 'THEMES', 'LOCATIONS', 'PERSONS', 'ORGANIZATIONS', 'TONE', 'CAMEOEVENTIDS', 'SOURCES', 'SOURCEURLS']
            },
            'statistics': self.stats
        }
        
        report_filename = f"{self.user_login}_gkg_comprehensive_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(report_filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\n📋 GKG comprehensive report saved: {report_filename}")
        return report_filename

    def print_final_gkg_summary(self):
        """Print final summary of all GKG operations"""
        print(f"\n" + "="*80)
        print(f"🎉 UNIFIED GKG DATA MANAGER - FINAL SUMMARY")
        print(f"="*80)
        print(f"👤 User: {self.user_login}")
        print(f"⏰ Completed at: {self.current_time}")
        print(f"📁 File format: YYYYMMDD.gkg.csv (GKG specification)")
        print(f"✅ NUMARTS: Fixed at 1 for all generated data")
        print(f"📋 Data format: Global Knowledge Graph (GKG)")
        
        # Generation summary
        if self.stats['generation']['total_files'] > 0:
            print(f"\n📝 GKG Data Generation:")
            print(f"   Files generated: {self.stats['generation']['total_files']}")
            print(f"   Total rows: {self.stats['generation']['total_rows']:,}")
            print(f"   ✅ All rows have NUMARTS = 1")
            print(f"   Total size: {self.stats['generation']['total_size_mb']:.2f} MB")
            print(f"   Avg size per file: {self.stats['generation']['total_size_mb']/self.stats['generation']['total_files']:.2f} MB")
        
        # Merge summary
        if self.stats['merge']['total_processed'] > 0:
            print(f"\n🔗 GKG Data Merge:")
            print(f"   Files processed: {self.stats['merge']['total_processed']}")
            print(f"   Rows added: {self.stats['merge']['total_added']:,}")
            print(f"   Duplicates skipped: {self.stats['merge']['total_duplicates_skipped']:,}")
            print(f"   ✅ NUMARTS = 1 maintained throughout merge")
            
            if self.stats['merge']['total_processed'] > 0:
                duplicate_rate = (self.stats['merge']['total_duplicates_skipped'] / 
                                (self.stats['merge']['total_added'] + self.stats['merge']['total_duplicates_skipped']) * 100)
                print(f"   Duplicate rate: {duplicate_rate:.2f}%")
            
            print(f"\n📅 Monthly Distribution:")
            for month, stats in self.stats['merge']['by_month'].items():
                if stats['files_processed'] > 0:
                    print(f"   📂 {month}: {stats['files_processed']} files, {stats['rows_added']:,} rows")

def main():
    print("="*90)
    print("🗞️  UNIFIED GKG (GLOBAL KNOWLEDGE GRAPH) DATA MANAGER")
    print("="*90)
    print(f"👤 User: strawberrymilktea0604")
    print(f"⏰ Current time (UTC): 2025-06-18 06:27:22")
    print(f"📁 File format: YYYYMMDD.gkg.csv (GKG specification)")
    print(f"✅ NUMARTS: Fixed at 1 (as per sample specification)")
    print(f"📅 Target period: April 1, 2025 - June 10, 2025")
    print(f"🎯 Purpose: Expand Kaggle dataset from 7GB to 10GB")
    print(f"📋 Data format: Follows GKG (Global Knowledge Graph) specification")
    print("="*90)
    
    manager = UnifiedGKGDataManager()
    
    print(f"\n📋 Available GKG Operations:")
    print(f"1. 📝 Generate daily GKG files (.gkg.csv) with NUMARTS=1")
    print(f"2. 🔗 Intelligent merge to monthly directories") 
    print(f"3. 📊 Analyze GKG directory structure")
    print(f"4. 🔍 Validate GKG data integrity (including NUMARTS)")
    print(f"5. 🚀 Full GKG process (generate + merge + validate)")
    print(f"6. ℹ️  Show GKG system information")
    
    choice = input(f"\n👉 Select GKG operation (1-6): ").strip()
    
    if choice == "1":
        target_gb = float(input("🎯 Enter target size in GB [3]: ").strip() or "3")
        print(f"\n📝 Starting generation of GKG files with NUMARTS=1...")
        manager.generate_gkg_daily_files(target_gb)
        
    elif choice == "2":
        print(f"\n🔗 Starting intelligent GKG merge process...")
        manager.intelligent_merge_to_monthly_dirs()
        
    elif choice == "3":
        manager.analyze_gkg_directory_structure()
        
    elif choice == "4":
        manager.validate_gkg_data_integrity()
        
    elif choice == "5":
        print(f"\n🚀 FULL GKG PROCESS INITIATED")
        print(f"=" * 60)
        
        target_gb = float(input("🎯 Enter target size in GB [3]: ").strip() or "3")
        
        # Step 1: Generate GKG files
        print(f"\n📝 Step 1: Generating GKG files with NUMARTS=1...")
        manager.generate_gkg_daily_files(target_gb)
        
        # Step 2: Merge to monthly directories
        print(f"\n🔗 Step 2: Intelligent GKG merge to monthly directories...")
        manager.intelligent_merge_to_monthly_dirs()
        
        # Step 3: Validate integrity
        print(f"\n🔍 Step 3: Validating GKG data integrity (including NUMARTS)...")
        manager.validate_gkg_data_integrity()
        
        # Step 4: Save comprehensive report
        manager.save_comprehensive_gkg_report()
        
        # Step 5: Final summary
        manager.print_final_gkg_summary()
        
        print(f"\n🎉 FULL GKG PROCESS COMPLETED SUCCESSFULLY!")
        print(f"✅ All data generated with NUMARTS = 1")
        
    elif choice == "6":
        print(f"\n💻 GKG System Information:")
        print(f"👤 User login: strawberrymilktea0604")
        print(f"⏰ Current UTC time: 2025-06-18 06:27:22")
        print(f"📁 File format: YYYYMMDD.gkg.csv")
        print(f"✅ NUMARTS specification: Fixed at 1")
        print(f"📋 Data specification: GKG (Global Knowledge Graph)")
        print(f"📅 Target date range: April 1, 2025 - June 10, 2025")
        print(f"📊 Total days in range: 71 days")
        print(f"🎯 Target additional size: 3GB")
        print(f"📂 Source directory: strawberrymilktea0604_april_june_2025")
        print(f"📁 Target directories: April/, May/, June/")
        print(f"🔗 Original dataset: https://www.kaggle.com/datasets/nguynlmtng/topic-news")
        
        # Show GKG column structure
        print(f"\n📋 GKG Column Structure:")
        gkg_columns = ['DATE', 'NUMARTS', 'COUNTS', 'THEMES', 'LOCATIONS', 'PERSONS', 'ORGANIZATIONS', 'TONE', 'CAMEOEVENTIDS', 'SOURCES', 'SOURCEURLS']
        for i, col in enumerate(gkg_columns, 1):
            if col == 'NUMARTS':
                print(f"   {i:2d}. {col} (Fixed at 1)")
            else:
                print(f"   {i:2d}. {col}")
        
        # Show current status
        dates = manager.get_date_list()
        print(f"\n📄 File Information:")
        print(f"📁 Files to be generated: {len(dates)} .gkg.csv files")
        print(f"📄 First file: {manager.get_filename(dates[0])}")
        print(f"📄 Last file: {manager.get_filename(dates[-1])}")
        print(f"✅ Each file will have NUMARTS = 1 for all rows")
        
        # Check current directory status
        if os.path.exists(manager.source_dir):
            existing_files = [f for f in os.listdir(manager.source_dir) if f.endswith(manager.file_extension)]
            print(f"📁 Current source files: {len(existing_files)}")
        else:
            print(f"📁 Source directory not yet created")
        
        print(f"\n✨ Ready for GKG data generation with NUMARTS=1!")
        
    else:
        print(f"❌ Invalid option!")

if __name__ == "__main__":
    main()