import os
import sys
import zipfile
import shutil
import adsdb  # Use the adsdb module in the workspace

def extract_blfe_database(blfe_file_path, output_directory):
    """
    Extract database files from a .blfe file (zip format) and prepare them for use with Advantage
    
    Args:
        blfe_file_path: Path to the .blfe file
        output_directory: Directory to extract files to
    
    Returns:
        dict: Paths to the extracted database files
    """
    print(f"Extracting database from: {blfe_file_path}")
    print(f"Output directory: {output_directory}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)
    
    # Create temporary directory for extraction
    temp_dir = os.path.join(output_directory, 'temp')
    os.makedirs(temp_dir, exist_ok=True)
    
    # Copy the file
    temp_blfe = os.path.join(output_directory, 'oldlifedata.blfe')
    shutil.copyfile(blfe_file_path, temp_blfe)
    
    # Check if valid zip file
    try:
        with zipfile.ZipFile(temp_blfe, 'r') as zip_check:
            file_list = zip_check.namelist()
            print(f"Files in archive: {file_list}")
            is_valid = True
    except zipfile.BadZipFile:
        is_valid = False
        print(f"Error: {blfe_file_path} is not a valid zip file")
        return None
    
    # Extract all files
    with zipfile.ZipFile(temp_blfe, 'r') as zip_file:
        zip_file.extractall(temp_dir)
    
    # List files in temp directory
    print(f"Files extracted to temp directory: {os.listdir(temp_dir)}")
    
    # Copy the main database files
    result_files = {}
    
    try:
        # Look for ADT and ADI files
        adt_files = [f for f in os.listdir(temp_dir) if f.lower().endswith('.adt')]
        adi_files = [f for f in os.listdir(temp_dir) if f.lower().endswith('.adi')]
        
        if not adt_files:
            print("Error: No ADT files found in the extracted archive")
            return None
            
        if not adi_files:
            print("Error: No ADI files found in the extracted archive")
            return None
            
        # Use the first ADT file found
        adt_src = os.path.join(temp_dir, adt_files[0])
        adt_dst = os.path.join(output_directory, 'olddata.adt')
        shutil.copyfile(adt_src, adt_dst)
        result_files['adt'] = adt_dst
        
        # Use the first ADI file found
        adi_src = os.path.join(temp_dir, adi_files[0])
        adi_dst = os.path.join(output_directory, 'olddata.adi')
        shutil.copyfile(adi_src, adi_dst)
        result_files['adi'] = adi_dst
        
        print(f"Database files created: {result_files}")
        
    except Exception as e:
        print(f"Error extracting database files: {e}")
        return None
    
    return result_files

def connect_with_adsdb(data_path):
    """
    Connect to Advantage database using the adsdb module
    
    Args:
        data_path: Path to the database directory
    
    Returns:
        Connection object if successful, None otherwise
    """
    try:
        # Get information about the adsdb module
        print(f"ADSDB version: {adsdb.apilevel}")
        print(f"Thread safety: {adsdb.threadsafety}")
        print(f"Parameter style: {adsdb.paramstyle}")
        
        # Try different connection approaches with positional arguments
        connection_attempts = [
            (os.path.join(data_path, "olddata.adt")),  # Just the path to the ADT file
            (os.path.join(data_path, "olddata")),      # Path without extension
            (data_path),                               # Directory only
            (os.path.join(data_path)),                 # Explicit directory path
        ]
        
        # Also try with connection strings
        connection_strings = [
            {"ServerType": "LOCAL"},
            {"ServerType": "LOCAL", "TableType": "ADT"},
            {"CommType": "TCP/IP", "ServerType": "LOCAL"},
            {"CommType": "UDP", "ServerType": "LOCAL"},
            {"TrimTrailingSpaces": "1"},
            {"LockMethod": "Proprietary"},
        ]
        
        error = None
        
        # Try positional arguments first
        for i, path in enumerate(connection_attempts):
            try:
                print(f"\nTrying direct connection {i+1}: {path}")
                conn = adsdb.connect(path)
                print("Connection successful!")
                return conn
            except adsdb.Error as e:
                print(f"Direct connection {i+1} failed: {str(e)}")
                error = e
                
        # Try combining path with connection strings
        for i, path in enumerate(connection_attempts):
            for j, conn_string in enumerate(connection_strings):
                try:
                    # Format the connection string
                    conn_params = {
                        path: '',  # The path as a key with empty value
                        **conn_string  # Add the connection string options
                    }
                    print(f"\nTrying connection {i+1}.{j+1}: {path} with {conn_string}")
                    conn = adsdb.connect(**conn_params)
                    print("Connection successful!")
                    return conn
                except adsdb.Error as e:
                    print(f"Connection {i+1}.{j+1} failed: {str(e)}")
                    error = e
                
        if error:
            print(f"\nAll connection approaches failed. Last error: {str(error)}")
            return None
            
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def query_database(conn, table_name):
    """
    Query the database and display results
    
    Args:
        conn: Connection object
        table_name: Name of the table to query
    """
    try:
        # Create a cursor
        cursor = conn.cursor()
        
        # Try different query formats
        query_formats = [
            f"SELECT * FROM {table_name}",
            f"SELECT * FROM \"{table_name}\"",
            f"SELECT TOP 10 * FROM {table_name}",
            f"SELECT TOP 10 * FROM \"{table_name}\""
        ]
        
        cursor_result = None
        
        for i, query in enumerate(query_formats):
            try:
                print(f"\nTrying query format {i+1}: {query}")
                cursor.execute(query)
                cursor_result = cursor
                print("Query successful!")
                break
            except adsdb.Error as e:
                print(f"Query format {i+1} failed: {str(e)}")
        
        if cursor_result:
            # Get column information
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                print(f"\nColumns: {columns}")
                
                # Fetch rows
                print("\nFetching rows:")
                rows = cursor.fetchmany(10)  # Get up to 10 rows
                
                if rows:
                    for i, row in enumerate(rows):
                        print(f"Row {i+1}: {row}")
                    print(f"Number of rows fetched: {len(rows)}")
                else:
                    print("No rows found")
            else:
                print("No column information available")
        
        # Close cursor
        cursor.close()
    
    except Exception as e:
        print(f"Error querying database: {str(e)}")

def main():
    # Step 1: Extract the database files
    data_dir = r"C:\adsdb\extracted"
    blfe_file = r"C:\adsdb\Training.blfe"
    
    print("Starting database extraction...")
    extracted_files = extract_blfe_database(blfe_file, data_dir)
    
    if not extracted_files:
        print("Failed to extract database files. Exiting.")
        return
    
    print(f"Successfully extracted database files to {data_dir}")
    
    # Step 2: Connect to the database using adsdb module
    print("\nAttempting to connect to database using adsdb module...")
    conn = connect_with_adsdb(data_dir)
    
    if conn:
        try:
            # Step 3: Query the database
            table_name = os.path.basename(extracted_files['adt']).replace('.adt', '')
            print(f"\nQuerying table: {table_name}")
            query_database(conn, table_name)
            
            # Close the connection
            conn.close()
            print("\nDatabase connection closed")
        except Exception as e:
            print(f"Error: {str(e)}")
    else:
        print("Could not connect to database")

if __name__ == "__main__":
    main() 