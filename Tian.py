import ctypes
import os
import zipfile
import shutil
import sys
import stat
from ctypes import c_char_p, c_long, byref, create_string_buffer, c_int, c_uint, c_ushort

# Advantage server types
ADS_LOCAL_SERVER = 0
ADS_REMOTE_SERVER = 1
ADS_AIS_SERVER = 2

# Advantage error codes and their explanations
ADS_ERROR_CODES = {
    5019: "File does not exist or path not found",
    5035: "Insufficient rights or permissions",
    5025: "Invalid path",
    5010: "Unable to load ACE DLL",
    5018: "Invalid connection handle",
    5081: "Invalid server type specified",
    5177: "Connection failed - unable to connect to server",
    5037: "Access denied",
    5026: "Invalid table name",
    5143: "Invalid SQL statement",
    5179: "Cannot open table",
    # Add more as needed
}

def get_error_message(error_code):
    """Get a human-readable error message for an Advantage error code"""
    return ADS_ERROR_CODES.get(error_code, f"Unknown error: {error_code}")

def check_file_permissions(file_path):
    """
    Check if a file exists and is readable/writable
    
    Args:
        file_path: Path to the file to check
        
    Returns:
        dict: Information about file permissions
    """
    if not os.path.exists(file_path):
        return {"exists": False, "message": f"File {file_path} does not exist"}
        
    result = {
        "exists": True,
        "readable": os.access(file_path, os.R_OK),
        "writable": os.access(file_path, os.W_OK),
        "executable": os.access(file_path, os.X_OK)
    }
    
    # Get file permissions in octal format
    try:
        file_stat = os.stat(file_path)
        result["permissions_octal"] = oct(file_stat.st_mode & 0o777)
        result["size"] = file_stat.st_size
        result["owner_uid"] = file_stat.st_uid
        result["group_gid"] = file_stat.st_gid
    except Exception as e:
        result["stat_error"] = str(e)
    
    return result

def set_file_permissions(file_path):
    """
    Set appropriate permissions on a file to ensure it can be accessed
    
    Args:
        file_path: Path to the file to modify
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if not os.path.exists(file_path):
            return False
            
        # Make the file readable and writable by everyone
        # 0o666 = rw-rw-rw-
        os.chmod(file_path, 0o666)
        
        return True
    except Exception as e:
        print(f"Error setting permissions on {file_path}: {str(e)}")
        return False

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
        
        # Set proper permissions on the database files
        for file_path in result_files.values():
            print(f"Setting permissions on {file_path}")
            set_file_permissions(file_path)
            
            # Check and display permissions
            perm_info = check_file_permissions(file_path)
            print(f"File permissions for {file_path}:")
            for key, value in perm_info.items():
                print(f"  {key}: {value}")
        
        print(f"Database files created: {result_files}")
        
    except Exception as e:
        print(f"Error extracting database files: {e}")
        return None
    
    return result_files

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
    
    # Check permissions on the directory
    dir_perms = check_file_permissions(data_dir)
    print(f"Directory permissions for {data_dir}:")
    for key, value in dir_perms.items():
        print(f"  {key}: {value}")
    
    # Step 2: Connect to the Advantage database using ACE
    try:
        # Load the Advantage Client Engine DLL
        ace_path = r"C:\Program Files\Advantage 11.10\acesdk\ace64.dll"
        
        if not os.path.exists(ace_path):
            print(f"Error: ACE DLL not found at {ace_path}")
            return
            
        try:
            ace = ctypes.WinDLL(ace_path)
            print(f"Successfully loaded ACE from {ace_path}")
        except Exception as e:
            print(f"Error loading ACE DLL: {str(e)}")
            return
        
        # Check if functions exist before assigning prototypes
        def function_exists(dll, func_name):
            try:
                getattr(dll, func_name)
                return True
            except AttributeError:
                return False
                
        # List available functions in the DLL (for debugging)
        print("\nChecking available ACE functions...")
        
        # Common Advantage functions we might need
        ace_functions = [
            'AdsConnect60', 
            'AdsCreateSQLStatement',
            'AdsExecuteSQLDirect', 
            'AdsDisconnect',
            'AdsFreeStatement',
            'AdsGetTableType',
            'AdsStmtNumResultCols',
            'AdsStmtResultColumnName',
            'AdsStmtResultColumnType',
            'AdsGetString',
            'AdsFetchNext'
        ]
        
        # Check which functions are available
        available_functions = {}
        for func_name in ace_functions:
            available_functions[func_name] = function_exists(ace, func_name)
            print(f"  {func_name}: {'Available' if available_functions[func_name] else 'Not available'}")
            
        # Set function prototypes for available functions
        if available_functions['AdsConnect60']:
            ace.AdsConnect60.argtypes = [c_char_p, c_char_p, c_char_p, c_ushort, ctypes.POINTER(c_long)]
            ace.AdsConnect60.restype = c_int
        else:
            print("Error: AdsConnect60 function is not available")
            return
            
        if available_functions['AdsCreateSQLStatement']:
            ace.AdsCreateSQLStatement.argtypes = [c_long, ctypes.POINTER(c_long)]
            ace.AdsCreateSQLStatement.restype = c_int
        
        if available_functions['AdsExecuteSQLDirect']:
            ace.AdsExecuteSQLDirect.argtypes = [c_long, c_char_p]
            ace.AdsExecuteSQLDirect.restype = c_int
        
        if available_functions['AdsDisconnect']:
            ace.AdsDisconnect.argtypes = [c_long]
            ace.AdsDisconnect.restype = c_int
        
        if available_functions['AdsFreeStatement']:
            ace.AdsFreeStatement.argtypes = [c_long]
            ace.AdsFreeStatement.restype = c_int
        
        # Create connection handle
        connection_handle = c_long()
        
        # With Advantage Local Server, it's important to use the correct path format
        # Try with directory first, which often works better with Local Server
        data_dir_path = os.path.dirname(extracted_files['adt'])
        # Ensure correct Windows path format
        data_dir_path = data_dir_path.replace('/', '\\')
        
        # Get table name from the ADT file
        adt_file_path = extracted_files['adt']
        table_name = os.path.basename(adt_file_path).replace('.adt', '')
        
        print(f"\nAttempting to connect to Advantage Database...")
        print(f"Directory Path: {data_dir_path}")
        print(f"Table name: {table_name}")
        
        # Try multiple connection approaches with appropriate permissions
        connection_successful = False
        
        # Approach 1: Try with full path to ADT file first
        print(f"\nApproach 1: Connecting with full table path...")
        print(f"File permissions for {adt_file_path}:")
        perm_info = check_file_permissions(adt_file_path)
        for key, value in perm_info.items():
            print(f"  {key}: {value}")
            
        result = ace.AdsConnect60(
            c_char_p(adt_file_path.encode('utf-8')),
            c_char_p(None),  # Username
            c_char_p(None),  # Password
            c_ushort(ADS_LOCAL_SERVER),  # Force local server mode
            byref(connection_handle)
        )
        
        if result == 0:
            print("Connection successful with full table path!")
            connection_successful = True
        else:
            print(f"Table path connection failed. Error code: {result} - {get_error_message(result)}")
            
            # Approach 2: Try with directory path
            print(f"\nApproach 2: Connecting with directory path...")
            print(f"Directory permissions for {data_dir_path}:")
            dir_perm_info = check_file_permissions(data_dir_path)
            for key, value in dir_perm_info.items():
                print(f"  {key}: {value}")
                
            result = ace.AdsConnect60(
                c_char_p(data_dir_path.encode('utf-8')),
                c_char_p(None),  # Username
                c_char_p(None),  # Password
                c_ushort(ADS_LOCAL_SERVER),
                byref(connection_handle)
            )
            
            if result == 0:
                print("Connection successful with directory path!")
                connection_successful = True
            else:
                print(f"Directory connection failed. Error code: {result} - {get_error_message(result)}")
                
                # Approach 3: Try with Parent Directory path
                parent_dir = os.path.dirname(data_dir_path)
                print(f"\nApproach 3: Connecting with parent directory path...")
                print(f"Parent directory: {parent_dir}")
                
                result = ace.AdsConnect60(
                    c_char_p(parent_dir.encode('utf-8')),
                    c_char_p(None),
                    c_char_p(None),
                    c_ushort(ADS_LOCAL_SERVER),
                    byref(connection_handle)
                )
                
                if result == 0:
                    print("Connection successful with parent directory path!")
                    connection_successful = True
                else:
                    print(f"Parent directory connection failed. Error code: {result} - {get_error_message(result)}")
                    
                    # Approach 4: Try remote server instead of local
                    print(f"\nApproach 4: Trying with remote server mode...")
                    result = ace.AdsConnect60(
                        c_char_p(adt_file_path.encode('utf-8')),
                        c_char_p(None),
                        c_char_p(None),
                        c_ushort(ADS_REMOTE_SERVER),  # Try remote server
                        byref(connection_handle)
                    )
                    
                    if result == 0:
                        print("Connection successful with remote server mode!")
                        connection_successful = True
                    else:
                        print(f"Remote server connection failed. Error code: {result} - {get_error_message(result)}")
        
        if not connection_successful:
            print("\nAll connection approaches failed. Diagnosis:")
            print("1. File permissions issues. Current permissions:")
            print(f"   - ADT file: {check_file_permissions(adt_file_path)['permissions_octal']}")
            print(f"   - Directory: {check_file_permissions(data_dir_path)['permissions_octal']}")
            print("2. Advantage Database Server components:")
            print("   - ACE DLL is loaded successfully")
            print("   - Some functions are missing or not properly implemented")
            print("3. Database format compatibility:")
            print("   - The database files may not be compatible with this version of Advantage")
            print("\nSuggested actions:")
            print("1. Verify Advantage Database Server is properly installed")
            print("2. Try running the application with administrator privileges")
            print("3. Check if the database format is compatible with your Advantage version")
            print("4. Consider using ODBC connection instead of direct ACE access")
            return
            
        print(f"Connection successful! Handle: {connection_handle.value}")
        
        # Create a SQL statement handle
        statement_handle = c_long()
        
        stmt_result = ace.AdsCreateSQLStatement(
            connection_handle, 
            byref(statement_handle)
        )
        
        if stmt_result != 0:
            print(f"Failed to create SQL statement. Error code: {stmt_result} - {get_error_message(stmt_result)}")
            ace.AdsDisconnect(connection_handle)
            return
        
        print(f"SQL statement handle created: {statement_handle.value}")
        
        # Try different query formats
        query_formats = [
            f"SELECT * FROM \"{table_name}\"",  # With quotes
            f"SELECT * FROM {table_name}",     # Without quotes
            f"SELECT TOP 10 * FROM \"{table_name}\"", # Limiting rows with quotes
            f"SELECT TOP 10 * FROM {table_name}",    # Limiting rows without quotes
        ]
        
        query_successful = False
        
        for i, query in enumerate(query_formats):
            print(f"\nTrying query format {i+1}: {query}")
            
            query_result = ace.AdsExecuteSQLDirect(
                statement_handle, 
                c_char_p(query.encode('utf-8'))
            )
            
            if query_result == 0:
                print(f"Query successful with format {i+1}")
                query_successful = True
                break
            else:
                print(f"Query format {i+1} failed. Error code: {query_result} - {get_error_message(query_result)}")
        
        if not query_successful:
            print("\nAll query formats failed. This suggests:")
            print("1. The table name or format isn't recognized")
            print("2. The table structure might be corrupted or incompatible")
            print("3. The connection was successful but the query syntax is incorrect for this database")
            
            # Clean up resources even on failure
            if available_functions['AdsFreeStatement']:
                ace.AdsFreeStatement(statement_handle)
            ace.AdsDisconnect(connection_handle)
            return
        
        # Success path - fetch results
        print("\nFetching query results...")
        
        # Get column information
        num_cols = c_long()
        if available_functions['AdsStmtNumResultCols']:
            ace.AdsStmtNumResultCols.argtypes = [c_long, ctypes.POINTER(c_long)]
            col_result = ace.AdsStmtNumResultCols(statement_handle, byref(num_cols))
            if col_result != 0:
                print(f"Error getting column count: {col_result}")
                if available_functions['AdsFreeStatement']:
                    ace.AdsFreeStatement(statement_handle)
                ace.AdsDisconnect(connection_handle)
                return
            print(f"Number of columns: {num_cols.value}")
        else:
            print("Warning: AdsStmtNumResultCols function not available")
            num_cols.value = 5  # Assume a default number to try
            
        # For each column, get info
        if (available_functions['AdsStmtResultColumnName'] and 
            available_functions['AdsStmtResultColumnType']):
            
            ace.AdsStmtResultColumnName.argtypes = [c_long, c_int, c_char_p, c_int]
            ace.AdsStmtResultColumnType.argtypes = [c_long, c_int, ctypes.POINTER(c_long)]
            
            for col in range(1, num_cols.value + 1):
                col_name = create_string_buffer(256)
                col_type = c_long()
                name_result = ace.AdsStmtResultColumnName(statement_handle, col, col_name, 256)
                type_result = ace.AdsStmtResultColumnType(statement_handle, col, byref(col_type))
                
                if name_result == 0 and type_result == 0:
                    print(f"Column {col}: {col_name.value.decode('utf-8', errors='replace')}, Type: {col_type.value}")
                else:
                    print(f"Column {col}: Error getting column info")
        else:
            print("Warning: Column information functions not available")
        
        # Fetch rows
        if available_functions['AdsFetchNext'] and available_functions['AdsGetString']:
            ace.AdsFetchNext.argtypes = [c_long]
            ace.AdsGetString.argtypes = [c_long, c_int, c_char_p, c_int, ctypes.POINTER(c_long)]
            
            row_count = 0
            while True:
                fetch_result = ace.AdsFetchNext(statement_handle)
                if fetch_result != 0:
                    break  # No more rows
                
                # Get data for each column
                row_data = []
                for col in range(1, num_cols.value + 1):
                    data_buffer = create_string_buffer(1024)
                    data_len = c_long()
                    get_result = ace.AdsGetString(statement_handle, col, data_buffer, 1024, byref(data_len))
                    if get_result == 0:
                        row_data.append(data_buffer.value.decode('utf-8', errors='replace'))
                    else:
                        row_data.append(f"Error getting data: {get_result}")
                
                row_count += 1
                print(f"Row {row_count} data: {row_data}")
                
                # Limit to 10 rows to avoid flooding console
                if row_count >= 10:
                    print("Only showing first 10 rows...")
                    break
                    
            print(f"Total rows fetched: {row_count}")
        else:
            print("Warning: Row fetching functions not available")
        
        # Clean up
        print("\nCleaning up resources...")
        if available_functions['AdsFreeStatement']:
            ace.AdsFreeStatement(statement_handle)
        ace.AdsDisconnect(connection_handle)
        print("Database connection closed")
    
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()