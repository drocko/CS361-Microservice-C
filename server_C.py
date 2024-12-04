import zmq
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import time


# Google Sheets API setup
def setup_google_sheets():
    credentials_name = "credentials.json"  # Can change credentials name
    sheets_name = "CS361 Water Hydration Google Sheet"  # Can change sheets name
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_name, scope)
    client = gspread.authorize(creds)
    sheet = client.open(sheets_name).sheet1

    # Check if the header exists, otherwise add it
    if not sheet.row_values(1):  # If the first row is empty
        sheet.append_row(["Timestamp", "Amount", "Unit"])  # Add header row

    return sheet


# ZeroMQ server setup
def server_main():
    """Main function to handle ZeroMQ server and Google Sheets data logging."""
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://0.0.0.0:5555")
    sheet = setup_google_sheets()

    print(">>> Microservice C (GOOGLE SHEETS))Server running...")

    while True:
        try:
            request = socket.recv_json()  # Expecting JSON format
            action = request.get("action", "").lower()
            data = request.get("data", {})

            if not action:
                socket.send_json({"status": "error", "message": "Invalid request format"})
                continue

            if action == "create":
                response = create_entry(sheet, data)
            elif action == "read":
                response = read_entries(sheet)
            elif action == "update":
                response = update_entry(sheet, data)
            elif action == "delete":
                response = delete_entry(sheet, data)
            elif action == "reset":
                response = reset_data(sheet)  # Reset data if action is "reset"
            elif action == "undo":
                response = undo_last_entry(sheet)  # Undo the last entry by deleting the last row
            else:
                response = {"status": "error", "message": "Invalid action"}

            socket.send_json(response)
        except Exception as e:
            print(f"Error: {e}")
            socket.send_json({"status": "error", "message": str(e)})


# CRUD Operations
def create_entry(sheet, data):
    try:
        # Extract values from the received data
        timestamp = data.get("timestamp")
        amount = data.get("amount")
        unit = data.get("unit")

        # Append the data as a row to the sheet (timestamp, amount, unit)
        sheet.append_row([timestamp, amount, unit])  # Correct format here
        return {"status": "success", "message": "Entry created"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def read_entries(sheet):
    try:
        # Retrieve all records from Google Sheets
        records = sheet.get_all_records()
        # Check if the records are in the correct format
        print(f"C Retrieved records from sheet: {records}")

        # Return records to the client
        return {"status": "success", "data": records}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def update_entry(sheet, data):
    try:
        timestamp = data.get("timestamp")  # Using timestamp as identifier
        cell = sheet.find(timestamp)  # Find row by timestamp
        sheet.update_cell(cell.row, 2, data.get("amount"))  # Update amount
        sheet.update_cell(cell.row, 3, data.get("unit"))  # Update unit
        return {"status": "success", "message": "Entry updated"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def delete_entry(sheet, data):
    try:
        timestamp = data.get("timestamp")  # Using timestamp as identifier
        cell = sheet.find(timestamp)  # Find row by timestamp
        sheet.delete_rows(cell.row)
        return {"status": "success", "message": "Entry deleted"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def reset_data(sheet):
    try:
        # Clear all rows in the sheet, keeping the header
        sheet.delete_rows(2, sheet.row_count)  # Start at row 2 to keep header
        return {"status": "success", "message": "All data reset"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Undo Functionality (delete the last filled row)
def undo_last_entry(sheet):
    try:
        # Ensure there's at least one data row (ignoring header row)
        if sheet.row_count > 1:
            # Get the last row's data (ignoring the header)
            last_row = sheet.row_values(sheet.row_count)  # Fetch the last row

            # Delete the last row from the sheet
            sheet.delete_rows(sheet.row_count)

            return {
                "status": "success",
                "message": "Last entry undone (deleted).",
                "data": last_row  # Return the last entry data
            }
        else:
            return {"status": "error", "message": "No entries to undo."}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Entry point
if __name__ == "__main__":
    server_main()
    # MICROSERVICE C
