import socket
import subprocess
import teradatasql
import json
import platform
import argparse

def test_port(ip):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect((ip, 1025))
        return True
    except (socket.timeout, ConnectionRefusedError):
        return False

def ping_server(ip):
    system = platform.system().lower()
    if system == "windows":
        command = ["ping", "-n", "1", ip]
    else:
        command = ["ping", "-c", "1", ip]

    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Ping command failed: {' '.join(command)}")
        print(f"Error: {e}")
        print(f"Standard Output: {e.stdout.decode()}")
        print(f"Standard Error: {e.stderr.decode()}")
        return False

def load_config():
    try:
        with open("teradata_config.json", "r") as config_file:
            return json.load(config_file)
    except FileNotFoundError:
        print("Error: Configuration file 'teradata_config.json' not found.")
        return None

def test_connection(conn_details):
    try:
        with teradatasql.connect(
            host=conn_details["host"].lower(),
            user=conn_details["user"].lower(),
            password=conn_details["password"]
        ) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT hostname, type, cname, ip, description, Active FROM db.tbl")
            results = cursor.fetchall()

            active_servers = []
            inactive_tpa_hsn_servers = []
            inactive_tms_servers = []

            active_failed = active_succeeded = 0
            tpa_hsn_failed = tpa_hsn_succeeded = 0
            tms_failed = tms_succeeded = 0

            for row in results:
                hostname, server_type, cname, ip, description, active = row
                if active.lower() == "y":
                    if test_port(ip):
                        active_succeeded += 1
                    else:
                        active_failed += 1
                        active_servers.append(row)
                elif active.lower() == "n":
                    if server_type.lower() in ("tpa", "hsn"):
                        if ping_server(ip):
                            tpa_hsn_succeeded += 1
                        else:
                            tpa_hsn_failed += 1
                            inactive_tpa_hsn_servers.append(row)
                    elif server_type.lower() == "tms":
                        if ping_server(ip):
                            tms_succeeded += 1
                        else:
                            tms_failed += 1
                            inactive_tms_servers.append(row)

            print("\nACTIVE:")
            print(f"Total: {active_failed + active_succeeded}, Succeeded: {active_succeeded}, Failed: {active_failed}")
            for row in active_servers:
                print(f"- {row[0]} ({row[3]}) - Failed")

            print("\nNot active (tpa/hsn):")
            print(f"Total: {tpa_hsn_failed + tpa_hsn_succeeded}, Succeeded: {tpa_hsn_succeeded}, Failed: {tpa_hsn_failed}")
            for row in inactive_tpa_hsn_servers:
                print(f"- {row[0]} ({row[3]}) - Failed")

            print("\nNot active (tms):")
            print(f"Total: {tms_failed + tms_succeeded}, Succeeded: {tms_succeeded}, Failed: {tms_failed}")
            for row in inactive_tms_servers:
                print(f"- {row[0]} ({row[3]}) - Failed")

    except teradatasql.Error as e:
        print(f"Error connecting to Teradata: {e}")

def main():
    parser = argparse.ArgumentParser(
        description="Test Teradata connections. This script reads IP addresses from a Teradata table, tests port 1025 for active servers, and pings inactive servers based on their type.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-s", "--server",
        help="Specify the server connection (dev, dr, prod, or all). If not provided, the script will prompt for a connection."
    )
    args = parser.parse_args()

    config = load_config()
    if not config:
        return

    debug_mode = config.get("global", {}).get("debug", False)

    if args.server:
        if args.server.lower() == "all":
            for conn_name in config:
                if conn_name != "global":
                    print(f"\nTesting connection: {conn_name}")
                    test_connection(config[conn_name])
        elif args.server.lower() in config:
            test_connection(config[args.server.lower()])
        else:
            print("Invalid connection name. Please choose from dev, dr, prod, or all.")
    else:
        print("Available connections:")
        for conn_name in config:
            if conn_name != "global":
                print(f"- {conn_name}")

        chosen_conn = input("Enter the connection name (dev, dr, or prod): ").lower()
        if chosen_conn not in config:
            print("Invalid connection name. Please choose from dev, dr, or prod.")
            return

        test_connection(config[chosen_conn])

if __name__ == "__main__":
    main()
