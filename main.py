import socket
import sys
import pandas as pd
import pyodbc
import schedule
import time


def tcp_server():
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind the socket to the address given on the command line
    server_address = ('', 2012)
    sock.bind(server_address)
    print('starting up on %s port %s' % sock.getsockname())
    sock.listen(1)

    while True:
        print('waiting for a connection', file=sys.stderr)
        connection, client_address = sock.accept()
        try:
            print('client connected:', client_address, file=sys.stderr, )
            while True:
                data = connection.recv(256)
                print('received "%s"' % data, file=sys.stderr)
                # the following is reserved for future use:
                # decoded_str = data.decode("utf-8")
                file1 = open("c:/input.txt", "ab")
                file1.write(data)

        finally:
            connection.close()


def parser_main():
    def txt_to_sql_parser():
        text = open("c:/input.txt", "r")
        text = ''.join([i for i in text]) \
            .replace("^", "")
        text = ''.join([i for i in text]) \
            .replace(", Temperature 2:-50.0, Temperature 3:-50.0, Temperature 4:-50.0", "")
        text = ''.join([i for i in text]) \
            .replace("Temperature 1:", "")
        x = open("c:/output.txt", "w")
        x.writelines(text)
        x.close()

        df = pd.read_csv("c:/output.txt", error_bad_lines=False, sep="|", engine="python")
        df.columns = ['Plate_number', 'Date_Time', 'NA1', 'NA2', 'NA3', 'NA4', 'Temp']
        df = df.fillna(value=0)

        # Saving the reference of the standard output
        original_stdout = sys.stdout

        print(df)
        with open('c:/demo.txt', 'w') as f:
            sys.stdout = f
            print(df)
            # Reset the standard output
            sys.stdout = original_stdout

        print('Output is Ready!!!.')

        # Connect to SQL Server
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=localhost/PULSESQLEXPRESS;'
                              'Database=PulseProject;'
                              'UID=sa;'
                              'PWD=pulse;'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()

        # Try to Create Table
        try:
            cursor.execute('CREATE TABLE Test_info (Plate_number nvarchar(MAX) NOT NULL, Date_Time nvarchar(MAX) NOT '
                           'NULL, '
                           'NA1 nvarchar(MAX) NOT NULL, NA2 nvarchar(MAX) NOT NULL, NA3 nvarchar(MAX) NOT NULL,'
                           'NA4 nvarchar(MAX) NOT NULL, Temp nvarchar(MAX) NOT NULL)')
        except:
            print("Table Is already in the DB!")

        finally:
            # Insert DataFrame to Table
            for row in df.itertuples():
                cursor.execute('''
                        INSERT INTO PulseProject.dbo.Test_info (Plate_number, Date_Time, NA1, NA2, NA3, NA4, Temp)
                        VALUES (?,?,?,?,?,?,?)
                        ''',
                               row.Plate_number,
                               row.Date_Time,
                               row.NA1,
                               row.NA2,
                               row.NA3,
                               row.NA4,
                               row.Temp
                               )
        conn.commit()

    schedule.every(60).seconds.do(txt_to_sql_parser)

    while 1:
        schedule.run_pending()
        time.sleep(2)


if __name__ == "__main__":
    tcp_server()
    parser_main()
