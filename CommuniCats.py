from csv import *
from ast import literal_eval
from pandas import read_csv
from time import sleep
import threading

CAT = "meow"




class Sender:
    """
    Handles writing to the master csv file
    """
    def __init__(self, masterFile, address='*'):
        self.masterFile = masterFile
        self.destination = address


    def send(self, data: tuple, address='*', masterFile='*') -> None:

        """
        Writes an address, data pair to the Master file.
        """

        # if no address is given and the Sender's destination is a wildcard, then raise a value error
        if address == '*' and self.destination == '*':
            raise Exception("address parameter required if this Sender's address is a wildcard")

        # sets the address to this Sender's destination
        if self.destination != '*':
            address = self.destination

        # if no masterFile is specified, then use the default master
        if masterFile == '*':
            masterFile = self.masterFile

        with open(masterFile, 'a') as master:

            # quoting the data so it works in CSV format
            quotedData = f"'{data}'"

            masterTemplate = ['address', 'data']
            dictTemplate = {'address': address, 'data': quotedData}

            masterWriter = DictWriter(master, fieldnames=masterTemplate)
            masterWriter.writerow(dictTemplate)


    def send_multiple(self, data: tuple, addresses: tuple, masterFile='*'):
        """
        Iterates over an address list sending the same data to all the addresses
        """

        if self.destination != '*':
            raise Exception("all addresses sent will be the locked address of this Sender")

        for address in addresses:

            self.send(data, address, masterFile)




class Receiver:
    """
    Handles collecting data from the master file by scanning the master file for the object's address
    """

    def __init__(self, address: str, masterFile: str, interval=5):
        self.address = address
        self.data = None
        self.interval = interval
        self.MASTER = masterFile

        listen = threading.Thread(target=self.listen())

        listen.daemon = True
        listen.start()


    def listen(self) -> None:
        """
        Constantly checks the master file for an offer with this receiver's address.
        Returns a list of dictionaries that
        """

        def check_file():

            while True:

                with open(self.MASTER, 'r') as master:

                    data = list()
                    masterReader = DictReader(master, delimiter=',')

                    for row in masterReader:

                        if row["address"] == self.address:
                            data.append(literal_eval(row['data']))
                            self.remove(row['address'])

                    if not data:
                        self.data = None
                    else:
                        self.data = data
                sleep(self.interval)

        # checks the Master file constantly in a different thread.
        fileChecker = threading.Thread(target=check_file)

        fileChecker.daemon = True
        fileChecker.start()


    def remove(self, address: str) -> None:
        """
        Removes ALL the offers which have an address matching the address parameter
        """
        master = read_csv(self.MASTER, index_col="address")
        master.drop(address, inplace=True)
        master.to_csv(self.MASTER, index=True)


    def wait(self):
        currentData = self.data

        while self.data == currentData:
            pass
        return self.data
