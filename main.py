import csv, sqlite3
sqlite_con = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

cur = sqlite_con.cursor()
plik = input()

cur.execute('''CREATE TABLE polaczenia (from_subscriber data_type INTEGER, 
                  to_subscriber data_type INTEGER, 
                  datetime data_type timestamp, 
                  duration data_type INTEGER , 
                  celltower data_type INTEGER);''') 

with open(plik,'r') as fin: 
    reader = csv.reader(fin, delimiter = ";") # comma is default delimiter
    next(reader, None)  # skip the headers
    rows = [x for x in reader]
    cur.executemany("INSERT INTO polaczenia (from_subscriber, to_subscriber, datetime, duration , celltower) VALUES (?, ?, ?, ?, ?);", rows)
    sqlite_con.commit()

class ReportGenerator:
  def __init__(self,connection, escape_string = "(%s)"):
    self.connection = connection
    self.report_text = None
    self.escape_string = escape_string

  def generate_report(self):
    cursor = self.connection.cursor()
    sql_query = f"Select sum(duration) from polaczenia"
    cursor.execute(sql_query)
    self.result = cursor.fetchone()[0]
    
    
  def get_report(self):	
    return self.result


if __name__ == "__main__":
 rg = ReportGenerator(sqlite_con, escape_string="?")
 rg.generate_report()
 rg.get_report()
