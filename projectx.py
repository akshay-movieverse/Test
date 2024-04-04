import requests
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import errorcode
import itertools
from datetime import datetime
import pdb
import time
import subprocess
from requests.exceptions import ProxyError

class CustomError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message

def restart_mysql():
    try:
        # Execute the command to restart MySQL service
        subprocess.run(['systemctl', 'restart', 'mysql.service'], check=True)
        print("MySQL service restarted successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error: Failed to restart MySQL service. {e}")

# Generate all possible 3-letter combinations of lowercase alphabets
combinations = [''.join(comb) for comb in itertools.product('abcdefghijklmnopqrstuvwxyz', repeat=3)]


def tele(x):

    #x = txt.encode()
    try:
        requests.get(f"https://api.telegram.org/bot7031701914:AAEtWet1t6amIwAqF6xWmZ5VrITpsW3UonM/sendMessage?chat_id=@ScraperSpider&text={x}")
    except:
        print("Tele Exception")
        
        
# Proxy details
proxy = {
    'http': 'http://marknavs:F2yK4Es3sHUNb3OE@proxy.packetstream.io:31112',
    "https": 'http://marknavs:F2yK4Es3sHUNb3OE@proxy.packetstream.io:31112'
}

# Connect to your MySQL server
def connect_to_mysql():
    while True:
        try:
            # Connect to MySQL server
            conn = mysql.connector.connect(
                host="161.35.226.120",
                user="akshay",
                password="et40kMqhGnRpsO6Z",
                database="sgp"
            )
            print("Connected to MySQL server successfully!")
            return conn
        except mysql.connector.Error as err:
            if err.errno == errorcode.CR_SERVER_LOST or err.errno == errorcode.CR_CONN_HOST_ERROR:
                print("Lost connection to MySQL server. Reconnecting...")
                time.sleep(10)  # Wait for 5 seconds before attempting to reconnect
            else:
                # Handle other MySQL errors
                print(f"MySQL Error: {err}")



#global conn
conn = connect_to_mysql()


def record_exists(cursor, registered_URL):
    query = "SELECT COUNT(*) FROM Company WHERE URL = %s"
    cursor.execute(query, (registered_URL,))
    result = cursor.fetchone()
    return result[0] > 0

def insert_data(dd_values):
    
    
    # Create a cursor object to interact with the database
    cursor = conn.cursor()
    
    
    val = tuple(dd_values)


    # SQL statement to insert data
    sql = "INSERT INTO Company (URL, RegisteredName, RegistrationNumber, UENIssueDate, UENStatus, LegalEntityType, LegalEntityTypeSuffix, Town) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

    # Check if record exists
    registered_URL = val[0]
    if not record_exists(cursor, registered_URL):
        # Insert record
        cursor.execute(sql, val)
        conn.commit()  # Commit each insertion


    # Close cursor and connection
    cursor.close()
    #

def db_check_data(Uri):
    cursor = conn.cursor()
    if record_exists(cursor, Uri):
        cursor.close()
        return True
    else:
        cursor.close()
        return False

def checkpoint(term):
    
    cursor = conn.cursor()

    # Prepare the SQL query to retrieve the page for the given term
    query = "SELECT page FROM Pagination WHERE term = %s"

    # Execute the SQL query with the given term
    cursor.execute(query, (term,))

    # Fetch the result (assuming only one record is expected)
    result = cursor.fetchone()
    
    cursor.close()
    
    if result:
        page = result[0]  # Extract the page from the result
        #print(f"Page for term '{term}': {page}")
        return page
    else:
        print(f"No page found for term '{term}'")


def set_page(term,page):
    
    cursor = conn.cursor()

    query = "UPDATE Pagination SET page = %s WHERE term = %s"
    #print(query)

    cursor.execute(query, (page,term))
    conn.commit()
    
    cursor.close()
    
    
def create_session(proxy):
    session = requests.Session()
    session.proxies.update(proxy)
    return session

session = create_session(proxy)


def fetch_data_with_retry(url, proxy):
    max_retries = 10
    retry_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            response = session.get(url, proxies=proxy, timeout=8)
            # Check if the response is successful
            if response is not None:
                if response.status_code == 200:
                    return response
                else:
                    print(f"Request failed with status code: {response.status_code}")
            else:
                print(f"REetrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                
        except ProxyError as e:
            print(f"ProxyError occurred: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay*attempt} seconds...")
                time.sleep(retry_delay*attempt)
            else:
                print("Max retries exceeded. Exiting...")
                return "Max"
        except Exception as e:
            print(f"An Proxy error occurred on {url}: {e}")
            #break


            
def stage3(company_link):
    
        #for index,company_link in enumerate(link_storage):
    url = company_link

    response = fetch_data_with_retry(url, proxy)

    if response == "Max":
        return

    soup = BeautifulSoup(response.text, 'html.parser')

    dl_tag = soup.find("dl")

    # Find all dd tags within the dl tag and get their values
    dd_values = [dd.get_text(strip=True) for dd in dl_tag.find_all("dd")][:-1]


                                                # Original date string
    date_str = dd_values[2]

    # Convert the date string to a datetime object
    date_obj = datetime.strptime(date_str, '%b %d, %Y')

    # Convert the datetime object to the 'YYYY-MM-DD' format
    formatted_date = date_obj.strftime('%Y-%m-%d')

    #print(formatted_date)  # Output: '2008-09-13'

    dd_values[2] = formatted_date

    dd_values.insert(0, company_link)

    insert_data(dd_values)
    
    #print("#"*20)
    #print(dd_values)
    #print("-----------------------------------------------------------------------")
    #print(url)
    #print("#"*20)

    #if index % 30 == 0:
    #print("Working Fine30")
        #time.sleep(10)

    #time.sleep(3)



def main(term,word_flag):
    
    #-----------------------------------------------------stage 1
    print("stage 1")
    link_storage = []


    # URL of the website you want to scrape
    url = f'https://companieshouse.sg/?term={term}'

    # Make a request using the proxy
    response = fetch_data_with_retry(url, proxy)
    
    
    if response == "Max":
        return word_flag

    # Parse the HTML content of the page
    soup = BeautifulSoup(response.text, 'html.parser')

    # Now you can extract whatever information you need from the parsed HTML
    # For example, let's extract all the links on the page
    a_tags = soup.find_all('a', class_='text-xs font-semibold text-blue-500 uppercase hover:text-blue-700')
    #pdb.set_trace()

    if len(a_tags) ==0:
        return word_flag

    # Print the extracted links
    for a_tag in a_tags:
        #print(a_tag['href'])
        if db_check_data(a_tag['href']):
            continue
        link_storage.append(a_tag['href'])
        stage3(a_tag['href'])
        #print(a_tag['href'])

    #--------------------------------------------------------stage 2
    print("stage 2")
    try:

        nav_tag = soup.select_one('nav[aria-label="Pagination Navigation"]')
        last_page = int(nav_tag.find_all('a')[-2].text)
        print(f"TotalPage {last_page}")
        tele(f"TotalPage {last_page}")
        
        starting_page = checkpoint(term)
        
        for page in range(starting_page,last_page+1):
            url = f'https://companieshouse.sg/?term={term}&page={page}'

            response = fetch_data_with_retry(url, proxy)
            
            if response == "Max":
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            a_tags = soup.find_all('a', class_='text-xs font-semibold text-blue-500 uppercase hover:text-blue-700')

            for a_tag in a_tags:
                #print(a_tag['href'])
                if a_tag['href'] in link_storage or db_check_data(a_tag['href']):
                    continue
                link_storage.append(a_tag['href'])
                stage3(a_tag['href'])
                #print(a_tag['href'])
                
            #time.sleep(2)
            print(page)
            set_page(term,page)
    except mysql.connector.Error as err:
        word_flag=False
        #print("Sql ecxption cha;a")
        #print(err)
        #if err.errno == errorcode.CR_SERVER_LOST or err.errno == errorcode.CR_CONN_HOST_ERROR or err.errno == 2006  or err.errno == 1053 or err.errno == -1:
        print(f"Lost connection to MySQL server. Going Back From Pagination: {err}")
        tele(f"Lost connection to MySQL server. Going Back From Pagination: {err}")
        #print(err)
                
    except Exception as e:
        print(f"pagination is not there: {e}")
        


    #--------------------------------------------------------stage 3     
    print("stage 3")
    if len(link_storage) ==0:  # new thing
        return word_flag
    


    #--------------------------------------------------------stage 4 
    print("stage 4")
    print(len(link_storage))
    link_storage.clear()
    
    return word_flag
    
    
def first():
    #max_retries = 5
    #retry_delay = 5  # seconds
    attempt = 0
    global conn
    while True:
        try:
            with open('3_letter_combinations.txt', 'a+') as file:
                file.seek(0)
                exist_lines = file.readlines()
                word_flag=True
                #print(exist_lines)
                for combination in combinations:
                    if f'{combination}\n' not in exist_lines:
                        print(combination)
                        tele(f'started :{combination}')
                        word_flag = main(combination,word_flag)
                        #print("woops")
                        
                        if not word_flag:
                            raise CustomError(f"Word Flag was triggered need to redo the word")
                        file.write(combination + '\n')
                        tele(f'finished :{combination}')

                        

                        #time.sleep(5)
        except mysql.connector.Error as err:
            tele(err)
            restart_mysql()
            time.sleep(5)
            if err.errno == errorcode.CR_SERVER_LOST or err.errno == errorcode.CR_CONN_HOST_ERROR or err.errno == 2006  or err.errno == 1053 or err.errno == -1:
                print(f"Trying to Establish Lost connection to MySQL server. Reconnecting...{err}")
                conn = connect_to_mysql()  # Reconnect to MySQL server
                attempt=0
                
            else:
                # Handle other MySQL errors
                print(f"MySQL Error: {err}")
                print(err.errno)
                attempt+=1
                if attempt == 5:

                    break
                    
        except CustomError as ce:
            tele(ce.message)
            print("Custom Error:", ce.message)
            restart_mysql()
            time.sleep(5)
            conn = connect_to_mysql()  # Reconnect to MySQL server
            
        except Exception as e:
            tele(e)
            # Handle the exception
            print(f"An error occurred: {e}")
            break

        finally:
            tele("Finally Stopeed working Please check")
            # Close the file explicitly
            file.close()
            #conn.close()
            #session.close()
            #break
        
first()
