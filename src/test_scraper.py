'''
Created on May 30, 2015

@author: shaun.viguerie
'''
from bs4 import BeautifulSoup
import urllib2
from multiprocessing import Process, Queue
import time
import re
from __builtin__ import False

# Needed  to prevent 403 error on Wikipedia
header = {'User-Agent': 'Mozilla/5.0'}  

# for tuning the number of processes to run-- 
# in general '1 process per core' yields optimal performance
num_processes = 2
# process table, for managing
procs = []

def scrape():

    wiki = "http://en.wikipedia.org/wiki/Academy_Award_for_Best_Picture#1920s"
    soup = get_soup(wiki)
    tables = soup.find_all("table", { "class" : "wikitable"})
    winner_links = []

    for table in tables:
        for row in table.find_all("tr", style="background:#FAEB86") :
            cells = row.find_all("td")
            if(len(cells) == 3):
                if(cells[0].find('a', href=True) is not None):
                    winner_links.append(cells[0].find('a', href=True)['href'])
                    break

    for link in winner_links:
        process_link(link)

def scrape_multithread():
    wiki = "http://en.wikipedia.org/wiki/Academy_Award_for_Best_Picture#1920s"
    soup = get_soup(wiki)
    tables = soup.find_all("table", { "class" : "wikitable"})

    queue = Queue()
    spawn_processes(queue)

    for table in tables:
        for row in table.find_all("tr", style="background:#FAEB86") :
            cells = row.find_all("td")
            if(len(cells) == 3):
                if(cells[0].find('a', href=True) is not None):
                    info = cells[0].find('a', href=True)
                    t = info['href'], info.getText();
                    queue.put(t, True)
                    break
    
    for x in range (0, num_processes):
        queue.put("DONE")
    
    join_processes()


def spawn_processes(queue):
    # pass the start_time to the process so that it can time its execution from start to finish
    for x in range(0, num_processes):
        p = Process(target=process_links, args=(queue,))
        p.start()
        procs.append(p)
        
def join_processes():
    print "Joining processes!"
    for proc in procs:
        proc.join()

def process_links(queue):
    while True:
        link = queue.get()
        if(link == "DONE"):
            break
        elif link is not None:
            process_link(link)  
            

def process_link(link):
    wiki = "http://en.wikipedia.org" + link[0]
    soup = get_soup(wiki)
    side_table = soup.find("table", {"class" : "infobox vevent"})
    budgetRow = side_table.find("th", text="Budget")
    
    if budgetRow is not None:
        budgetEntry = budgetRow.parent.find("td")
        if budgetEntry is not None:
            dirty_budget = budgetEntry.getText()
            print link[1] + ", raw_budget= " + dirty_budget + " normalized_budget= " + str(normalize(dirty_budget))     
        else:
            print "BUDGET ENTRY NOT FOUND: " + wiki
    else:
        print "BUDGET NOT FOUND: " + wiki

def normalize_budgets(budget_str):
    budget_str.replace("\\[[0-9]\\]", "")

def get_soup(url):
    req = urllib2.Request(url, headers=header)
    page = urllib2.urlopen(req)
    soup = BeautifulSoup(page)
    return soup

def normalize(budget):
    
    hasMillions = False
    #isUSD = False
    
    if re.search("million", budget, 0) is not None:
        hasMillions = True                 
    
    #if re.search("\$", budget, 0) is not None:   
    #   isUSD = True
    
    temp =  re.sub("\[[0-9]{1,2}\]", "", budget, 3)
    clean = re.sub(r'[^0-9.]+', '', temp)
    if hasMillions == True: 
        return 1000000 * float(clean)
    
    return float(clean)

def test_normalize(budget):
    temp =  re.sub("\[[0-9]{1,2}\]", "", budget, 2)
    temp = re.sub("\$", "", temp, 1)
    if re.search("million", temp, 0) is not None:
        return (1000000 * float(re.sub("million","", temp, 1)))
    
    return re.sub("\[[0-9]{1,2}\]", "", budget, 2)
    
    
start_time = time.time()
scrape_multithread()


#print normalize("$27 million[2]")