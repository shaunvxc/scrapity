'''
Created on May 28, 2015
 

   # tree = html.fromstring(page.text)
   # prices = tree.xpath('//span[@class="item-price"]/text()')

@author: shaunviguerie
'''
from bs4 import BeautifulSoup
import urllib2
from multiprocessing import Process, Queue

header = {'User-Agent': 'Mozilla/5.0'} #Needed to prevent 403 error on Wikipedia

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
        process_link( link)
    

def scrape_multithread():
    
    wiki = "http://en.wikipedia.org/wiki/Academy_Award_for_Best_Picture#1920s"
    
    soup = get_soup(wiki)
    tables = soup.find_all("table", { "class" : "wikitable"})
    
    queue =  Queue()
    spawn_processes(queue)
    
    for table in tables:
        for row in table.find_all("tr", style="background:#FAEB86") :
            cells = row.find_all("td")
            if(len(cells) == 3):
            
                if(cells[0].find('a', href=True) is not None):
                    queue.put(cells[0].find('a', href=True)['href'], True)
                    break
                
    
    queue.put("DONE")

def spawn_processes(queue):
    p1 = Process(target = process_links, args=(queue,))
    p1.start()
    
    p2 = Process(target = process_links, args=(queue,))
    p2.start()
    

def process_links(queue):
    while True:
        link = queue.get()
        if(link == "DONE"):
            break
        elif link is not None:
            process_link(link)  

def process_link(link):
    wiki = "http://en.wikipedia.org" + link
    soup = get_soup(wiki)
    side_table = soup.find("table", {"class" : "infobox vevent"})
    
    budgetRow = side_table.find("th", text="Budget")
   
    if budgetRow is not None:
        budgetEntry= budgetRow.parent.find("td")
        if budgetEntry is not None:
            print link + ", budget=" + budgetEntry.getText()         
        else:
            print "BUDGET ENTRY NOT FOUND: " + wiki
    else:
        print "BUDGET NOT FOUND: " + wiki

def get_soup(url):
    req = urllib2.Request(url,headers=header)
    page = urllib2.urlopen(req)
    soup = BeautifulSoup(page)
    return soup

scrape_multithread()
