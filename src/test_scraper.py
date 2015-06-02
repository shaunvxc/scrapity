'''
Created on May 30, 2015

@author: shaun.viguerie
'''
from bs4 import BeautifulSoup
import urllib2
from multiprocessing import Process, Queue
import re
from __builtin__ import False
from constants import *
import sys
# Needed  to prevent 403 error on Wikipedia
header = {'User-Agent': 'Mozilla/5.0'}  
# the number of processes to run, in general '1 process per core' yields optimal performance
num_processes = 2
# process table
procs = []
# switch on/off the verbose flag to disable/enable logging
verbose = False
'''
    Top-level method, takes a link as argument, though this script has been fairly specifically
    catered to start @ "http://en.wikipedia.org/wiki/Academy_Award_for_Best_Picture#1920s"
'''
def scrape_multithread(wiki):
    print "Scraping..."
    soup = get_soup(wiki)
    tables = soup.find_all("table", { "class" : "wikitable"})

    work_queue = Queue()
    done_queue = Queue()
    
    spawn_processes(work_queue, done_queue)

    for table in tables:
        # The winner is always given in a separate color than the other cells,
        # by specifying the style to be this color in our find()- we reduce the search
        # space to be ONLY data that we want, at the expense of RELYING on this color
        # remaining the same going forward.   
        # -- An alternate approach would be go work around the ordering of the winners/nominees through the tables.
        for row in table.find_all("tr", style="background:" + WINNER_COLOR) :
            year = get_year_safe(table)
            cells = row.find_all("td")
            if(len(cells) == 3):
                if(cells[0].find('a', href=True) is not None):
                    info = cells[0].find('a', href=True)
                    t = info['href'], info.getText(), year;
                    work_queue.put(t, True)
                    break
    
    for x in range (0, num_processes):
        work_queue.put(QUEUE_SENTINEL)
    
    join_processes()
    collect_results(done_queue)

'''
    Handles result collection-- pulls result tuples from done_queue as they come in, computes a Running Average (to summing large numbers), 
    and places the entries into a list.
    
    Once the queue has been emptied, the list is sorted based on the YEAR (tuple[0]), and the entries are then printed 
    in <YEAR TITLE BUDGET CURRENCY> order.   
    
    Two averages are then printed: the average budget for all winning titles (American and Foreign)
                                   the average budget for only titles with USD budgets
                                   
    Wasn't sure about how to best handle the currency conversions here, given that exchange rates in the 40s were not the same
    as they are now.  However, it would be rather easy to convert these values using todays exchange rates.
'''    
def collect_results(done_queue):
    if verbose:
        print "Collecting results"
    
    data = []
    running_average = 0
    non_usd_sum = 0
    non_usd_count = 0
    n = 0 
    procs_finished = 0 

    while True:
        result = done_queue.get()
        if(result == QUEUE_SENTINEL):
            procs_finished = procs_finished +1 
            # only break out when all running processes have finished!
            if procs_finished == num_processes:
                break
        elif result is not None:
            data.append(result)
            # only increment running_average values if a budget was scraped
            if str(result[2]) != NOT_FOUND:
                n = n + 1
                running_average = running_average + ( (result[2] - running_average) / n)
                
                # for factoring the NON-USD values out of the average at the end. 
                if(result[3] != USD):
                    non_usd_sum = non_usd_sum + result[2]
                    non_usd_count = non_usd_count + 1
     
    # after multiprocessing, data won't be perfectly ordered, 
    # so sort the data by year (tup[0]               
    data.sort(key=lambda tup: tup[0], reverse=False)
    for entry in data:
        print entry[0] + ", " + entry[1] + ", " + str(entry[2]) + ", " + entry[3] 
    
    # output results
    print "Average Budget = " + str(running_average)
    
    if non_usd_count > 0:
        usd_only_average = ((running_average * n) - non_usd_sum) / (n - non_usd_count)
        print "Average Budget USD Only = " + str(usd_only_average)
                       

'''
    Spawn worker processes
'''
def spawn_processes(queue, done_queue):

    for x in range(0, num_processes):
        p = Process(target=process_links, args=(queue,done_queue,x))
        if verbose:
            print "Spawning process " + str(x) 
        
        p.start()
        procs.append(p)
        
'''
    It is best practice to call join() on running procs to ensure that the processes
    safely complete before the program exits.
'''
def join_processes():
    for proc in procs:
        proc.join()
        
'''        
     this implementation for get year relies on the formatting of the current wiki pages
     and DOES not perform safety checks on the patterns (If safety is desired over performance, use
     get_year_safe(table)
     
     @return year
'''
def get_year(table):
        # get the year
        year_caption = table.find("caption", style="text-align:left").find("big").find_all("a")
        if len(year_caption) >= 2:
            return year_caption[0].getText() + "/" + year_caption[1].getText()
        else:
            return  year_caption[0].getText()  

'''      
     safer get_yeear implementation, makes sure that the strings are all numbers.
     
     @NOTE I am currently using this implementation because, given that for the first several years the Academy
           Awards actually spanned 2 years, so it is necessary to capture more than just 1 year in those cases.
           The above implementation, when handling this edge case, would sometimes pick up subscripts as additional years.  
   
    @return year
'''
def get_year_safe(table):
        year_caption = table.find("caption", style="text-align:left").find("big").find_all("a")
        year = ""
        for a in year_caption:
            raw = a.getText()
            #make sure there are no special chars in the year string-- should be only numbers!
            if re.search("r'[^0-9]+'", raw, 0) is None:
                if len(raw) == 2 or len(raw) == 4:
                    year = year + raw + "/"
            else:
                break
        
        if year != "":
            return year.rstrip("/")
        return year     

'''
    Per-process method that continuously pulls an entry from the work_queue, processes the link, 
    and puts the result on the 'done_queue'.  This will continue until a SENTINEL is dequeued, in
    which the process will notify the DONE queue that it is finished by pushing a SENTINAL
    onto the done_queue.
'''
def process_links(worker_queue, done_queue, id):
    while True:
        link = worker_queue.get()
        if(link == QUEUE_SENTINEL):
            if verbose:
                print "Process " + str(id) + " Complete"
            done_queue.put(QUEUE_SENTINEL)
            break
        elif link is not None:
            done_queue.put(process_link(link), True)
            
'''
    This method takes the link snipped grapped from the initial pass thru the Wiki list page, 
    searches for the Budget information, and calls normalize() on the extracted text.
        
    @return a tuple (year, title, budget, currency)
'''
def process_link(link):
    wiki = "http://en.wikipedia.org" + link[0]
    soup = get_soup(wiki)
    side_table = soup.find("table", {"class" : "infobox vevent"})
    budgetRow = side_table.find("th", text="Budget")
    
    if budgetRow is not None:
        budgetEntry = budgetRow.parent.find("td")
        if budgetEntry is not None:
            dirty_budget = budgetEntry.getText()
            norm_info = normalize(dirty_budget)
            return (link[2], link[1], norm_info[0], norm_info[1])
        else:
            return (link[2], link[1], NOT_FOUND, "")
    else:
        return (link[2], link[1], NOT_FOUND, "")



'''
    This method takes in a raw budget string, cleans it up, and normalizes it into millions.
    
    This  method does the following:
        
        1. Strip out invalid characters and replaces them with allowable substitutes
        
        2. Determines if the number is given in ONES (ie 1,000,000) or MILLIONS (ie 1.2 million)
        
        3. Checks for cases in which the budget is given in multiple denominations, ie ($1 million or 600,000GBP)
           In these cases, we hold onto the USD values.  
        
        4. Grab the currency denomination of the budget (for all cases)-- currently, this data only has  USD and GBP
           data points.  In order to support additional currencies, one only needs to add the desired currency to 
           get_currency(), and multi_currencies(). 
        
        5. Check for budgets expressed as ranges (i.e. $16-18 million).  In these cases, the average of the two given 
           numbers is returned
        
        6. Lastly, we remove all unneeded characters with regexps to get at the raw numeric content
        
        7. If the millions flag WAS set, we return multiply the number by 1000000 before returning
        
    @return Tuple (normalized_budget, Currency)
'''
def normalize(budget):
    
    hasMillions = False
    ccy = ""
    
    # remove any bad characters from the string before normalization
    budget = clean_str(budget)
    
    # check for millions suffix
    if re.search(MILLION, budget, 0) is not None:
        hasMillions = True    
    
    if has_multi_currencies(budget):
        budget = get_usd_rep(budget)
        ccy = USD
    else:
        ccy = get_currency(budget)
    
    # if the budget is given as a range of numbers, take the average!
    if is_ranged_budget(budget):
        budget = str(get_ranged_budget(budget))    
    
    temp = re.sub("\[[0-9]{1,2}\]", "", budget, 3)
    clean = re.sub(r'[^0-9.]+', '', temp)
    
    if hasMillions == True: 
        return (1000000 * float(clean), ccy )
    
    return (float(clean), ccy)


'''
    Replaces any illegal characters with allowable substitutes.
    
    To see the current (illegal_char, allowed_substitute) mappings, 
    check the init_invalid_chars() method.
'''
def clean_str(budget):
    for pair in invalid_chars:
        if re.search(pair[0], budget):
            budget = re.sub(pair[0], pair[1], budget)
        
    return budget   
    
'''
   To detect a ranged budget, we look for a sequence of Number-separator-Number. 
   As of now, the only needed separator in this data set is the '-' character.
   
   To support additional separators, the Regular Expression can be updated to 
   be a character class as such [\-, etc]
   
   @refer to contants.py to see the RANGED_BUDGET_REGEXP
'''
def is_ranged_budget(budget):
    return (re.search(RANGED_BUDGET_REGEXP, budget, 0) is not None) 
         
'''
    To handle the edge case of ranged budgets, we use the same regexp used in
    is_ranged_budget(), and then use Python's grouping functionality to get
    at the numeric groups and average them. 
   
    @refer to contants.py to see the RANGED_BUDGET_REGEXP
    @return average of the two numbers in the range
'''
def get_ranged_budget(budget):    
    match = re.search(RANGED_BUDGET_REGEXP, budget, 0)
    
    first = match.group(1)
    second = match.group(2)
    return (float(first) + float(second)) / 2

'''
    @return the currency found in the string, to support more, simply add to this function.
    
    In a more diverse data set, it would make sense to use a list or a dictionary for this sort of thing.
'''
def get_currency(budget):
    if budget.find("$") != -1:
        return USD
    elif budget.find(GBP) != -1:
        return GBP
    
    return CCY_NOT_FOUND

'''
    Check if a budget string has more than 1 currency representation.  
    
    In this data set, the only cases of this were of USD & GBP appearing together, so 
    this is a little contrived, but accomplishes the task.
    
    To make it  more scalable, a list of currencies could be used as well as a counter.
'''
def has_multi_currencies(budget):
    if str(budget).find("$") != -1 and str(budget).find(GBP) != -1:
        return True
    return False

'''
    @return USD representation of the budget, for cases in which multiples are given (ie $1 million, or GBP 600000)
    
    To get at the USD, we simply partition the string on the USD symbol.  Partition will give us a tuple (before, USD, after).
    
    Common convention dictates that the number will appear directly after USD, so we split up tuple[2] using ' ' as the delim, 
    ASSUMING that the character immediately following it will be the USD number.  
    
    One could use a regexp.match('[0-9]{1,}', usd_piece[0]) to make this method safer, but for now it is working fine.
'''
def get_usd_rep(budget):
    pieces = str(budget).partition("$")
    usd_piece = pieces[2].split(' ')
    return usd_piece[0]
    
'''
    Utility method to BeautifulSoup representation of HTML content
'''
def get_soup(url):
    req = urllib2.Request(url, headers=header)
    page = urllib2.urlopen(req)
    soup = BeautifulSoup(page)
    return soup

'''
    Function to remove invalid characters from the scraped budget strings, and replace them with
    their eligible counterparts.
    
    Any other invalid characters encountered during processing can be added to this list so that 
    the invalid characters will be removed & replaced with safe chars before any further processing
    is done.
'''
def init_invalid_chars():
    ret = []
    ret.append((u"\u2013", "-")) # weird non unicode dash
    ret.append((u"\u00A0", " ")) # non-breaking spaces
    ret.append((u"\u00A3", GBP)) # out of bounds ascii GB Pound symbol
    return ret


if sys.argv[len(sys.argv)-1].isdigit():
    num_processes =int(sys.argv[len(sys.argv) -1])

# build the invalid character-allowable substitute character set
invalid_chars = init_invalid_chars( )
scrape_multithread("http://en.wikipedia.org/wiki/Academy_Award_for_Best_Picture#1920s")
