import requests
from bs4 import BeautifulSoup
import time
import warnings
from datetime import datetime, timedelta
import csv
import re
import argparse
import traceback

#custom imports
# from JobDetail import JobDetail

#Ignore warnings shown while making api requests
warnings.filterwarnings('ignore')

# parser = argparse.ArgumentParser()
# parser.add_argument('--job_title', dest='job_title', type=str, help='Add job title you want to filter', required=False)
# parser.add_argument('--job_location', dest='job_location', type=str, help='Add job location you want to filter', required=False)
# parser.add_argument('--output_filename', dest='output_filename', type=str, help='Add Output file name', required=False)
# args = parser.parse_args()

# job_title, job_location = "Data Analyst", "Canada"
# job_title = args.job_title
# job_location = args.job_location
# output_filename = ""
# if args.output_filename is not None:
#     output_filename = args.output_filename
#     output_filename = output_filename[:output_filename.rfind('.')] if output_filename.find('.') > 0 else output_filename
job_title, job_location, output_filename = "Data Analyst", "Canada", "sample_output_9k_DA_CA"
job_details = []

class JobDetail:
        job_id = ""
        job_title = ""
        company_name = ""
        company_location = ""
        pay_range = ""
        job_level = ""
        employment_type = ""
        job_posted_datetime = ""
        industry = ""

def parse_relative_date(relative_str):
    current_date = datetime.now()

    # Split the relative string to get the numeric value and unit
    value, unit, _ = relative_str.split()
    value = int(value)

    # Determine the unit and calculate the timedelta accordingly
    if unit in ["day", "days"]:
        delta = timedelta(days=value)
    elif unit in ["hour", "hours"]:
        delta = timedelta(hours=value)
    elif unit in ["week", "weeks"]:
        delta = timedelta(weeks=value)
    elif unit in ["month", "months"]:
        delta = timedelta(days=value * 30)  # Approximate, assuming 30 days per month
    elif unit in ["year", "years"]:
        delta = timedelta(days=value * 365)  # Approximate, assuming 365 days per year
    else:
        return ""

    # Calculate the new date
    return (current_date - delta).isoformat(sep=" ", timespec="seconds")

def parse_job_details(job_id, attempt = 1):
    try:
        job_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
        if attempt > 10:
            print(f"Could not get proper response from api even after 10 attempts!!! Skipping this job_id => {job_id}")
            return

        time.sleep(0.5)
        job_detail_response = requests.get(job_url, verify=False)
        #print(f"Getting job details attempt - {attempt} and got response - {job_detail_response.status_code}")
        if job_detail_response.status_code == 200:

            job_details_soup = BeautifulSoup(job_detail_response.text, "html.parser")
            jd_job_title_tag = job_details_soup.find("h2", {"class": "top-card-layout__title font-sans text-lg papabear:text-xl font-bold leading-open text-color-text mb-0 topcard__title"})
            jd_company_location_tag = job_details_soup.find("span", {"class" : "topcard__flavor topcard__flavor--bullet"})
            # jd_company_name_tag = job_details_soup.find("a", {"class": "topcard__org-name-link topcard__flavor--black-link"})
            jd_company_name_tag = jd_company_location_tag.find_previous_sibling("span")
            jd_job_criteria_li_tags = job_details_soup.find("ul", {"class", "description__job-criteria-list"}).find_all("li")
            jd_pay_range_tag = job_details_soup.find("div", {"class", "salary compensation__salary"})
            # jd_job_posted_datetime_tag = job_details_soup.find("span", {"class", "posted-time-ago__text posted-time-ago__text--new topcard__flavor--metadata"})
            jd_job_posted_datetime_tag = jd_company_location_tag.find_parent("div").find_next_sibling("div").findChild("span")

            #Adding data to JobDetail object
            job_detail = JobDetail()
            job_detail.job_id = job_id
            job_detail.job_title = jd_job_title_tag.text.strip()
            job_detail.company_name = jd_company_name_tag.text.strip()
            job_detail.company_location = jd_company_location_tag.text.strip()
            job_detail.job_level = jd_job_criteria_li_tags[0].find("span").text.strip()
            job_detail.employment_type = jd_job_criteria_li_tags[1].find("span").text.strip()
            job_detail.industry = jd_job_criteria_li_tags[3].find("span").text.strip()
            try:
                job_detail.pay_range = jd_pay_range_tag.text.strip()
            except:
                job_detail.pay_range = ""
            job_detail.job_posted_datetime = parse_relative_date(jd_job_posted_datetime_tag.text.strip())

            job_details.append(job_detail)
            return
        else:
            attempt += 1
        #Call function with same job id for another attempt
        parse_job_details(job_id, attempt)
    except Exception as ex:
        print(f"Exception inside parse_job_details function - {ex}")
        print(traceback.format_exc())

def write_output(write_mode):
    #writing the list of job_detail object to csv file
    mode = "w" if write_mode == "header" else "a"
    with open(output_filename + ".csv", mode=mode, newline='', encoding="utf-8") as file:
        writer = csv.writer(file, delimiter='~')
        if(write_mode == "header"):
            # Write header manually (if needed)
            writer.writerow(['job_id','job_title','company_name', 'company_location', 'pay_range','job_level','employment_type','job_posted_date','industry'])
        else:
            # Write job data
            for jd in job_details:
                writer.writerow([jd.job_id,jd.job_title, jd.company_name, jd.company_location, jd.pay_range, jd.job_level, jd.employment_type, jd.job_posted_datetime,jd.industry])

def process_jobs():
    processed_job_count = 0
    #Assuming each page contains 10 records, divide the start_position(page_num) by 10 to loop over
    print(f"Found {job_count} jobs which will loop {job_count/10} times")
    start_position = 1
    reattempt = 1
    while start_position <= (job_count/10) and reattempt <= 50:
        #jobs_list_url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=Data%2BAnalyst&location=United%2BStates&start={start_position}"
        jobs_list_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"

        #Add start position(or page number) param in the query api
        queryParams["start"] = start_position
        #Wait before making request
        # time.sleep(1)

        if len(job_details) > 100:
            write_output("data")
            job_details.clear()

        time.sleep(0.5)
        job_list_response = requests.get(jobs_list_url, verify=False, params = queryParams)

        if job_list_response.status_code == 200:
            # if start_position > 100: break
            reattempt = 1
            jobs_list_soup = BeautifulSoup(job_list_response.text, "html.parser")
            jobs_list = jobs_list_soup.find_all("li")
            print(f"Found {len(jobs_list)} more jobs in page - {start_position} at {datetime.now()}. Processed job count {processed_job_count}")
            for job_detail in jobs_list:
                try:
                    base_card_div = job_detail.find(attrs={"class": "base-card"})
                    job_id = base_card_div.get("data-entity-urn").split(":")[3]
                    parse_job_details(job_id)
                    processed_job_count += 1
                    #wait 1 second before continuing the loop
                    # time.sleep(1)
                except Exception as ex:
                    print(f"Error occured -> {ex}")
        else:
            print(f"Reponse code => {job_list_response.status_code} , Current Start position(page) => {start_position} , attempt -> {reattempt}, time {datetime.now()}")
            start_position -= 1
            reattempt += 1

        start_position += 1
    print(f"Processed jobs - {processed_job_count}")

if __name__ == "__main__":
    # start execution
    start_time = datetime.now()
    print(f"Execution started at {start_time}")

    queryParams = {}
    if(job_title != ""):
        print(f"Job title search condition is: {job_title}")
        queryParams["keywords"] = job_title

    if(job_location != ""):
        print(f"Job location search condition is: {job_location}")
        queryParams["location"] = job_location

    if(output_filename == ""):
        output_filename = "sample_output"

    queryParams["pageNum"] = 0

    job_count = 0
    #Write header to file
    print(f"Output filename is {output_filename}")
    write_output("header")

    #Finding the number of jobs
    tryattempt = 0
    while tryattempt < 10:
        # initial_url = "https://www.linkedin.com/jobs/search?keywords=Data%20Analyst&location=United%20States&position=1&pageNum=0"
        initial_url = "https://www.linkedin.com/jobs/search"
        initial_url_response = requests.get(initial_url, verify=False, params=queryParams)
        if(initial_url_response.status_code == 200):
            init_soup  = BeautifulSoup(initial_url_response.text, "html.parser")
            job_count_txt = init_soup.find("span", {"class", "results-context-header__job-count"}).text
            #Get only the numbers from job count text
            job_count = int(re.sub(r"[^0-9]","",job_count_txt))
            break
        else:
            tryattempt += 1
    #Delete pageNum from the query params for other requests after getting the number of jobs
    del queryParams["pageNum"]

    try:
        process_jobs()
    except Exception as ex:
        print(f"Error occured -> {ex}")

    #If remaining data in job_details, append to file
    if(len(job_details)) > 0 :
        write_output("data")

    print(f"Execution completed at {datetime.now()}")
    print(f"Execution completed in {datetime.now() - start_time}")
