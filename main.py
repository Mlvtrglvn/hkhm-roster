
# coding: utf-8

# In[1]:


from collections import defaultdict
import requests
import json
import re
import pandas as pd
import time
import datetime
import re


# In[2]:


api_key = '3d7b334537c5f57594153385732f72'
group_urlname = 'hongkonghikingmeetup'
status_list = ['waitlist', 'yes', 'attended', 'noshow', 'excused', 'no']


# In[3]:


def make_request(url, parameters, suppress_output=False):
    """
    Make request to Meetup API. Handles multiple pages and the throttling limit.
    Expect list as json return type.
    :return List of json objects
    """
    try:
        page = 0
        if not suppress_output:
            print('Fetching page %d' % page)
        response = requests.get(url, params=parameters)
        response_json = response.json()
        if not suppress_output:
            print('Fetched %d' % len(response_json))
        # Pause the requests if near to the time limit imposed by Meetup API
        if int(response.headers['X-RateLimit-Remaining']) < 2:
            time.sleep(int(response.headers['X-RateLimit-Reset']) + 1)
        if not response.ok:
            print(response.text)
            raise ConnectionError('Request Failed')
        # Handle multiple pages
        links_used = [url]
        while 'Link' in response.headers:
            page += 1
            if not suppress_output:
                print('Fetching page %d' % page)
            mtc = re.search(r'<(https.+?)>; rel="next"', response.headers['Link'])
            if mtc is not None:
                next_page_url = mtc.group(1).strip()
                if next_page_url in links_used:  # Link already visited (possible bug of Meetup API)
                    break
                links_used.append(next_page_url)
                response = requests.get(next_page_url, params=parameters)
                if not response.ok:
                    print(response.text)
                    raise ConnectionError('Request Failed')
                if int(response.headers['X-RateLimit-Remaining']) < 2:
                    time.sleep(int(response.headers['X-RateLimit-Reset']) + 1)
                response_json += response.json() # Assume both types are lists
                print('Fetched %d' % len(response_json))
                if 'page' in parameters and len(response.json()) < parameters['page']:  # Fix Meetup bug in event fetching
                    break
            else:
                break
        return response_json
    except ConnectionError:
        return []


# # Extract member information

# In[4]:


members_url = 'https://api.meetup.com/hongkonghikingmeetup/members'
member_list = {}
parameters = {
    'key': api_key
}
response = make_request(members_url, parameters)

for mem in response:
    try:
        member = {}
        member['id'] = mem['id']
        member['name'] = mem['name']
        member['designation'] = ''
        if 'title' in mem['group_profile']:
            member['designation'] = mem['group_profile']['title']
        member['role'] = ''
        if 'role' in mem['group_profile']:
            member['role'] = mem['group_profile']['role'] 
        member['joined'] = mem['group_profile']['created']
        for s in status_list:
            member[s] = 0
        member['noshow_times'] = []
        member['events_attended'] = []
        member_list[member['id']] = member
    except TypeError:
        continue
        
len(member_list)


# # Extract event information

# In[23]:


events_url = 'https://api.meetup.com/hongkonghikingmeetup/events'
events_list = {}
event_hosts = set()
parameters = {
    'key': api_key,
    'status': 'past', 
    'fields': 'event_hosts',
    'page': 200
}
response = make_request(events_url, parameters)
for ev in response:
    event = {}
    event['id'] = ev['id']
    event['title'] = ev['name']
    event['time'] = ev['time']
    if 'event_hosts' in ev:
        event['host'] = [eh['name'] for eh in ev['event_hosts']]
    else:
        event['host'] = ['UNKNOWN']
    event['yes'] = ev['yes_rsvp_count'] if 'yes_rsvp_count' in ev else 0
    event['no'] = ev['no_rsvp_count'] if 'no_rsvp_count' in ev else 0
    event['waitlist'] = ev['waitlist_count'] if 'waitlist_count' in ev else 0
    event['attended'] = 0
    event['absent'] = 0
    event['noshow'] = 0
    event['link'] = ev['link']

    events_list[event['id']] = event


# In[ ]:





# # Extract attendance information

# In[7]:


for ix, eid in enumerate(events_list):
    if ix % 50 == 0:
        print('Processed %f%%' % (ix * 100. / len(events_list)))
    all_responses = []
    rsvps_url = 'https://api.meetup.com/hongkonghikingmeetup/events/%s/attendance' % eid
    for s in ['attended', 'noshow']:
        parameters = {
            'key': api_key,
            'filter': s
        }
        response = make_request(rsvps_url, parameters, suppress_output=True)
        all_responses += response
    all_responses = [r for r in all_responses if r['status'] in status_list]

    for res in all_responses:
        member_id = res['member']['id']
        status = res['status']
        
        if member_id in member_list:
            member_list[member_id][status] += 1
            if status == 'noshow':
                member_list[member_id]['noshow_times'].append(events_list[eid]['time'])
                events_list[eid]['noshow'] += 1
            elif status == 'attended':
                member_list[member_id]['events_attended'].append(eid)
                events_list[eid]['attended'] += 1
             


# # Generate member table

# In[11]:


def get_events_attended_time(user):
    return [events_list[a]['time'] for a in user['events_attended']]
def get_noshows_threemonths(user):
    return len(list(filter(lambda x: x > (datetime.datetime.now() - datetime.timedelta(days=90)).timestamp() * 1000,
                     user['noshow_times'])))
member_final_list = list(member_list.values())
member_ids = [a['id'] for a in member_final_list]
member_names = [a['name'] for a in member_final_list]
member_designations = [a['designation'] for a in member_final_list]
member_roles = [a['role'] for a in member_final_list]
member_joined = [pd.to_datetime(a['joined'], unit='ms') for a in member_final_list]
member_hike_count = [a['attended'] for a in member_final_list]
member_last_hike = [pd.to_datetime(max(get_events_attended_time(a)), unit='ms') if len(get_events_attended_time(a)) > 0 else ''
                    for a in member_final_list]
member_noshows = [a['noshow'] for a in member_final_list]
member_noshows_threemonth = [get_noshows_threemonths(a) for a in member_final_list]

df = pd.DataFrame({
    'Id': member_ids,
    'Username': member_names,
    'Designation': member_designations,
    'Role': member_roles,
    'Joined': member_joined,
    'Hike count': member_hike_count,
    'Last hike': member_last_hike,
    'Noshows': member_noshows,
    'Noshows last three months': member_noshows_threemonth
})
parsed = [re.search(r'([0-9,]+?)\+', item) for item in df['Designation']]
df['Numerical designation'] = [int(p.group(1).replace(',', '')) if p is not None else 0 for p in parsed]
df['Needs update'] = ['YES' if (hikes >= designation + 50 and designation >= 50) or (hikes >= 50 and designation == 30) or 
                      (hikes >= 30 and designation == 0) else '' 
                      for hikes, designation in zip(df['Hike count'], df['Numerical designation'])]
df['Check noshows'] = ['YES' if ns3m >= 2 else '' 
                      for ns3m in df['Noshows last three months']]
df = df.reindex_axis(['Id', 'Username', 'Designation', 'Role', 'Joined', 'Last hike', 'Hike count', 'Noshows', 'Noshows last three months', 'Numerical designation', 'Needs update', 'Check noshows'], axis=1)
df = df.sort_values('Hike count', ascending=False)
df.to_csv('~/Dropbox/hiking_members_20170724.csv')
df.to_excel('/home/brtdra/Dropbox/hiking_members_20170724.xlsx')


# # Generate events table

# In[24]:


events_final_list = list(events_list.values())
events_ids = [a['id'] for a in events_final_list]
events_titles = [a['title'] for a in events_final_list]
events_hosts = [a['host'] for a in events_final_list]
events_times = [pd.to_datetime(a['time'], unit='ms') for a in events_final_list]
events_links = [a['link'] for a in events_final_list]
events_yes = [a['yes'] for a in events_final_list]
events_no = [a['no'] for a in events_final_list]
events_waitlist = [a['waitlist'] for a in events_final_list]
events_attended = [a['attended'] for a in events_final_list]
events_noshows = [a['noshow'] for a in events_final_list]

events_df = pd.DataFrame({
    'Id': events_ids, 
    'Title': events_titles,
    'Host(s)': events_hosts,
    'Time': events_times,
    'Link': events_links,
    'Yes': events_yes,
    'Attended': events_attended,
    'Noshows': events_noshows
})

events_df = events_df.reindex_axis(['Id', 'Title', 'Host(s)', 'Time', 'Link', 'Yes', 'Attended', 'Noshows'], axis=1)
events_df = events_df.sort_values('Time', ascending=False)
events_df.to_csv('~/Dropbox/hiking_events_20170724.csv')
events_df.to_excel('/home/brtdra/Dropbox/hiking_events_20170724.xlsx')

