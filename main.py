#!/usr/bin/python3
# coding: utf-8
from collections import defaultdict
import requests
import json
import re
import pandas as pd
import time
import datetime
import re

api_key = '3d7b334537c5f57594153385732f72' # Fill here the api key obtained from Meetup website
group_urlname = 'hongkonghikingmeetup' # Fill here the group urlname (*** in www.meetup.com/***)
status_list = ['waitlist', 'yes', 'attended', 'noshow', 'excused', 'absent', 'no']


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


def extract_member_data(urlname, api_key):
    """ Extract member lists and data, excluding events attended """
    members_url = 'https://api.meetup.com/%s/members' % urlname
    members_list = {}
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
            member['challenge'] = 0.0
            members_list[member['id']] = member
        except (TypeError, KeyError):
            continue
        
    return members_list


# # Extract event information

def extract_event_data(urlname, api_key):
    """ Extract events list and data. Does not include attendance. """
    events_url = 'https://api.meetup.com/%s/events' % urlname
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
        event['rating'] = 2.0  # Default
        event['link'] = ev['link']

        score_re = re.search(r'(\d\.\d+)', event['title'])
        if score_re is not None:
            score = float(score_re.group(1))
            event['rating'] = score

        events_list[event['id']] = event
        
    return events_list

# # Extract attendance information

def extract_update_attendance_data(urlname, api_key, members_list, events_list):
    """ Update events list and members list with attendance and noshow count.
    Due to the Meetup api, the call is very slow. Does not download counts other than attended and noshow for this reason. """
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
                                
        for res in all_responses:
            try:
                member_id = res['member']['id']
            except KeyError:
                print('\nERROR: ')
                print(res)
                continue
            if 'status' in res.keys():
                status = res['status']
            else:
                status = res['rsvp']['response']
        
            if member_id in members_list:
                # members_list[member_id][status] += 1
                if status == 'noshow':
                    members_list[member_id]['noshow_times'].append(events_list[eid]['time'])
                    events_list[eid]['noshow'] += 1
                elif status == 'yes' or status == 'attended':  # Used to be 'attended'
                    members_list[member_id]['attended'] += 1
                    members_list[member_id]['events_attended'].append(eid)
                    events_list[eid]['attended'] += 1
                    if datetime.datetime.fromtimestamp(events_list[eid]['time'] / 1000) >= datetime.datetime(2018, 2, 1):
                        members_list[member_id]['challenge'] += events_list[eid]['rating']

def generate_member_excel(members_list, events_list, output_csv, output_excel):
    """ Generate csv and excel list of members. """
    def get_events_attended_time(user):
        return [events_list[a]['time'] for a in user['events_attended']]
    def get_noshows_threemonths(user):
        return len(list(filter(lambda x: x > datetime.datetime(2017, 9, 15).timestamp() * 1000, user['noshow_times'])))
    members_final_list = list(members_list.values())
    member_ids = [a['id'] for a in members_final_list]
    member_names = [a['name'] for a in members_final_list]
    member_designations = [a['designation'] for a in members_final_list]
    member_roles = [a['role'] for a in members_final_list]
    member_joined = [pd.to_datetime(a['joined'], unit='ms') for a in members_final_list]
    member_hike_count = [a['attended'] / 2 for a in members_final_list]
    member_challenge = [a['challenge'] / 2 for a in members_final_list]
    member_last_hike = [pd.to_datetime(max(get_events_attended_time(a)), unit='ms') if len(get_events_attended_time(a)) > 0 else ''
                    for a in members_final_list]
    member_noshows = [a['noshow'] for a in members_final_list]
    member_noshows_threemonth = [get_noshows_threemonths(a) for a in members_final_list]

    df = pd.DataFrame({
        'Id': member_ids,
        'Username': member_names,
        'Designation': member_designations,
        'Role': member_roles,
        'Joined': member_joined,
        'Hike count': member_hike_count,
        'Challenge estimate score': member_challenge,
        'Last hike': member_last_hike,
        'Noshows': member_noshows,
        'Noshows from 15/09': member_noshows_threemonth
    })
    parsed = [re.search(r'([0-9,]+?)\+', item) for item in df['Designation']]
    df['Numerical designation'] = [int(p.group(1).replace(',', '')) if p is not None else 0 for p in parsed]
    df['Needs update'] = ['YES' if (hikes >= designation + 50 and designation >= 50) or (hikes >= 50 and designation == 30) or 
                      (hikes >= 30 and designation == 0) else '' 
                      for hikes, designation in zip(df['Hike count'], df['Numerical designation'])]
    df['Check noshows'] = ['YES' if ns3m >= 2 else '' 
                      for ns3m in df['Noshows from 15/09']]
    df = df.reindex_axis(['Id', 'Username', 'Designation', 'Role', 'Joined', 'Last hike', 'Hike count', 'Challenge estimate score', 'Noshows', 'Noshows from 15/09', 'Numerical designation', 'Needs update', 'Check noshows'], axis=1)
    df = df.sort_values('Hike count', ascending=False)
    df.to_csv(output_csv)
    df.to_excel(output_excel)


def generate_event_excel(events_list, output_csv, output_excel): 
    events_final_list = list(events_list.values())
    events_ids = [a['id'] for a in events_final_list]
    events_titles = [a['title'] for a in events_final_list]
    events_hosts = [a['host'] for a in events_final_list]
    events_times = [pd.to_datetime(a['time'], unit='ms') for a in events_final_list]
    events_links = [a['link'] for a in events_final_list]
    events_rating = [a['rating'] for a in events_final_list]
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
        'Rating': events_rating,
        'Yes': events_yes,
        'Attended': events_attended,
        'Noshows': events_noshows
    })

    events_df = events_df.reindex_axis(['Id', 'Title', 'Host(s)', 'Time', 'Link', 'Yes', 'Attended', 'Noshows'], axis=1)
    events_df = events_df.sort_values('Time', ascending=False)
    events_df.to_csv(output_csv)
    events_df.to_excel(output_excel)


if __name__ == '__main__':
    members = extract_member_data(group_urlname, api_key)
    events = extract_event_data(group_urlname, api_key)
    extract_update_attendance_data(group_urlname, api_key, members, events)
    today_date = datetime.datetime.today().strftime('%Y%m%d')
    generate_member_excel(members, events, './members_%s.csv' % today_date, 'members_%s.xlsx' % today_date)
    generate_event_excel(events, './events_%s.csv' % today_date, 'events_%s.xlsx' % today_date)
    
