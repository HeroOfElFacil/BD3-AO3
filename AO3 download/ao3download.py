import AO3
import pandas as pd
import requests
from AO3.requester import requester
from bs4 import BeautifulSoup
from tqdm.auto import tqdm, trange
import time
import os
import sys

fandoms_sorted = pd.read_csv('ao3_band_fic_count.csv', sep=';')
fandoms_sorted.drop(index=6).reset_index(drop=True).to_csv('ao3_band_fic_count_update.csv',sep=';', index = False)
top10 = pd.read_csv('ao3_band_fic_count_update.csv', sep=';')
bands = top10.iloc[1:, 0]
def get_page_data(search):

    data_single = pd.DataFrame(columns=['id', 'date_updated', 'bookmarks', 'nchapters', 'complete', 'comments', 'hits', 'kudos', 'language', 'rating', 'status', 'summary', 'title', 'words'])
    data_multi = pd.DataFrame(columns=['id', 'meta_name', 'meta_val'])
    meta_multi = ['categories', 'characters', 'fandoms', 'relationships', 'tags', 'warnings', 'authors', 'series']
    meta_single = ['id', 'date_updated', 'bookmarks', 'nchapters', 'complete', 'comments', 'hits', 'kudos', 'language', 'rating', 'status', 'summary', 'title', 'words']

    for result in search.results:
        meta_dict = {}
        for meta in meta_single:
            try:
                meta_dict[meta] = result.metadata[meta]
            except Exception:
                meta_dict[meta] = 0
        data_single = data_single.append(meta_dict, ignore_index=True)

        for meta in meta_multi:
            meta_dict = {}
            for value in result.metadata[meta]:
                meta_dict['id'] = result.metadata['id']
                meta_dict['meta_name'] = meta
                meta_dict['meta_val'] = value
                data_multi = data_multi.append(meta_dict, ignore_index=True)
    return data_single, data_multi

with open('pass.txt', 'r') as file:
    password = file.read()


sess = AO3.GuestSession()
search = AO3.Search(fandoms=bts, any_field='sort:>updated', revised_at='>2020-01-01', session=sess)
search.update()
print(search.total_results)
print(search.pages)
data_single = pd.DataFrame(columns=['id', 'date_updated', 'bookmarks', 'nchapters', 'complete', 'comments', 'hits', 'kudos', 'language', 'rating', 'status', 'summary', 'title', 'words'])
data_multi = pd.DataFrame(columns=['id', 'meta_name', 'meta_val'])
data_single.to_csv('ao3_band_fic_metadata_part2.csv', sep=';', index = False, mode='a')
data_multi.to_csv('ao3_band_tag_metadata_part2.csv', sep=';', index = False, mode='a')

for i in trange(1, search.pages+1):
    search.page = i
    wait_time = 0
    while True:
        try:
            search.update()
            sys.stdout.write('\r                                                           ')
            break
        except Exception:
            wait_time+=15
            sys.stdout.write(f'\r Waiting for page {i}... {wait_time}s                     ')
            time.sleep(15)
    page_single, page_multi = get_page_data(search)
    page_single.to_csv('ao3_band_fic_metadata_part2.csv', sep=';', index = False, mode='a', header=False)
    page_multi.to_csv('ao3_band_tag_metadata_part2.csv', sep=';', index = False, mode='a', header=False)
#     data_single = data_single.append(page_single, ignore_index=True)
#     data_multi = data_multi.append(page_multi, ignore_index=True)
data_single = pd.DataFrame(columns=['id', 'date_updated', 'bookmarks', 'nchapters', 'complete', 'comments', 'hits', 'kudos', 'language', 'rating', 'status', 'summary', 'title', 'words'])
data_multi = pd.DataFrame(columns=['id', 'meta_name', 'meta_val'])
data_single.to_csv('ao3_band_fic_metadata_part3.csv', sep=';', index = False, mode='a')
data_multi.to_csv('ao3_band_tag_metadata_part3.csv', sep=';', index = False, mode='a')

for j, band in enumerate(bands):
    search = AO3.Search(fandoms=band, any_field='sort:>updated', session=sess)
    search.update()
    band_number = j+1
    print(f'{band_number}/9: {band}')
    
    for i in trange(1, search.pages+1):
        search.page = i
        wait_time = 0
        while True:
            try:
                search.update()
                sys.stdout.write('\r                                                           ')
                break
            except Exception:
                wait_time+=15
                sys.stdout.write(f'\r Waiting for page {i}... {wait_time}s                     ')
                time.sleep(15)
        page_single, page_multi = get_page_data(search)
        page_single.to_csv('ao3_band_fic_metadata_part3.csv', sep=';', index = False, mode='a', header=False)
        page_multi.to_csv('ao3_band_tag_metadata_part3.csv', sep=';', index = False, mode='a', header=False)

fic1 = pd.read_csv('ao3_band_fic_metadata_part1.csv', sep=';')
tag1 = pd.read_csv('ao3_band_tag_metadata_part1.csv', sep=';')
fic2 = pd.read_csv('ao3_band_fic_metadata_part2.csv', sep=';')
tag2 = pd.read_csv('ao3_band_tag_metadata_part2.csv', sep=';')
fic3 = pd.read_csv('ao3_band_fic_metadata_part3.csv', sep=';')
tag3 = pd.read_csv('ao3_band_tag_metadata_part3.csv', sep=';')
fic1 = fic1.append(fic2, ignore_index=True)
fic1 = fic1.append(fic3, ignore_index=True)
tag1 = tag1.append(tag2, ignore_index=True)
tag1 = tag1.append(tag3, ignore_index=True)
fic1 = fic1.sort_values(by='date_updated', ascending=True)
fic1 = fic1.drop_duplicates(subset=['id'], keep='last', ignore_index=True)
tag1 = tag1.drop_duplicates(ignore_index=True)
fic1.to_csv('ao3_band_fic_metadata_clean.csv', sep=';', index = False)
tag1.to_csv('ao3_band_tag_metadata_clean.csv', sep=';', index = False)