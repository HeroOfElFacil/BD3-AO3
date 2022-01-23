import AO3
import pandas as pd
import requests
from AO3.requester import requester
from bs4 import BeautifulSoup
from tqdm.auto import tqdm, trange
import time
import os
import sys

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

bands = pd.read_csv('ao3_band_fic_count_update.csv', sep=';')
bts = '방탄소년단 | Bangtan Boys | BTS'
bands = bands.iloc[1:, 0]
sess = AO3.GuestSession()

#part 1
search = AO3.Search(fandoms=bts, any_field='sort:>updated', revised_at='<2020-01-01', session=sess)
search.update()
print(search.total_results)
print(search.pages)
data_single = pd.DataFrame(columns=['id', 'date_updated', 'bookmarks', 'nchapters', 'complete', 'comments', 'hits', 'kudos', 'language', 'rating', 'status', 'summary', 'title', 'words'])
data_multi = pd.DataFrame(columns=['id', 'meta_name', 'meta_val'])
data_single.to_csv('ao3_band_fic_metadata_part1.csv', sep=';', index = False, mode='a')
data_multi.to_csv('ao3_band_tag_metadata_part1.csv', sep=';', index = False, mode='a')

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
    page_single.to_csv('ao3_band_fic_metadata_part1.csv', sep=';', index = False, mode='a', header=False)
    page_multi.to_csv('ao3_band_tag_metadata_part1.csv', sep=';', index = False, mode='a', header=False)
#     data_single = data_single.append(page_single, ignore_index=True)
#     data_multi = data_multi.append(page_multi, ignore_index=True)

#part 2
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

#part 3
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

# mergr
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
fic = fic1
tag = tag1

#cleaning
fic = fic.replace(to_replace='\n', value='', regex=True)
fic = fic.replace(to_replace='\\', value='', regex=True)
fic = fic.replace(to_replace=';', value=':', regex=True)
fic = fic.replace(to_replace='"', value='\'', regex=True)
tag = tag.replace(to_replace='\n', value='', regex=True)
tag = tag.replace(to_replace='\\', value='', regex=True)
tag = tag.replace(to_replace=';', value=':', regex=True)
tag = tag.replace(to_replace='"', value='\'', regex=True)
fic = fic.fillna('N//A')

#unifikacja fandomow
fandom_string = ['bts', 'one direction', 'nct', 'exo', 'stray kids', 'seventeen', 'got7', 'ateez', 'my chemical romance', 'monsta x', '1d', '1 d', 'one d', 'mcr']
fandom_new = ["BTS", "One Direction", "NCT 127", "EXO", "Stray Kids", "SEVENTEEN", "GOT7", "ATEEZ", "My Chemical Romance", "Monsta X", 'One Direction', 'One Direction', 'One Direction', 'My Chemical Romance']
for i in tqdm(range(len(fandom_new))):
    tag.loc[(tag['meta_name']=='fandoms') & (tag['meta_val'].str.contains(fandom_string[i], na=False, case=False)), 'meta_val'] = fandom_new[i]

#save
fic.to_csv('ao3_band_fic_metadata_clean.csv', sep=';', index = False)
tag.to_csv('ao3_band_tag_metadata_clean.csv', sep=';', index = False)
