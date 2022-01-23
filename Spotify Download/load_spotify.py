import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd

auth_manager = SpotifyClientCredentials(client_id='eb61b94463394a85a20aa479c80e1b8e', 
                                        client_secret='0b0555b252ae4a2d8cdb05bcbd746d44')
sp = spotipy.Spotify(auth_manager=auth_manager)
bands = pd.read_csv("/home/vagrant/nifi/ao3_band_fic_count.csv", sep = ';')
bands = bands.sort_values(ascending=False, by="FanfictionCount").head(10)
bands_list = bands["Band"]

rows_bands = []
rows_genres = []
rows_albums = []
for band in bands_list:
    print(f'Downloading {band}...')
    band = band.replace("(Band)", "")
    band = band.strip()
    band_dict = sp.search(q="artist:{}".format(band), type="artist")['artists']['items'][0]
    albums_list = sp.artist_albums(band_dict['uri'], album_type='album')['items']
    for album in albums_list:
        rows_albums.append({'BandName': band_dict['name'], 'AlbumName': album['name'], 'ReleaseDate': album['release_date']})
    for genre in band_dict['genres']:
        rows_genres.append({'BandName': band_dict['name'], 'Genre': genre})
    rows_bands.append({'BandName': band_dict['name'], 
                       'Popularity': band_dict['popularity'], 
                       'Followers': band_dict['followers']['total']})
    print(f'Done!')
    
bands_output = pd.DataFrame(rows_bands)
genres_output = pd.DataFrame(rows_genres)
albums_output = pd.DataFrame(rows_albums).drop_duplicates(subset=['BandName', 'AlbumName'])
bands_output.to_csv('/home/vagrant/nifi/bands.csv', index=False, sep=';')
genres_output.to_csv('/home/vagrant/nifi/genres.csv', index=False, sep=';')
albums_output.to_csv('/home/vagrant/nifi/albums.csv', index=False, sep=';')
print('All done!')
