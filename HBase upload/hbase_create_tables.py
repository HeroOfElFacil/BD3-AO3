import happybase

host = '192.168.137.177'
port = '50070'
user_name = 'vagrant'

spotify = ['albums', 'bands', 'genres']

connection = happybase.Connection(host)
connection.open()

for tablename in spotify:
    connection.create_table(
        tablename,
        {
            tablename: dict()
        }
    )

connection.create_table(
    'ao3_band_fic_metadata_clean',
    {
        '_id': dict(), #id, title, summary, language
        '_status': dict(), #date_updated, status, n_chapters, complete, words
        '_rating': dict() #rating, kudos, hits, comments, bookmarks
    }
)

connection.create_table(
    'ao3_band_tag_metadata_clean',
    {
        'tag_meta': dict()
    }
)
