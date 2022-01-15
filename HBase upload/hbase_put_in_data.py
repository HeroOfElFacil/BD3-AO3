import happybase
from pywebhdfs.webhdfs import PyWebHdfsClient
import io
from fastavro import reader

host = '192.168.137.182'
port = '50070'
user_name = 'vagrant'

spotify = ['albums', 'bands', 'genres']

hdfs = PyWebHdfsClient(host=host, port=port, user_name=user_name)

for filename in spotify:
    file = hdfs.read_file('user/vagrant/spotify/'+ filename +'.csv')
    rb = io.BytesIO(file)
    data = reader(rb)

    table = connection.table(filename)
    i = 0
    for record in data:
        colnames = eval(str(record.keys())[10:-1])
        for j in range(1, len(colnames)):
            table.put(str(i), {filename + ':' + colnames[j]: str(record[colnames[j]])})
        i+=1


file = hdfs.read_file('user/vagrant/ao3/ao3_band_fic_metadata_clean.csv')
rb = io.BytesIO(file)
data = reader(rb)

table = connection.table('fic_meta')
i = 0
for record in data:
    table.put(str(i), {
        '_id:id': str(record['id']),
        '_id:title': str(record['title']),
        '_id:summary': str(record['summary']),
        '_id:language': str(record['language']),
        '_status:status': str(record['status']),
        '_status:date_updated': str(record['date_updated']),
        '_status:nchapters': str(record['nchapters']),
        '_status:complet': str(record['complete']),
        '_status:words': str(record['words']),
        '_rating:rating': str(record['rating']),
        '_rating:kudos': str(record['kudos']),
        '_rating:hits': str(record['hits']),
        '_rating:comments': str(record['comments']),
        '_rating:bookmarks': str(record['bookmarks'])
    })
    i+=1


file = hdfs.read_file('user/vagrant/ao3/ao3_band_tag_metadata_clean.csv')
rb = io.BytesIO(file)
data = reader(rb)

table = connection.table('tag_meta')
i = 0
for record in data:
    colnames = eval(str(record.keys())[10:-1])
    for j in range(1, len(colnames)):
        table.put(str(i), {'tag_meta:' + colnames[j]: str(record[colnames[j]])})
    i+=1