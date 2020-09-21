# -*- coding: utf-8 -*-
"""

Nayna Project 1 Spotify API pipline with bigdata Kafka and Spark
"""
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from kafka import KafkaProducer
import string
Topic='Bigdata-Task9'

Producer = KafkaProducer(bootstrap_servers='localhost:9092')

wid="101211004c504423b4b67f2a141d6320"
wsecret="2176752c3eb6497a845eb3b051bbcfd9"

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id = wid, \
                                                        client_secret= wsecret))


        
Artistlist = ["The Beatles", "Elvis Presley", "Michael Jackson", "Whitney Houston"\
              "ABBA", "AC/DC", "Adele", "Donna Summer", "Drake", "Eagles", \
              "Earth Wind & Fire", "Ed Sheeran", "Elton John", "Flo Rida", \
              "Foreigner", "Frank Sinatra", "Garth Brooks", "Genesis", \
              "George Michael", "George Strait", "Britney Spears", "Madonna",\
              "Pink", "Prince", "Olivia Newton-John", "Paul McCartney", \
              "Phil Collins", "Pink", "Pink Floyd", "Prince", "Queen", \
              "Red Hot Chili Peppers", "Rihanna", "Robbie Williams", \
              "The Police", "The Rolling Stones", "The Who", "Tim McGraw", \
              "Tina Turner", "Tom Petty", "Tupac Shakur", "U2", "Usher", \
              "Van Halen", "Whitney Houston"]
Artistlist.sort()         
final = []
           
for i in Artistlist:
    x = sp.search(i,50,0,'artist') 
    items = x['artists']['items']
    for c in range (0, len(items)):
        artist=items[c]        
        if (artist['name'] == i):
            Aname =  artist['name'].encode('utf-8')
            Auri  =  artist['uri'].encode('utf-8')
            Aid   =  artist['id'].encode('utf-8')
            Apop  =  artist['popularity']
            Ahref =  artist['href'].encode('utf-8')
            artistinfo = Aname + " === " + Auri + " === " + Aid + " === " \
                         + str(Apop) + " === " + Ahref  
            count =0
            for q in artist['genres']:
                count = count + 1
                Agenres = q.encode('utf-8')
                if count < 6:
                   artistinfo = artistinfo + " === " + Agenres
                                       
            for q in range (count+1 , 6):
                artistinfo = artistinfo + " === " + " " 
               
            y = sp.artist_albums(Aid,album_type='album',limit=50,offset=0)
            itemy = y['items']
#            print (len(itemy))
            for q in range (0, len(itemy)):
                album = itemy[q]
                ABname = album['name'].encode('utf-8')
                ABname = ABname.translate(None, string.punctuation)
                ABrdte = album['release_date'].encode('utf-8')
                ABuri = album['uri'].encode('utf-8')
                ABtott = album['total_tracks']
                ABid = album['id'].encode('utf-8')
                z = sp.album_tracks(ABid,limit=50,offset=0) 
                itemz = z['items']
#                print (len(itemz))
                for j in range (0,  len(itemz)):
                    track = itemz[j]
                    Tname = track['name'].encode('utf-8')
                    Tname = Tname.translate(None, string.punctuation)
                    Tdiscn = track['disc_number']
                    Tdur = track['duration_ms']
                    artistinfo2 = ABname + " === "+ ABrdte + " === " + \
                                 ABuri + " === "+ str(ABtott) + " === " + \
                                 ABid + " === " + Tname + " === " + \
                                 str(Tdiscn) + " === " + str(Tdur)
#                    print (artistinfo2) 
                                 
                    final.append(artistinfo + ',' + artistinfo2) 
                    
                    msg = artistinfo + ' === '+ artistinfo2
                    print ("Message sent: ", Aname, " ", Tname)
                    Producer.send(Topic, msg)
                    
                     
Producer.close()

