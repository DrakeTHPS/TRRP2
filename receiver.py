import socket
import rsa
from Cryptodome.Cipher import DES
import pika
import pymysql


def mySplit(string):
    word = ''
    result = []
    for cnt in range(len(string)):
        if string[cnt] == ')':
            result.append(word.replace(' ', '', 1))
            break
        if string[cnt] == ",":
            if string[cnt-1] == "'" or string[cnt-1].isdigit():
                result.append(word.replace(' ', '', 1))
                word = ''
            else:
                word += string[cnt]
        else:
            if string[cnt] != '(':
                word += string[cnt]

    return result

def fillCinemas(city, address):
    cur.execute(f"SELECT count(*) FROM cinemas where city={city} and cinema_address={address}")
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute(f'insert into cinemas(city, cinema_address) values ({city}, {address})')
        mycon.commit()
    cur.execute(f"SELECT id FROM cinemas where city={city} and cinema_address={address}")
    cinema = cur.fetchone()[0]
    return cinema


def fillMovies(movie, genre, duration, age_limit):
    cur.execute(f"SELECT count(*) FROM movies where name={movie} and genre={genre} and duration={duration} and age_limit={age_limit}")
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute(f'insert into movies(name, genre, duration, age_limit) values ({movie}, {genre}, {duration}, {age_limit})')
        mycon.commit()
    cur.execute(f"SELECT id FROM movies where name={movie} and genre={genre} and duration={duration} and age_limit={age_limit}")
    movieId = cur.fetchone()[0]
    return movieId


def fillHallTypes(hallType, form, cost):
    cur.execute(f"SELECT count(*) FROM hall_types where type={hallType} and format={form} and cost={cost}")
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute(f'insert into hall_types(type, format, cost) values ({hallType}, {form}, {cost})')
        mycon.commit()
    cur.execute(f"SELECT id FROM hall_types where type={hallType} and format={form} and cost={cost}")
    hall_type = cur.fetchone()[0]
    return hall_type


def fillHalls(cinema, hall_number, hallType):
    cur = mycon.cursor()
    cur.execute(f"SELECT count(*) FROM halls where cinema_id={cinema} and hall_number={hall_number} and hall_type_id={hallType}")
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute(f'insert into halls(cinema_id, hall_number, hall_type_id) values ({cinema}, {hall_number}, {hallType})')
        mycon.commit()
    cur.execute(f"SELECT id FROM halls where cinema_id={cinema} and hall_number={hall_number} and hall_type_id={hallType}")
    hallId = cur.fetchone()[0]
    return hallId


def fillSessions(hall_id, datetime, movie_id, available):
    cur = mycon.cursor()
    cur.execute(f"SELECT count(*) FROM sessions where hall_id={hall_id} and datetime={datetime[0:19]}' and movie_id={movie_id} and available={available}")
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute(f"insert into sessions(hall_id, datetime, movie_id, available) values ({hall_id}, {datetime[0:19]}', {movie_id}, {available})")
        mycon.commit()


def callback(ch, method, properties, body):
    data = des.decrypt(body).decode('utf8')
    strings = mySplit(data)
    cinema = fillCinemas(strings[0], strings[1])
    movie = fillMovies(strings[6], strings[7], strings[8], strings[11])
    hallType = fillHallTypes(strings[3], strings[4], strings[9])
    hallId = fillHalls(cinema, strings[2], hallType)
    fillSessions(hallId, strings[5], movie, strings[10])
    cur.execute('select * from sessions')
    #print(cur.fetchone())


#server_ip = '127.0.0.1'
#server_ip = '192.168.0.2'
with open('C:\\Users\\DrakeTHPS\\Desktop\\TRRP\\connection.txt') as f:
    conInfo = f.readlines()
conInfo = [x.strip() for x in conInfo]
server = socket.socket()
server.connect((conInfo[0], int(conInfo[1])))


(pubkey, privkey) = rsa.newkeys(1024)
print('Сформирована пара закрытый, открытый ключ')


pubkey_pem = pubkey.save_pkcs1()
server.send(pubkey_pem)
print(f'Открытый ключ отправлен передатчику\n{pubkey}')


server_data = server.recv(1024)
desKey = rsa.decrypt(server_data, privkey)
print(f'Получен и расшифрован ключ симметричного шифрования DES\n{server_data}\n{desKey}')
des = DES.new(desKey, DES.MODE_ECB)

mycon = pymysql.connect('localhost', 'root', '2358', 'sessions')
cur = mycon.cursor()
cur.execute('truncate table cinemas')
cur.execute('truncate table movies')
cur.execute('truncate table hall_types')
cur.execute('truncate table halls')
cur.execute('truncate table sessions')

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
stop = input('Для продолжения введи что-то')
channel = connection.channel()
channel.queue_declare(queue='database')
channel.basic_consume(queue='database', on_message_callback=callback, auto_ack=True)

channel.start_consuming()
