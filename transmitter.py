import socket
import rsa
from Cryptodome.Cipher import DES
import sqlite3
import pika

desKey = b'abcdefgh'
des = DES.new(desKey, DES.MODE_ECB)


def pad(text):
    while len(text) % 8 != 0:
        text += b' '
    return text


with open('C:\\Users\\DrakeTHPS\\Desktop\\TRRP\\connection.txt') as f:
    conInfo = f.readlines()
sock = socket.socket()
sock.bind(('', int(conInfo[1])))
sock.listen(1)
conn, addr = sock.accept()

data = conn.recv(1024)
pubkey = rsa.PublicKey.load_pkcs1(data, 'PEM')
print(f'Получен открытый ключ RSA\n{pubkey}')


message = rsa.encrypt(desKey, pubkey)
conn.send(message)
print(f'Отправлен ключ симметричного шифрования DES\n{message}')


sqlConn = sqlite3.connect('C:\\Users\\DrakeTHPS\\SQLite dbs\\sessions.db')
cursor = sqlConn.cursor()


connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='database')

sql = 'select * from sessions'
for row in cursor.execute(sql):
    print(row)
    data = str(row).encode('utf8')
    encrypted = des.encrypt(pad(data))
    channel.basic_publish(exchange='', routing_key='database', body=encrypted)

connection.close()
