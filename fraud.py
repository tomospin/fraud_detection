import socket
HOST = 'localhost'
PORT = 3223
sock =  socket.socket(socket.AF_INET,  socket.SOCK_STREAM) 
sock.bind((HOST,PORT))
sock.listen(5)
c,address = sock.accept()
while 1:
	buf = c.recv(1024)
	if(len(buf)>0):
		print buf
