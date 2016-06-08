package com.kong.learning.socket;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class MyServerSocket {

	private static String[] key ={"1","2","4","4"}; 
	
	public static void main(String[] args) {
		try {
			ServerSocket serverSocket = new ServerSocket(8888);
			while (true) {
				Socket socket = serverSocket.accept();
				InputStream input = socket.getInputStream();
				DataInputStream dInput = new DataInputStream(input);
				OutputStream output = socket.getOutputStream();
				DataOutputStream doutput = new DataOutputStream(output);
				String str = dInput.readUTF();
				for (String string : key) {
					if (str.equals(string)) {
						System.out.println("start connect!");
						System.out.println(str);
					}
				}
				doutput.writeUTF("connect success!");
				doutput.close();
				dInput.close();
				output.close();
				input.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
