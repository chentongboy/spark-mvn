package com.kong.learning.socket;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class MySocket {

	public static void main(String[] args) {
		try {
			for (int i = 0; i < 10; i++) {
				Socket socket = new Socket("127.0.0.1",8888);
				OutputStream output = socket.getOutputStream();
				DataOutputStream doutput = new DataOutputStream(output);
				InputStream input = socket.getInputStream();
				DataInputStream dinput = new DataInputStream(input);
				doutput.writeUTF(String.valueOf(i));
				String str = dinput.readUTF();
				System.out.println(str);
				dinput.close();
				input.close();
				doutput.close();
				output.close();
				socket.close();
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
