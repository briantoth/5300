package rpc;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import testPackage.SessionServlet.*;

public class RPC {
	private static final int MAX_PACKET_SIZE = 512;
	// TODO synchronize access to LAST_CALL_ID?
	private static int LAST_CALL_ID = 0;
	
	private ConcurrentHashMap<String, SessionData> sessionMap;
	private Set<ServerAddress> memberSet;
	private Thread serverThread;
	private Semaphore mutex;
	
	public enum OpCode {
		SESSION_READ, 
		SESSION_WRITE,
		SESSION_DELETE, 
		GET_MEMBERS
	}
	
	public RPC(ConcurrentHashMap<String, SessionData> sMap, Set<ServerAddress> mSet) {
		super();
		
		this.sessionMap = sMap;
		this.memberSet = mSet; // TODO synchronize access to memberSet
		
		this.mutex = new Semaphore(1); // TODO necessary?
		
		// instantiate and fork server thread
		this.serverThread = new Thread(new RPCServer());
		this.serverThread.setDaemon(true);
		this.serverThread.start();
	}
	
	public DatagramPacket sessionRead(String sessionID, int sessionVersionNum) {
		DatagramPacket response = null;
		try {
			// create a new socket for communication
			DatagramSocket rpcSocket = new DatagramSocket();
			
			// generate callID
			int callID = generateCallID();
			
			// set opCode
			String opCode = OpCode.SESSION_READ.toString();
			
			// outputData =  [callID, OpCode.SESSION_READ, sessionID, sessionVersionNum]
			byte[] outputData = (String.valueOf(callID) 
					+ opCode
					+ sessionID 
					+ String.valueOf(sessionVersionNum)).getBytes();
			
			// output buffer has to be 512 bytes
			byte[] outputBuffer = new byte[MAX_PACKET_SIZE];
			fillOutputBuffer(outputData, outputBuffer);
			
			// get addresses of recipients from sessionID string
			// TODO confirm this is what to do...
			ServerAddress[] addresses = getServerAddresses(sessionID);
			
			// send packet to each destAddr, destPort pair
			sendPackets(rpcSocket, addresses, outputBuffer);
			
			// get response
			response = getResponse(rpcSocket, callID);
  
			// close the socket
			rpcSocket.close();
		} catch (SocketException e) {
			// TODO figure out proper error handling
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO handle timeouts here
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO consider following section on thread synchronization
		return response;
	}
	
	public DatagramPacket sessionWrite(String sessionID, int sessionVersionNum, String data, Date discardTime) {
		DatagramPacket response = null;
		try {
			// create a new socket for communication
			DatagramSocket rpcSocket = new DatagramSocket();
			
			// generate a callID
			int callID = generateCallID();;
			
			// set opCode
			String opCode = OpCode.SESSION_WRITE.toString();
			
			// construct the output string
			String outputString = "";
			outputString += String.valueOf(callID);
			outputString += opCode;
			outputString += sessionID;
			outputString += String.valueOf(sessionVersionNum);
			outputString += data;
			outputString += discardTime.toString();
			
			// fill outputBuffer with [callID, opCode, sessionID, sessionVersionNum,
			//							data, discardTime]
			byte[] outputBuffer = new byte[MAX_PACKET_SIZE];
			fillOutputBuffer(outputString.getBytes(), outputBuffer);
			
			// get recipient address(es)
			ServerAddress[] addresses = getServerAddresses(sessionID);
			// send packet to each recipient
			sendPackets(rpcSocket, addresses, outputBuffer);
			
			// get response
			response = getResponse(rpcSocket, callID);
  
			// close the socket
			rpcSocket.close();
			
		// TODO error handling
		} catch (SocketException e) {
			// TODO figure out proper error handling
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO handle timeouts here
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return response;
	}
	
	public DatagramPacket sessionDelete(String sessionID, int sessionVersionNum) {
		DatagramPacket response = null;
		try {
			// create a new socket for communication
			DatagramSocket rpcSocket = new DatagramSocket();
			
			// generate a callID
			int callID = generateCallID();;
			
			// set opCode
			String opCode = OpCode.SESSION_WRITE.toString();
			
			// TODO construct the output string
			String outputString = "";
			
			// fill outputBuffer with [TODO]
			byte[] outputBuffer = new byte[MAX_PACKET_SIZE];
			fillOutputBuffer(outputString.getBytes(), outputBuffer);
			
			// get recipient address(es)
			ServerAddress[] addresses = getServerAddresses(sessionID);
			// send packet to each recipient
			sendPackets(rpcSocket, addresses, outputBuffer);
			
			// get response
			response = getResponse(rpcSocket, callID);
  
			// close the socket
			rpcSocket.close();
			
		// TODO error handling
		} catch (SocketException e) {
			// TODO figure out proper error handling
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO handle timeouts here
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return response;
	}
	
	public void getMembers(int size) {
		// TODO finish me
	}
	
	public static int generateCallID() {
		// TODO make a better way of generating unique callID values
		return ++LAST_CALL_ID;
	}
	
	public static void fillOutputBuffer(byte[] outputData, byte[] outputBuffer) {
		if (outputData.length > MAX_PACKET_SIZE) {
			// TODO handle case where too much data
		} else if (outputBuffer.length != MAX_PACKET_SIZE) {
			// TODO handle case where the ouytputBuffer is not the right size
		} else {
			for (int i = 0; i < MAX_PACKET_SIZE; i++) {
				outputBuffer[i] = outputData[i];
			}
		}
	}
	
	public static ServerAddress[] getServerAddresses(String sessionID) {
		// TODO implement me
		ServerAddress[] addresses = new ServerAddress[10];
		return addresses;
	}
	
	public static void sendPackets(DatagramSocket rpcSocket, ServerAddress[] addresses, byte[] outputBuffer) throws SocketException, IOException {
		for (ServerAddress addr : addresses) {
			DatagramPacket pkt = new DatagramPacket(outputBuffer, outputBuffer.length, 
					addr.getSocketAddress());
			rpcSocket.send(pkt);
		}
	}
	
	public static DatagramPacket getResponse(DatagramSocket rpcSocket, int callID) {
		byte[] inputBuffer = new byte[MAX_PACKET_SIZE];
		DatagramPacket rPkt = new DatagramPacket(inputBuffer, inputBuffer.length);
		int responseCallID = 0;
		do {
			// TODO get call ID in response packet
			rPkt.setLength(inputBuffer.length);
			try {
				rpcSocket.receive(rPkt);
			} catch (IOException e) {
				// TODO problem getting response
				e.printStackTrace();
			}
		} while (responseCallID != callID);
		return rPkt;
	}
	
	public class RPCServer implements Runnable {
		/**
		 * 
		 * RPCServer thread waits and receives incoming datagram packets.
		 * The thread parses each packet as it enters, determines the 
		 * appropriate course of action based on the OpCode, and then
		 * computes the proper response and returns it to the Client.
		 * 
		 */

		@Override
		public void run() {
			try {
				// create a new socket for listening
				DatagramSocket servSocket = new DatagramSocket();
				int serverPort = servSocket.getLocalPort();
				// continuously listen for and handle packets
				while (true) {
					byte[] inputBuffer = new byte[MAX_PACKET_SIZE];
					DatagramPacket rPkt = new DatagramPacket(inputBuffer, inputBuffer.length);
					servSocket.receive(rPkt);
					// TODO get the return address for the response
					ServerAddress responseAddr = new ServerAddress("","");
					// TODO get the callID and the operation code from the packet
					OpCode opCode = OpCode.SESSION_READ;
					// process packet and build response
					byte[] outputBuffer = new byte[MAX_PACKET_SIZE];
					switch (opCode) {
						case SESSION_READ:
							// TODO: handle me
							break;
						case SESSION_WRITE:
							// TODO: handle me
							break;
						case SESSION_DELETE:
							// TODO: handle me
							break;
						case GET_MEMBERS:
							// TODO: handle me
							break;
					}
					// create the response packet and send the response
					DatagramPacket responsePacket = new DatagramPacket(outputBuffer, outputBuffer.length, 
							responseAddr.getSocketAddress());
					servSocket.send(responsePacket);
				}
			// TODO error handling
			} catch (SocketException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
