package rpc;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import testPackage.SessionServlet.ServerAddress;
import testPackage.SessionServlet.SessionData;


public class RPC {
	
	private static final int MAX_PACKET_SIZE = 512;
	private static final int RECEIVE_TIMEOUT = 1000 * 10;
	private static int COUNTER;
	
	private Set<ServerAddress> memberSet;
	private Thread serverThread;
	private int rpcListenerPort;
	
	public enum OpCode {
		SESSION_READ, 
		SESSION_WRITE,
		SESSION_DELETE, 
		GET_MEMBERS;
		
		public String sendingForm(int port){
			return name() + "|" + port + "|";
		}
	}
	
	public RPC(ConcurrentHashMap<String, SessionData> sMap, Set<ServerAddress> mSet) {
		super();
		
		this.memberSet = mSet;
		
		// instantiate and fork server thread
		this.serverThread = new Thread(new RPCServer(sMap, mSet));
		this.serverThread.setDaemon(true);
		this.serverThread.start();
	}
	
	public SessionData sessionRead(String sessionID, int sessionVersionNum, ServerAddress[] addresses) {
		DatagramPacket response = null;
		SessionData sessionData = null;
		try {
			// create a new socket for communication
			DatagramSocket rpcSocket = new DatagramSocket();
			
			// generate callID
			int callID = generateCallID();
			
			// set opCode
			String opCode = OpCode.SESSION_READ.sendingForm(rpcListenerPort);
			
			// outputString = "callID,OpCode.SESSION_READ,sessionID,sessionVersionNum"
			String outputString = String.valueOf(callID) 
					+ "~" + opCode
					+ "~" + sessionID 
					+ "~" + String.valueOf(sessionVersionNum) + "~";
			
			// output buffer has to be 512 bytes
			byte[] outputBuffer = new byte[MAX_PACKET_SIZE];
			fillOutputBuffer(outputString.getBytes(), outputBuffer);
			
			for(ServerAddress address : addresses) {
				// send packet to each destAddr, destPort pair
				sendPackets(rpcSocket, new ServerAddress[] {address}, outputBuffer);
				
				// get response and construct session data instance
				response = getResponse(rpcSocket, callID, address);
				if(response != null) {
					// {callID, sessionID, versionNum, message, expiration timestamp}
					String[] sessionString = new String(response.getData()).split("~");
					System.out.println("response: " + new String(response.getData()));
					if (sessionString.length > 4) {
						sessionData = new SessionData();
						sessionData.sessionID = sessionString[1];
						sessionData.version = Integer.valueOf(sessionString[2]);
						sessionData.message = sessionString[3];
						sessionData.expiration_timestamp = getDateFromString(sessionString[4]);
						sessionData.responseAddress = new ServerAddress(response.getAddress().toString(), "" + response.getPort());
					}
					
					//We got our response so we don't need to send to anymore servers
					break;
				}
			}

			// close the socket
			rpcSocket.close();
		} catch (SocketException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return sessionData;
	}
	
	public boolean sessionWrite(String sessionID, int sessionVersionNum, String data, Date discardTime, ServerAddress[] addresses) {
		boolean success = false;
		try {
			// create a new socket for communication
			DatagramSocket rpcSocket = new DatagramSocket();
			
			// generate a callID
			int callID = generateCallID();
			
			// set opCode
			String opCode = OpCode.SESSION_WRITE.sendingForm(rpcListenerPort);
			
			SimpleDateFormat sdf= new SimpleDateFormat();
			// outputString = "callID,opCode.SESSION_WRITE,sessionID,sessionVersionNum,data,discardTime"
			String outputString = String.valueOf(callID)
					+ "~" + opCode
					+ "~" + sessionID
					+ "~" + String.valueOf(sessionVersionNum)
					+ "~" + data
					+ "~" + sdf.format(discardTime) + "~";
			
			// fill outputBuffer with [callID, opCode, sessionID, sessionVersionNum,
			//							data, discardTime]
			byte[] outputBuffer = new byte[MAX_PACKET_SIZE];
			fillOutputBuffer(outputString.getBytes(), outputBuffer);
			
			for(ServerAddress address : addresses) {
				// send packet to each recipient
				sendPackets(rpcSocket, new ServerAddress[] {address}, outputBuffer);
				
				// get response
				DatagramPacket response = getResponse(rpcSocket, callID, address);
				if(response != null) {
					String[] responseString = new String(response.getData()).split("~");
					if (responseString.length > 1) {
						if(responseString[1].equals("0")) {
							success = true;
						}
					}
				}
			}
			// close the socket
			rpcSocket.close();
		} catch (SocketException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return success;
	}
	
	public boolean sessionDelete(String sessionID, int sessionVersionNum, ServerAddress[] addresses) {
		boolean success = false;
		try {
			// create a new socket for communication
			DatagramSocket rpcSocket = new DatagramSocket();
			
			// generate a callID
			int callID = generateCallID();;
			
			// set opCode
			String opCode = OpCode.SESSION_DELETE.sendingForm(rpcListenerPort);
			
			// outputString = "callID,OpCode.SESSION_DELETE,sessionID,sessionVersionNum"
			String outputString = String.valueOf(callID)
					+ "~" + opCode
					+ "~" + sessionID
					+ "~" + String.valueOf(sessionVersionNum) + "~";
			
			// fill outputBuffer with outputString
			byte[] outputBuffer = new byte[MAX_PACKET_SIZE];
			fillOutputBuffer(outputString.getBytes(), outputBuffer);
			
			// send packet to each recipient
			sendPackets(rpcSocket, addresses, outputBuffer);
			
			// get response
			DatagramPacket response = getResponse(rpcSocket, callID, addresses[0]);
			if(response != null) {
				String[] responseString = new String(response.getData()).split("~");
				if (responseString.length > 1 && responseString[1] == "0") {
					success = true;
				}			
			}
  
			// close the socket
			rpcSocket.close();
		} catch (SocketException e) {
			e.printStackTrace();
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return success;
	}
	
	public Set<ServerAddress> getMembers(int size, ServerAddress[] addresses) {
		Set<ServerAddress> members = null;
		try {
			// create new socket for communication
			DatagramSocket rpcSocket = new DatagramSocket();
			
			// generate unique call id
			int callID = generateCallID();
			
			// set OpCode
			String opCode = OpCode.GET_MEMBERS.sendingForm(rpcListenerPort);
			
			// outputString = "callID,OpCode.GET_MEMBERS,size"
			String outputString = String.valueOf(callID)
					+ "~" + opCode
					+ "~" + String.valueOf(size) + "~";
			
			// fill output buffer with outputString
			byte[] outputBuffer = new byte[MAX_PACKET_SIZE];
			fillOutputBuffer(outputString.getBytes(), outputBuffer);
			
			for(ServerAddress address : addresses) {
				// send packet
				sendPackets(rpcSocket, new ServerAddress[] {address}, outputBuffer);
				
				// get response
				DatagramPacket response = getResponse(rpcSocket, callID, address);
				if(response != null) {
					String[] responseString = new String(response.getData()).split(",");
					if (responseString.length > 1 && responseString[1] != "-1") {
						members = new HashSet<ServerAddress>();
						for (int i = 1; i < responseString.length; i++) {
							String[] splitAddress = responseString[i].split("_");
							ServerAddress addr = new ServerAddress(splitAddress[0], splitAddress[1]);
							members.add(addr);
						}
					}
				}
			}
			
			// close the socket
			rpcSocket.close();
		} catch (SocketException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return members;
	}
	
	public static int generateCallID() {
		return ++COUNTER;
	}
	
	public static void fillOutputBuffer(byte[] outputData, byte[] outputBuffer) {
		if (!(outputData.length > MAX_PACKET_SIZE)) {
			for (int i = 0; i < outputData.length ; i++) {
				outputBuffer[i] = outputData[i];
			}
		}
	}
	
	private void sendPackets(DatagramSocket rpcSocket, ServerAddress[] addresses, byte[] outputBuffer) throws SocketException, IOException {
		for (ServerAddress addr : addresses) {
			//check for null or otherwise illegal address
			if(memberSet.contains(addr)){
				try {
					DatagramPacket pkt = new DatagramPacket(outputBuffer, outputBuffer.length, 
							addr.getSocketAddress());
					
					System.out.println("Sending packet to " + addr.toString());
					System.out.println("This is the packet sent:" + new String(pkt.getData()));
					rpcSocket.send(pkt);
				} catch (InterruptedIOException e) {
					// handle timeout here
					// remove member from set
					memberSet.remove(addr);
				} catch (Exception e){
					e.printStackTrace();
				}
			}
		}
	}
	
	private DatagramPacket getResponse(DatagramSocket rpcSocket, int callID, ServerAddress expectingMessageFrom) throws IOException {
		byte[] inputBuffer = new byte[MAX_PACKET_SIZE];
		DatagramPacket rPkt = new DatagramPacket(inputBuffer, inputBuffer.length);
		rpcSocket.setSoTimeout(RECEIVE_TIMEOUT);
		String[] splitData;
		int responseCallID = -1;
		try {
			do {
				// get call ID in response packet
				rPkt.setLength(inputBuffer.length);
				System.out.println("This is the port I expect a response to " + rpcSocket.getLocalPort());
				rpcSocket.receive(rPkt);
				
				splitData = (new String(rPkt.getData())).split("~");
				System.out.println("this is received: " + new String(rPkt.getData()));
				responseCallID = Integer.valueOf(splitData[0]);
			} while (responseCallID != callID);
		} catch(SocketTimeoutException e) {
			//if the message never showed up, we can assume the sender is dead
			memberSet.remove(expectingMessageFrom);
			return null;
		}
		return rPkt;
	}
	
	public static OpCode matchOpCode(String opCode) {
		OpCode code = null;
		if (opCode.equalsIgnoreCase(OpCode.SESSION_READ.name())) {
			code = OpCode.SESSION_READ;
		} else if (opCode.equalsIgnoreCase(OpCode.SESSION_WRITE.name())) {
			code = OpCode.SESSION_WRITE;
		} else if (opCode.equalsIgnoreCase(OpCode.SESSION_DELETE.name())) {
			code = OpCode.SESSION_DELETE;
		} else if (opCode.equalsIgnoreCase(OpCode.GET_MEMBERS.name())) {
			code = OpCode.GET_MEMBERS;
		}
		return code;
	}
	
	public static Date getDateFromString(String s) {
		System.out.println("trying to parse this date: " + s);
		// return a date instance from the string parameter
		SimpleDateFormat df = new SimpleDateFormat();
		Date d = null;
		try {
			 d = df.parse(s);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
		return d;
	}
	
	@SuppressWarnings("deprecation")
	public void crash() {
		// purposely crash the server thread
		this.serverThread.stop();
	}
	
	public int getRpcListenerPort() {
		return this.rpcListenerPort;
	}

	public void setRpcListenerPort(int rpcListenerPort) {
		this.rpcListenerPort = rpcListenerPort;
	}
	
	private class RPCServer implements Runnable {
		/**
		 * 
		 * RPCServer thread waits and receives incoming datagram packets.
		 * The thread parses each packet as it enters, determines the 
		 * appropriate course of action based on the OpCode, and then
		 * computes the proper response and returns it to the Client.
		 * 
		 */
		
		private ConcurrentHashMap<String, SessionData> sessionMap;
		private Set<ServerAddress> memberSet;
		private DatagramSocket servSocket;
		
		public RPCServer(ConcurrentHashMap<String, SessionData> sMap, Set<ServerAddress> mSet) {
			super();
			this.sessionMap = sMap;
			this.memberSet = mSet;

			// create a new socket for listening
			try {
				servSocket = new DatagramSocket();
			} catch (SocketException e) {
				e.printStackTrace();
			}
				
			int serverPort = servSocket.getLocalPort();
			setRpcListenerPort(serverPort);
			COUNTER = serverPort*10000;
		}

		@Override
		public void run() {
				// continuously listen for and handle packets
				while (true) {
					byte[] inputBuffer = new byte[MAX_PACKET_SIZE];
					DatagramPacket rPkt = new DatagramPacket(inputBuffer, inputBuffer.length);
					try {
						servSocket.receive(rPkt);
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					System.out.println("got something at server");
					
					// get the return address for the response
					ServerAddress responseAddr = new ServerAddress(rPkt.getAddress().toString(), String.valueOf(rPkt.getPort()));
					
					// get the callID and the operation code from the packet
					String pktData = new String(rPkt.getData());
					System.out.println("RPC SERVER RECEIVED PACKET\n" + pktData + "\n---------------");
					String[] splitData = pktData.trim().split("~");
					int callID = Integer.valueOf(splitData[0]);
					
					for (String item : splitData) {
						System.out.println(item);
					}
					
					String[] opCodePieces= splitData[1].split("\\|");
					//pull the address of the sending server out of the request.  we can add this to the member set
					ServerAddress newAddress= new ServerAddress(rPkt.getAddress().toString(), opCodePieces[1]);
					memberSet.add(newAddress);
					
					OpCode opCode = matchOpCode(opCodePieces[0]);
					System.out.println(opCode);
					
					// process packet and build response
					byte[] outputBuffer = new byte[MAX_PACKET_SIZE];
					String sessionID;
					int sessionVersionNum;
					String data;
					Date discardTime;
					SessionData sessionData;
					
					// prepend the callID to the response string
					String responseString = String.valueOf(callID) + "~";
					
					switch (opCode) {
						case SESSION_READ:
							// get session ID and version number
							sessionID = splitData[2];
							System.out.println("Attempting to parse '" + splitData[3] + "'");
							sessionVersionNum = Integer.parseInt(splitData[3]);
							// look in session table for the requested information
							sessionData = this.sessionMap.get(sessionID);
							if (sessionData == null || sessionData.version < sessionVersionNum) {
								// construct response string with -1 version number
								responseString += "-1" + "~";
							} else {
								// responseString = "callID, sessionData string"
								responseString += sessionData.toString() + "~";
							}
							// fill output buffer
							fillOutputBuffer(responseString.getBytes(), outputBuffer);	
							break;
						case SESSION_WRITE:
							// get session ID and version number
							sessionID = splitData[2];
							sessionVersionNum = Integer.valueOf(splitData[3]);
							// get new data and new discard time
							data = splitData[4];
							discardTime = getDateFromString(splitData[5]);
							// check and see if there is already an entry for sessionID
							sessionData = this.sessionMap.get(sessionID);
							if (sessionData == null) {
								// construct a new sessionData instance and update sessionID
								sessionData = new SessionData();
								sessionData.sessionID = sessionID;
								sessionData.version = -1;
							}
							
							if (sessionData.version <= sessionVersionNum) {
								sessionData.message = data;
								sessionData.version = sessionVersionNum;
								sessionData.expiration_timestamp = discardTime;
							}

							this.sessionMap.put(sessionID, sessionData);
							// construct success response and fill output buffer
							responseString += "0" + "~";
							fillOutputBuffer(responseString.getBytes(), outputBuffer);
							break;
						case SESSION_DELETE:
							// get sessionID and version number
							sessionID = splitData[2];
							sessionVersionNum = Integer.valueOf(splitData[3]);
							// get session data from session map
							sessionData = this.sessionMap.get(sessionID);
							
							if (sessionData == null || sessionData.version <= sessionVersionNum) {
								// nothing to do... 
								this.sessionMap.remove(sessionID);
								responseString += "0" + "~";
							} else {
								// version number is higher than requested delete, leave alone and send -1 response
								responseString += "-1" + "~";
							}
							fillOutputBuffer(responseString.getBytes(), outputBuffer);
							break;
						case GET_MEMBERS:
							// get size of member set to be returned
							int size = Integer.valueOf(splitData[3]);
							int mSetSize = this.memberSet.size();
							Set<ServerAddress> members = new HashSet<ServerAddress>();
							HashSet<Integer> randPos = new HashSet<Integer>();
							// get a random subset of the server addresses in memberSet
							if (size > mSetSize) {
								// return the entire memberSet
								members.addAll(this.memberSet);
							} else {
								// generate 'size' random numbers between 0 and mSetSize-1
								ServerAddress[] mSet = (ServerAddress[]) this.memberSet.toArray();
								int randInt = 0;
								int i = 0;
								while (i < size) {
									randInt = (int) Math.rint(Math.random() * (mSetSize));
									if (randInt == mSetSize) {
										randInt--;
									}
									if (!randPos.contains(randInt)) {
										randPos.add(randInt);
										i++;
									}
								}
								// collect all the members in positions selected at random
								for (int p : randPos) {
									members.add(mSet[p]);
								}
							}
							// construct response string and fill output buffer
							responseString = String.valueOf(callID);
							for (ServerAddress addr : members) {
								responseString += "~";
								responseString += addr.toString() + "~";
							}
							fillOutputBuffer(responseString.getBytes(), outputBuffer);
							break;
						default:
							responseString += "-1" + "~";
							fillOutputBuffer(responseString.getBytes(), outputBuffer);
							break;
					}
					
					// create the response packet and send the response
					DatagramPacket responsePacket= null;
					try {
						responsePacket = new DatagramPacket(outputBuffer, outputBuffer.length, responseAddr.getSocketAddress());
					} catch (SocketException e1) {
						e1.printStackTrace();
					}
//					DatagramPacket respPack= null;
//					System.out.println(responseAddr.serverPort);
//					System.out.println(Integer.valueOf(responseAddr.serverPort));
//					InetSocketAddress blah= new InetSocketAddress("localhost", Integer.valueOf(responseAddr.serverPort));
//					try{
//						respPack= new DatagramPacket(outputBuffer, outputBuffer.length, blah); //responseAddr.getSocketAddress());
//					} catch (Exception e){
//						System.out.println("Failure returning packet: " + e);
//					}
//					System.out.println("address is: " + blah); //responseAddr.getSocketAddress());
//					System.out.println("response packet is: " + new String(respPack.getData()));
					try {
						servSocket.send(responsePacket);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

	}

}
