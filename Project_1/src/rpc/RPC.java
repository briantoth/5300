package rpc;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import testPackage.SessionServlet.*;

// TODO handle bad responses from RPC server?
// when a client receives a bad response, how do they handle it, should this be delegated to the caller?

public class RPC {
	private static final int MAX_PACKET_SIZE = 512;
	// TODO consult on this
	private static int COUNTER = (int) Math.rint(100000*Math.random());
	
	// private ConcurrentHashMap<String, SessionData> sessionMap;
	// private Set<ServerAddress> memberSet;
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
		
		// this.sessionMap = sMap;
		// this.memberSet = mSet;
		
		this.mutex = new Semaphore(1);
		
		// instantiate and fork server thread
		this.serverThread = new Thread(new RPCServer(sMap, mSet));
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
			
			// outputString = "callID,OpCode.SESSION_READ,sessionID,sessionVersionNum"
			String outputString = String.valueOf(callID) 
					+ "," + opCode
					+ "," + sessionID 
					+ "," + String.valueOf(sessionVersionNum);
			
			// output buffer has to be 512 bytes
			byte[] outputBuffer = new byte[MAX_PACKET_SIZE];
			fillOutputBuffer(outputString.getBytes(), outputBuffer);
			
			// get addresses of recipients from sessionID string
			ServerAddress address = getServerAddress(sessionID);
			
			// send packet to each destAddr, destPort pair
			sendPacket(rpcSocket, address, outputBuffer);
			
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
			
			// outputString = "callID,opCode.SESSION_WRITE,sessionID,sessionVersionNum,data,discardTime"
			String outputString = String.valueOf(callID)
					+ "," + opCode
					+ "," + sessionID
					+ "," + String.valueOf(sessionVersionNum)
					+ "," + data
					+ "," + discardTime.toString();
			
			// fill outputBuffer with [callID, opCode, sessionID, sessionVersionNum,
			//							data, discardTime]
			byte[] outputBuffer = new byte[MAX_PACKET_SIZE];
			fillOutputBuffer(outputString.getBytes(), outputBuffer);
			
			// get recipient address(es)
			ServerAddress address = getServerAddress(sessionID);
			
			// send packet to each recipient
			sendPacket(rpcSocket, address, outputBuffer);
			
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
			String opCode = OpCode.SESSION_DELETE.toString();
			
			// outputString = "callID,OpCode.SESSION_DELETE,sessionID,sessionVersionNum"
			String outputString = String.valueOf(callID)
					+ "," + opCode
					+ "," + sessionID
					+ "," + String.valueOf(sessionVersionNum);
			
			// fill outputBuffer with outputString
			byte[] outputBuffer = new byte[MAX_PACKET_SIZE];
			fillOutputBuffer(outputString.getBytes(), outputBuffer);
			
			// get recipient address(es)
			ServerAddress address = getServerAddress(sessionID);
			
			// send packet to each recipient
			sendPacket(rpcSocket, address, outputBuffer);
			
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
	
	public DatagramPacket getMembers(int size, ServerAddress address) {
		DatagramPacket response = null;
		try {
			// create new socket for communication
			DatagramSocket rpcSocket = new DatagramSocket();
			
			// generate unique call id
			int callID = generateCallID();
			
			// set OpCode
			OpCode opCode = OpCode.GET_MEMBERS;
			
			// outputString = "callID,OpCode.GET_MEMBERS,size"
			String outputString = String.valueOf(callID)
					+ "," + OpCode.GET_MEMBERS.toString()
					+ "," + String.valueOf(size);
			
			// fill output buffer with outputString
			byte[] outputBuffer = new byte[MAX_PACKET_SIZE];
			fillOutputBuffer(outputString.getBytes(), outputBuffer);
			
			// send packet
			sendPacket(rpcSocket, address, outputBuffer);
			
			// get response
			response = getResponse(rpcSocket, callID);
			
			// close the socket
			rpcSocket.close();
		} catch (SocketException e) {
			// TODO error creating socket
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO handle timeouts here
			e.printStackTrace();
		} catch (IOException e) {
			// TODO handle other errors
			e.printStackTrace();
		}
		return response;
	}
	
	public static int generateCallID() {
		// TODO make a better way of generating unique callID values
		return ++COUNTER;
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
	
	// TODO this is wrong, i think that the addresses as i'm getting them here should be 
	// passed as part of an additional parameter in the method stubs
	public static ServerAddress getServerAddress(String sessionID) {
		String[] splitSessionID = sessionID.split("_");
		ServerAddress address = new ServerAddress(splitSessionID[1], splitSessionID[2]);
		return address;
	}
	
	public static void sendPacket(DatagramSocket rpcSocket, ServerAddress address, byte[] outputBuffer) throws SocketException, IOException {
		// TODO possibly revert to allow multiple server addresses
		DatagramPacket pkt = new DatagramPacket(outputBuffer, outputBuffer.length, 
				address.getSocketAddress());
		rpcSocket.send(pkt);
	}
	
	public static DatagramPacket getResponse(DatagramSocket rpcSocket, int callID) {
		byte[] inputBuffer = new byte[MAX_PACKET_SIZE];
		DatagramPacket rPkt = new DatagramPacket(inputBuffer, inputBuffer.length);
		String[] splitData;
		int responseCallID = 0;
		do {
			// get call ID in response packet
			rPkt.setLength(inputBuffer.length);
			try {
				rpcSocket.receive(rPkt);
				splitData = (new String(rPkt.getData())).split(",");
				responseCallID = Integer.valueOf(splitData[0]);
			} catch (IOException e) {
				// TODO problem getting response
				e.printStackTrace();
			}
		} while (responseCallID != callID);
		return rPkt;
	}
	
	public static OpCode matchOpCode(String opCode) {
		OpCode code = null;
		if (opCode.equalsIgnoreCase(OpCode.SESSION_READ.toString())) {
			code = OpCode.SESSION_READ;
		} else if (opCode.equalsIgnoreCase(OpCode.SESSION_WRITE.toString())) {
			code = OpCode.SESSION_WRITE;
		} else if (opCode.equalsIgnoreCase(OpCode.SESSION_DELETE.toString())) {
			code = OpCode.SESSION_DELETE;
		} else if (opCode.equalsIgnoreCase(OpCode.GET_MEMBERS.toString())) {
			code = OpCode.GET_MEMBERS;
		}
		return code;
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
		
		private ConcurrentHashMap<String, SessionData> sessionMap;
		private Set<ServerAddress> memberSet;
		
		public RPCServer(ConcurrentHashMap<String, SessionData> sMap, Set<ServerAddress> mSet) {
			super();
			this.sessionMap = sMap;
			this.memberSet = mSet;
		}

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
					
					// get the return address for the response
					// TODO confirm this is correct
					ServerAddress responseAddr = new ServerAddress(rPkt.getAddress().toString(), String.valueOf(rPkt.getPort()));
					
					// get the callID and the operation code from the packet
					String pktData = new String(rPkt.getData());
					String[] splitData = pktData.split(",");
					int callID = Integer.valueOf(splitData[0]);
					OpCode opCode = matchOpCode(splitData[1]);
					if (opCode == null) {
						// TODO some error handling
					}
					
					// process packet and build response
					byte[] outputBuffer = new byte[MAX_PACKET_SIZE];
					String sessionID;
					int sessionVersionNum;
					String data;
					Date discardTime;
					SessionData sessionData;
					String responseString = String.valueOf(callID) + ",";
					switch (opCode) {
						case SESSION_READ:
							// get session ID and version number
							sessionID = splitData[2];
							// TODO do anything with this?
							sessionVersionNum = Integer.valueOf(splitData[3]);
							// look in session table for the requested information
							sessionData = this.sessionMap.get(sessionID);
							if (sessionData == null) {
								// construct response string with -1 version number
								responseString += "-1,READ";
							} else {
								// responseString = "versionNum, sessionData"
								responseString += String.valueOf(sessionData.version);
								responseString += ",";
								responseString += sessionData.toString();
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
							// TODO figure out string to date conversion
							discardTime = new Date();
							// check and see if there is already an entry for sessionID
							sessionData = this.sessionMap.get(sessionID);
							if (sessionData == null) {
								// construct a new sessionData instance and update sessionID
								sessionData = new SessionData();
								sessionData.sessionID = sessionID;
							} 
							sessionData.message = data;
							sessionData.version = sessionVersionNum;
							sessionData.expiration_timestamp = discardTime;
							this.sessionMap.put(sessionID, sessionData);
							// construct success response and fill output buffer
							responseString = String.valueOf(callID) + ",";
							responseString += "0,WRITE";
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
								responseString = "0,DEL";
							} else {
								// version number is higher than requested delete, leave alone and send -1 response
								responseString += "-1,DEL";
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
								responseString += ",";
								responseString += addr.toString();
							}
							fillOutputBuffer(responseString.getBytes(), outputBuffer);
							break;
						default:
							responseString += "-1,SWITCH";
							fillOutputBuffer(responseString.getBytes(), outputBuffer);
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
