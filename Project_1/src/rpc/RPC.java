package rpc;

public class RPC implements Runnable{
	
	public RPC() {
		super();
	}
	
	public void sessionRead() {
//		//
//		  // SessionReadClient(sessionID, sessionVersionNum)
//		  //   with multiple [destAddr, destPort] pairs
//		  //
//		  callID = generate unique id for call
//		  byte[] outBuf = new byte[...];
//		  fill outBuf with [ callID, operationSESSIONREAD, sessionID, sessionVersionNum ]
//		  for( each destAddr, destPort ) {
//		    DatagramPacket sendPkt = new DatagramPacket(outBuf, length, destAddr, destPort)
//		    rpcSocket.send(sendPkt);
//		  }
//		  byte [] inBuf = new byte[maxPacketSize];
//		  DatagramPacket recvPkt = new DatagramPacket(inBuf, inBuf.length);
//		  try {
//		    do {
//		      recvPkt.setLength(inBuf.length);
//		      rpcSocket.receive(recvPkt);
//		    } while( the callID in inBuf is not the expected one );
//		  } catch(InterruptedIOException iioe) {
//		    // timeout 
//		    recvPkt = null;
//		  } catch(IOException ioe) {
//		    // other error 
//		    ...
//		  }
//		  return recvPkt;
		
		// TODO consider following section on thread synchronization
	}
	
	public void sessionWrite() {
//		DatagramSocket rpcSocket = new DatagramSocket();
//		  serverPort = rpcSocket.getLocalPort();
//		  ...
//		  while(true) {
//		    byte[] inBuf = new byte[...]
//		    DatagramPacket recvPkt = new DatagramPacket(inBuf, inBuf.length);
//		    rpcSocket.receive(recvPkt);
//		    InetAddress returnAddr = recvPkt.getAddress();
//		    int returnPort = recvPkt.getPort();
//		    // here inBuf contains the callID and operationCode
//		    int operationCode = ... // get requested operationCode
//		    byte[] outBuf = NULL;
//		    switch( operationCode ) {
//		    	...
//		    	case operationSESSIONREAD:
//		    		outBuf = SessionRead(recvPkt.getdata(), recvPkt.getLength());
//		    		break;
//		    	...
//		    }
//		    // here outBuf should contain the callID and results of the call
//		    DatagramPacket sendPkt = new DatagramPacket(outBuf, outBuf.length,
//		    	returnAddr, returnPort);
//		    rpcSocket.send(sendPkt);
//		  }

	}
	
	public void sessionDelete() {
	}
	
	public void getMembers() {
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}
