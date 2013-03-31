package testPackage; // Always use packages. Never use default package.

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import rpc.RPC;

/** Very simplistic servlet that generates plain text.
 *  Uses the @WebServlet annotation that is supported by
 *  Tomcat 7 and other servlet 3.0 containers. 
 *  
 *  From <a href="http://courses.coreservlets.com/Course-Materials/">the
 *  coreservlets.com tutorials on servlets, JSP, Struts, JSF 2.x, 
 *  Ajax, jQuery, GWT, Spring, Hibernate/JPA, Hadoop, and Java programming</a>.
 */

@WebServlet("/")
public class SessionServlet extends HttpServlet {

	/* ONLY GET THIS VALUE FROM THE METHOD getNextSessionID since access
	 * to this variable must be syncrhonized across concurrent requests
	 */
	private static long nextSessionID = 0;

	//Number of milliseconds before session expires
	private static final long SESSION_EXPIRATION_TIME = 5 * 60 * 1000;
	private static final long CLEANUP_TIMEOUT = 5 * 1000;

	private static final String COOKIE_NAME = "CS5300_WJK56_DRM237_BDT25";
	private static final String DEFAULT_MESSAGE = "Hello User!";
	private static final ServerAddress NULL_ADDRESS = new ServerAddress("0.0.0.0", "0");

	private ConcurrentHashMap<String, SessionData> sessionMap;
	private Thread cleanupDaemon;
	private RPC rpcServer;
	private Set<ServerAddress> memberSet = new HashSet<ServerAddress>();
	
	private ServerAddress localAddress;
	private Boolean crashed= false;

	private int rpcListenerPort;

	public SessionServlet() {
		super();
		sessionMap = new ConcurrentHashMap<String, SessionServlet.SessionData>();

		cleanupDaemon = new Thread(new SessionTableCleaner());
		cleanupDaemon.setDaemon(true);
		cleanupDaemon.start();
		
		rpcServer = new RPC(sessionMap, memberSet);
		rpcListenerPort = rpcServer.getRpcListenerPort();
	}

	@Override
	public void doGet(HttpServletRequest request,
			HttpServletResponse response)
					throws ServletException, IOException {
		
		synchronized(crashed){
			if(crashed){
				try {
					Thread.sleep(1000000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				return;
			}
		}
		
		if(localAddress == null)
			localAddress = new ServerAddress(request.getLocalAddr(), "" + rpcListenerPort);
		
		String sessionSource = "";
		Cookie[] cookies = request.getCookies();
		String cmd = request.getParameter("cmd");

		//if the client is visiting the site for the first time
		if(cookies == null) {
			SessionData sessionData = newSessionState(request, response);
			refresh(request, response, sessionData, "new session");
			System.out.println("Starting new session");
			return;
		}
		else {
			for(Cookie cookie : cookies) {
				if(COOKIE_NAME.equals(cookie.getName())) {
					VerboseCookie verboseCookie = new VerboseCookie(cookie);
					String sessionID = verboseCookie.sessionID;
					
					updateMemberSet(verboseCookie);
					
					SessionData sessionData = null;
					//Check to see if the cookie is stored in our local session
					//map. If it is then we don't need to really do anything
					if(! (verboseCookie.location1.equals(localAddress) ||
						  verboseCookie.location2.equals(localAddress))) {
						//It is not stored in our local map, so we
						// need to get it using an RPC call
						
						ServerAddress[] serverAddresses = {verboseCookie.location1, verboseCookie.location2};
						
						sessionData = rpcServer.sessionRead(sessionID, verboseCookie.version, serverAddresses);
						
						
					} else {
						//It is in our local map
						sessionData = sessionMap.get(sessionID);
						sessionSource = localAddress.toString() + " -- IPP Local";
					}
		
					
					if(sessionData == null) {
						//The session expired or the RPC calls failed
						//TODO Change this, we need to display a message that
						//the session expired
						
						//backdated cookie to force the client to clear its cookie
						Cookie newCookie= new Cookie(COOKIE_NAME, "old_cookie");
						newCookie.setMaxAge(0);
						response.addCookie(newCookie);
						
						response.setContentType("text/html");
						try {
							PrintWriter out = response.getWriter();
							out.println("<!DOCTYPE html>\n" +
									"<html>\n" +
									"<head><title>Session Expired!</title></head>\n" +
									"<body>\n" +
									"<h1>Session Expired</h1>\n" +
									"</body></html>");
						} catch (IOException e) { }
						return;
					} else {
						if(sessionData.responseAddress != null) {
							if(sessionData.responseAddress.equals(verboseCookie.location1)) {
								sessionSource = sessionData.responseAddress.toString() + " -- IPP primary";
							} else if(sessionData.responseAddress.equals(verboseCookie.location2)) {
								sessionSource = sessionData.responseAddress.toString() + " -- IPP backup";
							} else {
								sessionSource = sessionData.responseAddress.toString();
							}
						}
						if(cmd == null || cmd.equals("Refresh")) {
							refresh(request, response, sessionData, sessionSource);
							return;
						} else if(cmd.equals("Replace")) {
							replace(request, response, sessionData);
							refresh(request, response, sessionData, sessionSource);
							return;
						} else if(cmd.equals("Crash")){
							synchronized(crashed){
								crashed=true;
								rpcServer.crash();
								try {
									Thread.sleep(1000000);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
								return;
							}
						} else if(cmd.equals("LogOut")) {
							logout(request, response, sessionData, verboseCookie.location1, verboseCookie.location2);
							return;
						}
					}
				}
			}
		}

		//If we got here than there are cookies that we set, but none with
		//the name COOKIE_NAME, so we need to create a new session
		//This should probably never happen, but it's nice to have
		//anyways
		System.out.println("There are cookies that we set, but none with the name " + COOKIE_NAME);
		SessionData sessionData = newSessionState(request, response);
		refresh(request, response, sessionData, "new session");
	}
	
	/** Update the member set. For each location we need to ensure that
	 *  it is not the null address or our local address
	 * @param verboseCookie
	 */
	private void updateMemberSet(VerboseCookie verboseCookie) {
		ServerAddress location1 = verboseCookie.location1;
		ServerAddress location2 = verboseCookie.location2;
		
		if(! (location1.equals(NULL_ADDRESS) || location1.equals(localAddress)))
			memberSet.add(location1);
		if(! (location2.equals(NULL_ADDRESS) || location2.equals(localAddress)))
			memberSet.add(location2);
		
	}

	/** Creates a new SessionData object and stores it in our local session map */
	private SessionData newSessionState(HttpServletRequest request, HttpServletResponse response) {
		SessionData session = new SessionData();
		session.sessionID = getNextSessionID() + "_" + localAddress.toString();

		//Set version number to 0 since it will be incremented in the
		//refresh method to 1
		session.version = 0;
		session.message = DEFAULT_MESSAGE;
		
		return session;
	}

	private static final long serialVersionUID = 1L;

	private void replace(HttpServletRequest request, HttpServletResponse response, SessionData session) {
		String newMessage = request.getParameter("new_message");

		//In case there is no new_message parameter in the request
		//This really shouldn't happen
		newMessage = newMessage == null ? "" : newMessage;

		session.message = newMessage;
	}

	private synchronized long getNextSessionID() {
		return nextSessionID++;
	}



	private void logout(HttpServletRequest request, HttpServletResponse response, SessionData sessionData, ServerAddress primary, ServerAddress backup) {
		
		if(primary.equals(localAddress)){
			sessionMap.remove(sessionData.sessionID);
		} else {
			rpcServer.sessionDelete(sessionData.sessionID, sessionData.version, new ServerAddress[] {primary});
		}
		if(backup.equals(localAddress)){
			sessionMap.remove(sessionData.sessionID);
		} else {
			rpcServer.sessionDelete(sessionData.sessionID, sessionData.version, new ServerAddress[] {backup});
		}
		
		response.setContentType("text/html");
		try {
			PrintWriter out = response.getWriter();
			out.println("<!DOCTYPE html>\n" +
					"<html>\n" +
					"<head><title>Bye!</title></head>\n" +
					"<body>\n" +
					"<h1>Bye!</h1>\n" +
					"</body></html>");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}




	private void refresh(HttpServletRequest request, HttpServletResponse response, SessionData sessionData, String dataSource) throws IOException {
		sessionData.version++;
		//reset expiration of session
		sessionData.expiration_timestamp = new Date(new Date().getTime() + SESSION_EXPIRATION_TIME);
		
		//save the session data because we are now the primary backup
		sessionMap.put(sessionData.sessionID, sessionData);
		
		//save this session data with another server to act as a backup
		ServerAddress backupLocation= null;
		List<ServerAddress> randomMembers= new ArrayList<ServerAddress>(memberSet);
		Collections.shuffle(randomMembers);
		for(ServerAddress address : randomMembers){
			//TODO: figure out real discard time
			boolean result= 
					rpcServer.sessionWrite(sessionData.sessionID, sessionData.version, sessionData.message,
							sessionData.expiration_timestamp, new ServerAddress[]{address});
			if (result){
				backupLocation= address;
				break;
			}
		}
		
		//make a new cookie
		VerboseCookie cookie= new VerboseCookie(sessionData, localAddress, backupLocation);
		Cookie cookieToSend= new Cookie(COOKIE_NAME, cookie.toString());
		cookieToSend.setMaxAge((int)SESSION_EXPIRATION_TIME/1000);
		response.addCookie(cookieToSend);
		
		//generate the site text
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println(
				"<!DOCTYPE html>\n" +
						"<html>\n" +
						"<head><title> Hello user </title></head>\n" +
						"<body bgcolor=\"#fdf5e6\">\n" +
						"<h1>" + sessionData.message + "</h1>\n" +
						"The serverID is: " + localAddress.toString() +"\n<br>" +
						"The session data was found in the " + dataSource +"\n<br>" +
						"The primary is now: " + cookie.location1 + "\n<br>" +
						"The secondary is now: " + (cookie.location2.equals(NULL_ADDRESS) ? "no secondary" : cookie.location2) + "\n<br>" +
						"The session expiration time is now: " + sessionData.expiration_timestamp + "\n<br>" +
						//TODO: update with real discard time
						"The discard_time is now: " + sessionData.expiration_timestamp + "\n<br>" +
						"The memberset is now: " + memberSet + "\n<br><br>" +
						"<form action='session' method='GET'>" +
						"<input type='submit' value='Replace' name='cmd'>" +
						"<input type='text' maxlength='512' size='40' name='new_message'>" +
						"</form>" +			
						"<form action='session' method='GET'>" +
						"<input type='submit' value='Refresh' name='cmd'>" +
						"</form>" +			
						"<form action='session' method='GET'>" +
						"<input type='submit' value='LogOut' name='cmd'>" +
						"</form>" +
						"<form action='session' method='GET'>" +
						"<input type='submit' value='Crash' name='cmd'>" +
						"</form>" +
						"<br> Session on: " + request.getLocalAddr() + ":" +request.getServerPort() +
						"<br> Expires: " + sessionData.expiration_timestamp +
				"</body></html>");
	}


	public static class SessionData {
		public String sessionID;
		public int version;
		public String message;
		public Date expiration_timestamp;
		public ServerAddress responseAddress;
		
		@Override
		public String toString(){
			SimpleDateFormat sdf= new SimpleDateFormat();
			return sessionID + "," + version +","+message+","+sdf.format(expiration_timestamp);
		}
	}
	
	public static class VerboseCookie {
		public String sessionID;
		public int version;
		public ServerAddress location1, location2;
		
		/** Creates a new VerboseCookie object from
		 *  a Cookie. Parses the sessionID, version number,
		 *  and locations from the cookie
		 *  
		 *  Example cookie value
		 *  
		 *  2_192.168.1.2_5300_8_192.168.1.2_5300_192.168.1.3_5301
		 *  |----------------||-||--------------| |------------|
		 *    session id      vers  location 1      location 2
		 *    
		 * @param cookie
		 */
		public VerboseCookie(Cookie cookie) {
			String[] cookieParts = cookie.getValue().split("_");
			
			if(cookieParts.length != 8)
				System.out.println("WARNING :: parsing cookie value without 7 parts" + cookie.getValue());
			
			sessionID = cookieParts[0] + "_" + cookieParts[1] + "_" + cookieParts[2];
			
			version = Integer.parseInt(cookieParts[3]);
			
			location1 = new ServerAddress(cookieParts[4], cookieParts[5]);
			//allow for non-replicated session cookies
			if (cookieParts.length > 7)
				location2 = new ServerAddress(cookieParts[6], cookieParts[7]);
			else
				location2 = NULL_ADDRESS;
			
			System.out.println("location2: " + location2);
		}
		
		public VerboseCookie(SessionData sd, ServerAddress primaryLocation, ServerAddress backupLocation){
			sessionID= sd.sessionID;
			version= sd.version;
			location1= primaryLocation;
			location2= backupLocation == null ? NULL_ADDRESS : backupLocation;
		}
		
		@Override
		public String toString(){
			String str= sessionID + "_" + version + "_" + location1;
			return location2.equals(NULL_ADDRESS) ?	str : str + "_" + location2;
		}
	}
	
	public static class ServerAddress {
		public String serverIpAddress;
		public String serverPort;
		
		public ServerAddress(String ipAddress, String port) {
			//Datagrampacket.getAddress().toString() puts a / in front of the address
			//The person who put that there should be shot
			if(ipAddress.contains("/"))
				serverIpAddress = ipAddress.substring(1);
			else 
				serverIpAddress = ipAddress;
			
			serverPort = port;
		}
		
		@Override
		public String toString() {
			return serverIpAddress + "_" + serverPort;
		}
		
		public InetSocketAddress getSocketAddress() {
			System.out.println(this.serverIpAddress);
			System.out.println(Integer.valueOf(this.serverPort));
			return new InetSocketAddress(this.serverIpAddress, Integer.valueOf(this.serverPort));
		}
		
		@Override
		public boolean equals(Object obj) {
			if(! (obj instanceof ServerAddress))
				return false;
			ServerAddress other = (ServerAddress) obj;
			
			return serverIpAddress.equals(other.serverIpAddress) &&
					serverPort.equals(other.serverPort);
		}
		
		@Override 
		public int hashCode() {
			return toString().hashCode();
		}
	}

	private class SessionTableCleaner implements Runnable {

		@Override
		public void run() {
			while(true) {
				for (SessionData session : sessionMap.values()) {
					if (session.expiration_timestamp.before(new Date())) 
						sessionMap.remove(session.sessionID);
				}

				try {
					Thread.sleep(CLEANUP_TIMEOUT);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
