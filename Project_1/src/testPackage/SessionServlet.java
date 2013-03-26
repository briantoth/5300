package testPackage; // Always use packages. Never use default package.

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashSet;
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
	private static final long SESSION_EXPIRATION_TIME = 60 * 1000;
	private static final long CLEANUP_TIMEOUT = 5 * 1000;

	private static final String COOKIE_NAME = "CS5300_WJK56_DRM237_BDT25";
	private static final String DEFAULT_MESSAGE = "Hello, User!";
	private static final ServerAddress NULL_ADDRESS = new ServerAddress("0.0.0.0", "0");
	private String serverName;

	private ConcurrentHashMap<String, SessionData> sessionMap;
	private Thread cleanupDaemon;
	private RPC rpcServer;
	private Set<ServerAddress> memberSet = new HashSet<ServerAddress>();
	
	private ServerAddress localAddress;

	//TODO: find out how to get the real constructor called
	public SessionServlet(){
		this("Server 1");
	}

	public SessionServlet(String serverName) {
		super();
		sessionMap = new ConcurrentHashMap<String, SessionServlet.SessionData>();
		if(serverName == null)
			this.serverName = "Server 1";
		else
			this.serverName = serverName;

		cleanupDaemon = new Thread(new SessionTableCleaner());
		cleanupDaemon.setDaemon(true);
		cleanupDaemon.start();
		
		rpcServer = new RPC(sessionMap, memberSet);
	}

	@Override
	public void doGet(HttpServletRequest request,
			HttpServletResponse response)
					throws ServletException, IOException {
		
		if(localAddress == null)
			localAddress = new ServerAddress(request.getLocalAddr(), "" + request.getLocalPort());
		
		Cookie[] cookies = request.getCookies();
		String cmd = request.getParameter("cmd");

		if(cookies == null) {
			SessionData sessionData = newSessionState(request, response);
			refresh(request, response, sessionData);
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
						
						//TODO
						//SessionData = SessionRead(sessionID, location1, location2);
					} else {
						//It is in our local map
						sessionData = sessionMap.get(sessionID);
					}
		
					
					if(sessionData == null) {
						//The session expired. 
						sessionData = newSessionState(request, response);
					}

					if(cmd == null || cmd.equals("Refresh")) {
						refresh(request, response, sessionData);
						return;
					} else if(cmd.equals("Replace")) {
						replace(request, response, sessionData);
						refresh(request, response, sessionData);
						return;
					} else if(cmd.equals("LogOut")) {
						logout(request, response, sessionData);
						return;
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
		refresh(request, response, sessionData);
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
		session.sessionID = getNextSessionID() + " _ " + localAddress.toString();

		//Set version number to 0 since it will be incremented in the
		//refresh method to 1
		session.version = 0;
		session.message = DEFAULT_MESSAGE;
		session.expiration_timestamp = new Date(new Date().getTime() + SESSION_EXPIRATION_TIME);

		//TODO Is it neccessary to put it in the session map right now?
		sessionMap.put(session.sessionID, session);
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



	private void logout(HttpServletRequest request, HttpServletResponse response, SessionData sessionData) {
		//TODO Remove this line. It is only here so it compiles;
		String sessionID = "";
		
		// remove session from session table
		sessionMap.remove(sessionID);
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
			// TODO do nothing?
		}
	}




	private void refresh(HttpServletRequest request, HttpServletResponse response, SessionData sessionData) throws IOException {
		//TODO Remove this line. It is only there so the program compiles
		String sessionID = "";
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		SessionData sd= sessionMap.get(sessionID);
		out.println(
				"<!DOCTYPE html>\n" +
						"<html>\n" +
						"<head><title> Hello user </title></head>\n" +
						"<body bgcolor=\"#fdf5e6\">\n" +
						"<h1>" + sd.message + "</h1>\n" +
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
						"<br> Session on: " + request.getLocalAddr() + ":" +request.getServerPort() +
						"<br> Expires: " + sd.expiration_timestamp +
				"</body></html>");

		sd.version++;
		//reset expiration of session
		sd.expiration_timestamp = new Date(new Date().getTime() + SESSION_EXPIRATION_TIME);
		response.addCookie(new Cookie(COOKIE_NAME, sd.packageCookie(serverName)));
	}


	public static class SessionData {
		public String sessionID;
		public int version;
		public String message;
		public Date expiration_timestamp;

		public String packageCookie(String serverName) {
			String value = "";
			value += this.sessionID;
			value += ",";
			value += String.valueOf(this.version);
			return value;
		}

		public static String getSessionID(Cookie cookie) {
			return cookie.getValue().split(",")[0];
		}
		
		@Override
		public String toString() {
			String value = "";
			value += this.sessionID;
			value += ",";
			value += String.valueOf(this.version);
			value += ",";
			value += this.message;
			value += ",";
			value += this.expiration_timestamp.toString();
			return value;
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
			location2 = new ServerAddress(cookieParts[6], cookieParts[7]);
			
		}
		
	}
	
	public static class ServerAddress {
		public String serverIpAddress;
		public String serverPort;
		
		public ServerAddress(String ipAddress, String port) {
			serverIpAddress = ipAddress;
			serverPort = port;
		}
		
		public String toString() {
			return serverIpAddress + "_" + serverPort;
		}
		
		public InetSocketAddress getSocketAddress() {
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
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
