package testPackage; // Always use packages. Never use default package.

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import coreservlets.ServletUtilities;

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
  private static long nextSessionID = 0;
  
  //Number of milliseconds before session expires
  private static final long SESSION_EXPIRATION_TIME = 60 * 1000;
  private static final long CLEANUP_TIMEOUT = 5 * 1000;
  
  private static final String COOKIE_NAME = "CS5300_WJK56_DRM237_BDT25";
  private static final String DEFAULT_MESSAGE = "Hello, User!";
  private String serverName;
  
  private ConcurrentHashMap<String, SessionData> sessionMap;
  private Thread cleanupDaemon;
  
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
	  System.out.println("Starting daemon");
	  cleanupDaemon.setDaemon(true);
	  cleanupDaemon.start();
  }
  
  @Override
  public void doGet(HttpServletRequest request,
                    HttpServletResponse response)
      throws ServletException, IOException {
	  
	  Cookie[] cookies = request.getCookies();
	  String cmd = request.getParameter("cmd");
	  
	  if(cookies == null) {
		  System.out.println("cookies is null");
		  String sessionID = newSessionState(request, response);
		  refresh(request, response, sessionID);
	  	  return;
	  }
	  else {
		  for(Cookie cookie : cookies) {
			  if(COOKIE_NAME.equals(cookie.getName())) {
				  String sessionID = SessionData.getSessionID(cookie);
				  
				  System.out.println(sessionID);
				  if(!sessionMap.containsKey(sessionID)){
					  System.out.println(sessionID);
					  sessionID = newSessionState(request, response);
					  refresh(request, response, sessionID);
					  return;
				  }
				  
				  if(cmd == null || cmd.equals("Refresh")) {
					  refresh(request, response, sessionID);
					  return;
				  } else if(cmd.equals("Replace")) {
					  replace(request, response, sessionID);
				  	  refresh(request, response, sessionID);
				  	  return;
				  } else if(cmd.equals("LogOut")) {
					  logout(request, response, sessionID);
					  return;
				  }
			  }
		  }
	  }
	  
	  //If we got here than there are cookies, but none with
	  //the name COOKIE_NAME, so we need to create a new session
	  
	  String sessionID = newSessionState(request, response);
	  refresh(request, response, sessionID);
  }

/** Creates a new SessionData object and stores it in the sessionMap */
private String newSessionState(HttpServletRequest request, HttpServletResponse response) {
	SessionData session = new SessionData();
	session.sessionID = serverName + "-" + nextSessionID++;
	session.version = 1;
	session.message = DEFAULT_MESSAGE;
	session.expiration_timestamp = new Date(new Date().getTime() + SESSION_EXPIRATION_TIME);
	
	sessionMap.put(session.sessionID, session);
	return session.sessionID;
}

private void logout(HttpServletRequest request, HttpServletResponse response, String sessionID) {
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


private void replace(HttpServletRequest request, HttpServletResponse response, String sessionID) {
	SessionData session = sessionMap.get(sessionID);
	String newMessage = request.getParameter("new_message");
	
	//In case there is no new_message parameter in the request
	//This really shouldn't happen
	newMessage = newMessage == null ? "" : newMessage;
	System.out.println(newMessage);
	
	session.message = newMessage;
}


private void refresh(HttpServletRequest request, HttpServletResponse response, String sessionID) throws IOException {

	response.setContentType("text/html");
	PrintWriter out = response.getWriter();
	SessionData sd= sessionMap.get(sessionID);
	out.println
	(ServletUtilities.headWithTitle("Hello user") +
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
