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

/** Very simplistic servlet that generates plain text.
 *  Uses the @WebServlet annotation that is supported by
 *  Tomcat 7 and other servlet 3.0 containers. 
 *  
 *  From <a href="http://courses.coreservlets.com/Course-Materials/">the
 *  coreservlets.com tutorials on servlets, JSP, Struts, JSF 2.x, 
 *  Ajax, jQuery, GWT, Spring, Hibernate/JPA, Hadoop, and Java programming</a>.
 */

@WebServlet("/session")
public class SessionServlet extends HttpServlet {
  private static long nextSessionID = 0;
  
  //Number of milliseconds before session expires
  private static final long SESSION_EXPIRATION_TIME = 60 * 60 * 1000;
  
  private static final String COOKIE_NAME = "CS5300_WJK56_DRM237_BDT25";
  private static final String DEFAULT_MESSAGE = "Hello, User!";
  private String serverName;
  
  private ConcurrentHashMap<String, SessionData> sessionMap;
  private Thread cleanupDaemon;
	
  public SessionServlet(String serverName) {
	  super();
	  sessionMap = new ConcurrentHashMap<String, SessionServlet.SessionData>();
	  if(serverName == null)
		  this.serverName = "Server 1";
	  else
		  this.serverName = serverName;
	  cleanupDaemon = new Thread(new SessionTableCleaner());
	  cleanupDaemon.setDaemon(true);
  }
  
  @Override
  public void doGet(HttpServletRequest request,
                    HttpServletResponse response)
      throws ServletException, IOException {
	  
	  Cookie[] cookies = request.getCookies();
	  String cmd = request.getParameter("command");
	  
	  if(cookies == null) {
		  String sessionID = newSessionState(request, response);
		  refresh(request, response, sessionID);
	  	  return;
	  }
	  else {
		  for(Cookie cookie : cookies) {
			  if(COOKIE_NAME.equals(cookie.getName())) {
				  String sessionID = SessionData.getSessionID(cookie);
				  
				  if(cmd == null || cmd.equals("refresh")) {
					  refresh(request, response, sessionID);
					  return;
				  } else if(cmd.equals("replace")) {
					  replace(request, response, sessionID);
				  	  refresh(request, response, sessionID);
				  	  return;
				  } else if(cmd.equals("logout")) {
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
	session.sessionID = serverName + nextSessionID++;
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
	
	session.message = newMessage;
}

private void refresh(HttpServletRequest request, HttpServletResponse response, String sessionID) {
	// TODO Auto-generated method stub
	
}


public static class SessionData {
	public String sessionID;
	public int version;
	public String message;
	public Date expiration_timestamp;
	
	public String packageCookie() {
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
		for (SessionData session : sessionMap.values()) {
			if (session.expiration_timestamp.before(new Date())) 
				sessionMap.remove(session.sessionID);
		}
	}
}
}
